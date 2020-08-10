// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <ostream>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "crimson/common/type_helpers.h"

#include "node_types.h"
#include "stages/stage_types.h"
#include "tree.h"
#include "tree_types.h"

namespace crimson::os::seastore::onode {

/**
 * in-memory subtree management:
 *
 * resource management (bottom-up):
 * USER          --> Ref<tree_cursor_t>
 * tree_cursor_t --> Ref<LeafNode>
 * Node (child)  --> Ref<InternalNode> (see parent_info_t)
 * Node (root)   --> Ref<Btree>, Ref<RootBlock>
 *
 * tracked lookup (top-down):
 * BTree         --> Node* (root)
 * InternalNode  --> Node* (children)
 * LeafNode      --> tree_cursor_t*
 */

class LeafNode;
class InternalNode;

class tree_cursor_t final
  : public boost::intrusive_ref_counter<
    tree_cursor_t, boost::thread_unsafe_counter> {
 public:
  ~tree_cursor_t();
  bool is_end() const { return position.is_end(); }
  const onode_t* get_p_value() const;

 private:
  tree_cursor_t(Ref<LeafNode>, const search_position_t&, const onode_t*);
  const search_position_t& get_position() const { return position; }
  Ref<LeafNode> get_leaf_node() { return leaf_node; }
  void invalidate_p_value() { p_value = nullptr; }
  void update_track(Ref<LeafNode>, const search_position_t&);
  void set_p_value(const onode_t* _p_value) {
    if (!p_value) {
      p_value = _p_value;
    } else {
      assert(p_value == _p_value);
    }
  }

  Ref<LeafNode> leaf_node;
  search_position_t position;
  mutable const onode_t* p_value;

  friend class LeafNode;
  friend class Node; // get_position(), get_leaf_node()
};

struct key_view_t;
struct key_hobj_t;
class LogicalCachedExtent;

class Node
  : public boost::intrusive_ref_counter<Node, boost::thread_unsafe_counter> {
 public:
  struct search_result_t {
    bool is_end() const { return p_cursor->is_end(); }
    Ref<tree_cursor_t> p_cursor;
    MatchKindBS match;
  };

  virtual ~Node();
  virtual level_t level() const = 0;
  virtual Ref<tree_cursor_t> lookup_smallest() = 0;
  virtual Ref<tree_cursor_t> lookup_largest() = 0;
  search_result_t lower_bound(const onode_key_t& key);
  std::pair<Ref<tree_cursor_t>, bool> insert(const onode_key_t&, const onode_t&);

  virtual std::ostream& dump(std::ostream&) const = 0;
  virtual std::ostream& dump_brief(std::ostream&) const = 0;

  static void mkfs(/* transaction, */Ref<Btree>);
  static Ref<Node> load_root(/* transaction */Ref<Btree>);

#ifndef NDEBUG
  virtual void test_make_destructable() = 0;
  virtual void test_clone_root(Ref<Btree>) const = 0;
  virtual void test_clone_non_root(Ref<InternalNode>) const = 0;
#endif

 public: // used by node_impl.h, FIXME: protected
  virtual bool is_level_tail() const = 0;
  virtual field_type_t field_type() const = 0;
  virtual laddr_t laddr() const = 0;
  virtual key_view_t get_key_view(const search_position_t&) const = 0;
  virtual key_view_t get_largest_key_view() const = 0;
  virtual search_result_t do_lower_bound(const key_hobj_t&, MatchHistory&) = 0;

 protected:
  Node() {}

  struct parent_info_t {
    search_position_t position;
    Ref<InternalNode> ptr;
  };
  bool is_root() const {
    assert((super && tree && !_parent_info.has_value()) ||
           (!super && !tree && _parent_info.has_value()));
    return !_parent_info.has_value();
  }
  void make_root(/* transaction */Ref<Btree> btree) {
    auto super = btree->get_super_block(/* transaction */);
    assert(super->get_onode_root_laddr() == L_ADDR_NULL);
    super->write_onode_root_laddr(laddr());
    as_root(btree, super);
  }
  void as_root(Ref<Btree> _tree, Ref<DummyRootBlock> _super) {
    assert(!super);
    assert(!tree);
    assert(!_parent_info);
    assert(_super->get_onode_root_laddr() == laddr());
    assert(is_level_tail());
    super = _super;
    tree = _tree;
    tree->do_track_root(*this);
  }
  void upgrade_root();
  template <bool VALIDATE = true>
  void as_child(const search_position_t&, Ref<InternalNode>);
  const parent_info_t& parent_info() const { return *_parent_info; }
  void insert_parent(Ref<Node> right_node);

  virtual void init(Ref<LogicalCachedExtent>, bool is_level_tail) = 0;
  static Ref<Node> load(laddr_t, bool is_level_tail);

 private:
  // as child/non-root
  std::optional<parent_info_t> _parent_info;
  // as root
  Ref<DummyRootBlock> super;
  Ref<Btree> tree;

  friend class InternalNode;
};
inline std::ostream& operator<<(std::ostream& os, const Node& node) {
  return node.dump_brief(os);
}

class InternalNode : virtual public Node {
 public:
  virtual ~InternalNode() { assert(tracked_child_nodes.empty()); }

 protected:
  // XXX: extract a common tracker for InternalNode to track Node,
  // and LeafNode to track tree_cursor_t.
  Ref<Node> get_or_track_child(
      const search_position_t& position, laddr_t child_addr) {
    Ref<Node> child;
    auto found = tracked_child_nodes.find(position);
    if (found == tracked_child_nodes.end()) {
      child = Node::load(child_addr, position.is_end());
      child->as_child(position, this);
    } else {
      child = found->second;
    }
    assert(child_addr == child->laddr());
    assert(position == child->parent_info().position);
    validate_child(*child);
    return child;
  }

  void track_insert(
      const search_position_t& insert_pos, match_stage_t insert_stage,
      Ref<Node> insert_child, Ref<Node> nxt_child = nullptr) {
    // update tracks
    auto pos_upper_bound = insert_pos;
    pos_upper_bound.index_by_stage(insert_stage) = INDEX_END;
    auto first = tracked_child_nodes.lower_bound(insert_pos);
    auto last = tracked_child_nodes.lower_bound(pos_upper_bound);
    std::vector<Node*> nodes;
    std::for_each(first, last, [&nodes](auto& kv) {
      nodes.push_back(kv.second);
    });
    tracked_child_nodes.erase(first, last);
    for (auto& node : nodes) {
      auto _pos = node->parent_info().position;
      assert(!_pos.is_end());
      ++_pos.index_by_stage(insert_stage);
      node->as_child(_pos, this);
    }
    // track insert
    insert_child->as_child(insert_pos, this);

#ifndef NDEBUG
    // validate left_child is before right_child
    if (nxt_child) {
      auto iter = tracked_child_nodes.find(insert_pos);
      ++iter;
      assert(iter->second == nxt_child);
    }
#endif
  }

  void replace_track(
      const search_position_t& position,
      Ref<Node> old_child, Ref<Node> new_child) {
    assert(tracked_child_nodes[position] == old_child);
    tracked_child_nodes.erase(position);
    new_child->as_child(position, this);
    assert(tracked_child_nodes[position] == new_child);
  }

  void track_split(
      const search_position_t& split_pos, Ref<InternalNode> right_node) {
    auto first = tracked_child_nodes.lower_bound(split_pos);
    auto iter = first;
    while (iter != tracked_child_nodes.end()) {
      search_position_t new_pos = iter->first;
      new_pos -= split_pos;
      iter->second->as_child<false>(new_pos, right_node);
      ++iter;
    }
    tracked_child_nodes.erase(first, tracked_child_nodes.end());
  }

  void validate_tracked_children() const {
#ifndef NDEBUG
    for (auto& kv : tracked_child_nodes) {
      assert(kv.first == kv.second->parent_info().position);
      validate_child(*kv.second);
    }
#endif
  }

#ifndef NDEBUG
  void test_clone_children(Ref<InternalNode> clone) const {
    for (auto& kv : tracked_child_nodes) {
      assert(kv.first == kv.second->parent_info().position);
      kv.second->test_clone_non_root(clone);
    }
  }
#endif

 private:
  // TODO: async
  virtual void apply_child_split(
      const search_position_t&, const key_view_t&, Ref<Node>, Ref<Node>) = 0;
  virtual const laddr_t* get_p_value(const search_position_t&) const = 0;
  void validate_child(const Node& child) const;
  template <bool VALIDATE>
  void do_track_child(Node& child) {
    if constexpr (VALIDATE) {
      validate_child(child);
    }
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos) == tracked_child_nodes.end());
    tracked_child_nodes[child_pos] = &child;
  }
  void do_untrack_child(const Node& child) {
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos)->second == &child);
    auto removed = tracked_child_nodes.erase(child_pos);
    assert(removed);
  }

  // XXX: leverage intrusive data structure to control memory overhead
  // track the current living child nodes by position
  std::map<search_position_t, Node*> tracked_child_nodes;

  friend class Node;
};

class LeafNode : virtual public Node {
 public:
  virtual ~LeafNode() { assert(tracked_cursors.empty()); }

 protected:
  // XXX: extract a common tracker for InternalNode to track Node,
  // and LeafNode to track tree_cursor_t.
  Ref<tree_cursor_t> get_or_track_cursor(
      const search_position_t& position, const onode_t* p_value) {
    if (position.is_end()) {
      assert(this->is_level_tail());
      assert(!p_value);
      // we need to return the leaf node to insert
      return new tree_cursor_t(this, position, p_value);
    }

    Ref<tree_cursor_t> p_cursor;
    auto found = tracked_cursors.find(position);
    if (found == tracked_cursors.end()) {
      p_cursor = new tree_cursor_t(this, position, p_value);
    } else {
      p_cursor = found->second;
      assert(p_cursor->get_leaf_node() == this);
      assert(p_cursor->get_position() == position);
      p_cursor->set_p_value(p_value);
    }
    return p_cursor;
  }

  Ref<tree_cursor_t> track_insert(
      const search_position_t& insert_pos, match_stage_t insert_stage,
      const onode_t* p_onode) {
    // invalidate cursor value
    auto pos_invalidate_begin = insert_pos;
    pos_invalidate_begin.index_by_stage(STAGE_RIGHT) = 0;
    auto begin_invalidate = tracked_cursors.lower_bound(pos_invalidate_begin);
    std::for_each(begin_invalidate, tracked_cursors.end(), [](auto& kv) {
      kv.second->invalidate_p_value();
    });

    // update cursor position
    auto pos_upper_bound = insert_pos;
    pos_upper_bound.index_by_stage(insert_stage) = INDEX_END;
    auto first = tracked_cursors.lower_bound(insert_pos);
    auto last = tracked_cursors.lower_bound(pos_upper_bound);
    std::vector<tree_cursor_t*> p_cursors;
    std::for_each(first, last, [&p_cursors](auto& kv) {
      p_cursors.push_back(kv.second);
    });
    tracked_cursors.erase(first, last);
    for (auto& p_cursor : p_cursors) {
      search_position_t new_pos = p_cursor->get_position();
      ++new_pos.index_by_stage(insert_stage);
      p_cursor->update_track(this, new_pos);
    }

    // track insert
    return new tree_cursor_t(this, insert_pos, p_onode);
  }

  void track_split(
      const search_position_t& split_pos, Ref<LeafNode> right_node) {
    // invalidate cursor value
    auto pos_invalidate_begin = split_pos;
    pos_invalidate_begin.index_by_stage(STAGE_RIGHT) = 0;
    auto begin_invalidate = tracked_cursors.lower_bound(pos_invalidate_begin);
    std::for_each(begin_invalidate, tracked_cursors.end(), [](auto& kv) {
      kv.second->invalidate_p_value();
    });

    // update cursor ownership and position
    auto first = tracked_cursors.lower_bound(split_pos);
    auto iter = first;
    while (iter != tracked_cursors.end()) {
      search_position_t new_pos = iter->first;
      new_pos -= split_pos;
      iter->second->update_track(right_node, new_pos);
      ++iter;
    }
    tracked_cursors.erase(first, tracked_cursors.end());
  }

  void validate_tracked_cursors() const {
#ifndef NDEBUG
    for (auto& kv : tracked_cursors) {
      assert(kv.first == kv.second->get_position());
      validate_cursor(*kv.second);
    }
#endif
  }

 private:
  virtual Ref<tree_cursor_t> insert_value(
      const key_hobj_t&,
      const onode_t&,
      const search_position_t&,
      const MatchHistory&) = 0;
  friend class Node;

  virtual const onode_t* get_p_value(const search_position_t&) const = 0;
  void validate_cursor(tree_cursor_t& cursor) const {
    assert(this == cursor.get_leaf_node().get());
    assert(!cursor.is_end());
    assert(get_p_value(cursor.get_position()) == cursor.get_p_value());
  }
  void do_track_cursor(tree_cursor_t& cursor) {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos) == tracked_cursors.end());
    tracked_cursors[cursor_pos] = &cursor;
  }
  void do_untrack_cursor(tree_cursor_t& cursor) {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos)->second == &cursor);
    auto removed = tracked_cursors.erase(cursor_pos);
    assert(removed);
  }
  // XXX: leverage intrusive data structure to control memory overhead
  // track the current living cursors by position
  std::map<search_position_t, tree_cursor_t*> tracked_cursors;
  friend class tree_cursor_t;
};

}
