// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <ostream>

#include "node.h"
#include "stages/node_layout.h"

namespace crimson::os::seastore::onode {

template <typename FieldType, node_type_t NODE_TYPE>
class node_extent_t;
class LogicalCachedExtent;

template <typename FieldType, node_type_t _NODE_TYPE, typename ConcreteType>
class NodeT : virtual public Node {
 public:
  using node_stage_t = node_extent_t<FieldType, _NODE_TYPE>;
  using value_t = value_type_t<_NODE_TYPE>;
  static constexpr auto FIELD_TYPE = FieldType::FIELD_TYPE;
  static constexpr auto NODE_TYPE = _NODE_TYPE;

  virtual ~NodeT() {
    if (is_root()) {
      tree->do_untrack_root(*this);
    } else {
      _parent_info->ptr->do_untrack_child(*this);
    }
  }

  bool is_root() const override final {
    assert((super && tree && !_parent_info.has_value()) ||
           (!super && !tree && _parent_info.has_value()));
    return !_parent_info.has_value();
  }
  void as_root(Ref<Btree> _tree, Ref<DummyRootBlock> _super) override final {
    assert(!super);
    assert(!tree);
    assert(!_parent_info);
    assert(_super->get_onode_root_laddr() == laddr());
    assert(is_level_tail());
    super = _super;
    tree = _tree;
    tree->do_track_root(*this);
  }
  void handover_root(Ref<InternalNode> new_root,
                     const search_position_t& tracked_pos) override final {
    assert(is_root());
    assert(super->get_onode_root_laddr() == laddr());
    super->write_onode_root_laddr(new_root->laddr());

    tree->do_untrack_root(*this);
    new_root->as_root(tree, super);
    super.reset();
    tree.reset();

    as_child(tracked_pos, new_root);
  }
  void as_child(const search_position_t& pos,
                Ref<InternalNode> parent_node) override final {
    assert(!super);
    assert(!tree);
    _parent_info = parent_info_t{pos, parent_node};
    parent_info().ptr->do_track_child(*this);
  }
  // TODO: template
  void as_child_unsafe(const search_position_t& pos,
                       Ref<InternalNode> parent_node) override final {
    assert(!super);
    assert(!tree);
    _parent_info = parent_info_t{pos, parent_node};
    parent_info().ptr->do_track_child_unsafe(*this);
  }
  const parent_info_t& parent_info() const override final { return *_parent_info; }

  bool is_level_tail() const override final { return _is_level_tail; }
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final;
  level_t level() const override final;
  full_key_t<KeyT::VIEW> get_key_view(const search_position_t&) const override final;
  full_key_t<KeyT::VIEW> get_largest_key_view() const override final;
  const value_t* get_value_ptr(const search_position_t&) const;
  std::ostream& dump(std::ostream&) const override final;
  std::ostream& dump_brief(std::ostream& os) const override final;

#ifndef NDEBUG
  void test_set_level_tail(bool is_tail) override final { _is_level_tail = is_tail; }
  void validate_unused() const {
    // node.fields().template validate_unused<NODE_TYPE>(is_level_tail());
  }
#endif

 protected:
  node_stage_t stage() const;
  LogicalCachedExtent& extent();
  const LogicalCachedExtent& extent() const;
  void set_level_tail(bool value) { _is_level_tail = value; }
  static Ref<ConcreteType> _allocate(level_t level, bool level_tail);

 private:
  void init(Ref<LogicalCachedExtent> _extent,
            bool is_level_tail) override final;

  // as non root
  std::optional<parent_info_t> _parent_info;
  // as root
  Ref<DummyRootBlock> super;
  Ref<Btree> tree;

  bool _is_level_tail;
  Ref<LogicalCachedExtent> _extent;
};

template <typename FieldType, typename ConcreteType>
class InternalNodeT : public InternalNode,
                      public NodeT<FieldType, node_type_t::INTERNAL, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::INTERNAL, ConcreteType>;
  using node_stage_t = typename parent_t::node_stage_t;

  virtual ~InternalNodeT() { assert(tracked_child_nodes.empty()); }

  Node::search_result_t do_lower_bound(
      const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;

  Ref<tree_cursor_t> lookup_smallest() override final {
    auto position = search_position_t::begin();
    laddr_t child_addr = *this->get_value_ptr(position);
    auto child = get_or_track_child(position, child_addr);
    return child->lookup_smallest();
  }

  Ref<tree_cursor_t> lookup_largest() override final {
    // NOTE: unlike L_NODE_T::lookup_largest(), this only works for the tail
    // internal node to return the tail child address.
    auto position = search_position_t::end();
    laddr_t child_addr = *this->get_value_ptr(position);
    auto child = get_or_track_child(position, child_addr);
    return child->lookup_largest();
  }

  void apply_child_split(const full_key_t<KeyT::VIEW>&,
                         Ref<Node>, Ref<Node>) override final;

#ifndef NDEBUG
  void test_clone_root(Ref<Btree>) const override final;
  void test_clone_non_root(Ref<InternalNode>) const override final;
#endif

  // TODO: extract a common tracker for InternalNode and LeafNode
  // to track Node and tree_cursor_t.
  void track_insert(const search_position_t&, match_stage_t, Ref<Node>);

  void track_split(const search_position_t&, Ref<ConcreteType>);

  static Ref<ConcreteType> allocate(level_t level, bool level_tail) {
    assert(level != 0u);
    return ConcreteType::_allocate(level, level_tail);
  }

 private:
  void validate_child(const Node& child) const {
    assert(this->level() - 1 == child.level());
    assert(this == child.parent_info().ptr);
    auto& child_pos = child.parent_info().position;
    assert(*this->get_value_ptr(child_pos) == child.laddr());
#ifndef NDEBUG
    if (child_pos.is_end()) {
      assert(this->is_level_tail());
      assert(child.is_level_tail());
    } else {
      assert(!child.is_level_tail());
      assert(get_key_view(child_pos) == child.get_largest_key_view());
    }
#endif
    // XXX(multi-type)
    assert(this->field_type() <= child.field_type());
  }

  // called by the tracked child nodes
  void do_track_child(Node& child) override final {
    validate_child(child);
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos) == tracked_child_nodes.end());
    tracked_child_nodes[child_pos] = &child;
  }
  void do_track_child_unsafe(Node& child) override final {
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos) == tracked_child_nodes.end());
    tracked_child_nodes[child_pos] = &child;
  }
  void do_untrack_child(const Node& child) override final {
    validate_child(child);
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos)->second == &child);
    auto removed = tracked_child_nodes.erase(child_pos);
    assert(removed);
  }

  Ref<Node> get_or_track_child(const search_position_t&, laddr_t);

  // TODO: intrusive
  // TODO: use weak ref
  // TODO: as transactions are isolated with each other, the in-memory tree
  // hierarchy needs to be attached to the specific transaction.

  // track the current living child nodes by position
  std::map<search_position_t, Node*> tracked_child_nodes;
};
class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {
 public:
  static void upgrade_root(Ref<Node> root);
};
class InternalNode1 final : public InternalNodeT<node_fields_1_t, InternalNode1> {};
class InternalNode2 final : public InternalNodeT<node_fields_2_t, InternalNode2> {};
class InternalNode3 final : public InternalNodeT<internal_fields_3_t, InternalNode3> {};

template <typename FieldType, typename ConcreteType>
class LeafNodeT: public LeafNode,
                 public NodeT<FieldType, node_type_t::LEAF, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::LEAF, ConcreteType>;
  using node_stage_t = typename parent_t::node_stage_t;

  virtual ~LeafNodeT() { assert(tracked_cursors.empty()); }

  search_result_t do_lower_bound(
      const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;
  Ref<tree_cursor_t> lookup_smallest() override final;
  Ref<tree_cursor_t> lookup_largest() override final;
  Ref<tree_cursor_t> insert_value(
      const full_key_t<KeyT::HOBJ>&, const onode_t&,
      const search_position_t&, const MatchHistory&) override final;

#ifndef NDEBUG
  void test_clone_root(Ref<Btree>) const override final;
  void test_clone_non_root(Ref<InternalNode>) const override final;
#endif

  // TODO: extract a common tracker for InternalNode and LeafNode
  // to track Node and tree_cursor_t.
  Ref<tree_cursor_t> track_insert(const search_position_t&, match_stage_t, const onode_t*);

  void track_split(const search_position_t&, Ref<ConcreteType>);

  static Ref<ConcreteType> allocate(bool is_level_tail) {
    return ConcreteType::_allocate(0u, is_level_tail);
  }

 private:
  const onode_t* get_p_value(const search_position_t&) const override final;

  void validate_cursor(tree_cursor_t& cursor) {
    assert(this == cursor.get_leaf_node().get());
    assert(!cursor.is_end());
    assert(this->get_value_ptr(cursor.get_position()) == cursor.get_p_value());
  }

  // called by the tracked cursors
  void do_track_cursor(tree_cursor_t& cursor) override final {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos) == tracked_cursors.end());
    tracked_cursors[cursor_pos] = &cursor;
  }
  void do_untrack_cursor(tree_cursor_t& cursor) override final {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos)->second == &cursor);
    auto removed = tracked_cursors.erase(cursor_pos);
    assert(removed);
  }

  Ref<tree_cursor_t> get_or_track_cursor(
      const search_position_t& position, const onode_t* p_value);

  // TODO: intrusive
  // TODO: use weak ref
  // TODO: as transactions are isolated with each other, the in-memory tree
  // hierarchy needs to be attached to the specific transaction.

  // track the current living cursors by position
  std::map<search_position_t, tree_cursor_t*> tracked_cursors;
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

}
