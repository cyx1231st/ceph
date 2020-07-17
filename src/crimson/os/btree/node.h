// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "crimson/common/type_helpers.h"

#include "node_types.h"
#include "stages/stage_types.h"
#include "tree_types.h"

namespace crimson::os::seastore::onode {

class LeafNode;
class InternalNode;

class tree_cursor_t
  : public boost::intrusive_ref_counter<
    tree_cursor_t, boost::thread_unsafe_counter> {
  // TODO: deref LeafNode if destroyed with leaf_node available
  // TODO: make sure to deref LeafNode if is_end()
  // TODO: make tree_cursor_t unique
 public:
  tree_cursor_t(Ref<LeafNode> node, const search_position_t& pos, const onode_t* p_value)
    : leaf_node{node}, position{pos}, p_value{p_value} {
    assert((!pos.is_end() && p_value) || (pos.is_end() && !p_value));
  }

  bool is_end() const { return position.is_end(); }
  Ref<LeafNode> get_leaf_node() { return leaf_node; }
  const search_position_t& get_position() const { return position; }
  const onode_t* get_p_value() const { return p_value; }

 private:
  Ref<LeafNode> leaf_node;
  search_position_t position;
  const onode_t* p_value;
};

struct key_view_t;
struct key_hobj_t;
class LogicalCachedExtent;

class Node
  : public boost::intrusive_ref_counter<Node, boost::thread_unsafe_counter> {
 public:
  struct parent_info_t {
    search_position_t position;
    Ref<InternalNode> ptr;
  };
  struct search_result_t {
    bool is_end() const { return p_cursor->is_end(); }
    Ref<tree_cursor_t> p_cursor;
    MatchKindBS match;
  };

  virtual ~Node() = default;

  virtual level_t level() const = 0;
  virtual Ref<tree_cursor_t> lookup_smallest() = 0;
  virtual Ref<tree_cursor_t> lookup_largest() = 0;
  search_result_t lower_bound(const onode_key_t& key);
  std::pair<Ref<tree_cursor_t>, bool> insert(const onode_key_t&, const onode_t&);

  virtual std::ostream& dump(std::ostream&) const = 0;
  virtual std::ostream& dump_brief(std::ostream&) const = 0;

#ifndef NDEBUG
  virtual Ref<Node> test_clone(Ref<Node>&) const = 0;
#endif

  static void allocate_root(Ref<Node>&);

 protected:
  Node() {}
  virtual void init(Ref<LogicalCachedExtent>, bool is_level_tail) = 0;
  static Ref<Node> load(laddr_t, bool is_level_tail);

 // FIXME: protected
 public:
  virtual void as_child(const parent_info_t&) = 0;
  virtual void as_root(Ref<Node>& ref) = 0;
  virtual void handover_root(Ref<InternalNode> new_root) = 0;
  virtual bool is_root() const = 0;
  virtual const parent_info_t& parent_info() const = 0;

  virtual bool is_level_tail() const = 0;
  virtual field_type_t field_type() const = 0;
  virtual laddr_t laddr() const = 0;
  virtual key_view_t get_key_view(const search_position_t&) const = 0;
  virtual search_result_t do_lower_bound(const key_hobj_t&, MatchHistory&) = 0;
};
inline std::ostream& operator<<(std::ostream& os, const Node& node) {
  return node.dump_brief(os);
}

class InternalNode : virtual public Node {
 public:
  virtual ~InternalNode() = default;

  // TODO: async
  virtual void apply_child_split(const key_view_t&, Ref<Node>, Ref<Node>) = 0;
};

class LeafNode : virtual public Node {
 public:
  virtual ~LeafNode() = default;

 private:
  virtual Ref<tree_cursor_t> insert_value(
      const key_hobj_t&,
      const onode_t&,
      const search_position_t&,
      const MatchHistory&) = 0;

  friend class Node;
};

}
