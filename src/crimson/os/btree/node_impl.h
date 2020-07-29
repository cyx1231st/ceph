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

  virtual ~NodeT() = default;

  void as_child(const parent_info_t& info) override final {
    assert(!p_root_ref);
    _parent_info = info;
  }
  void as_root(Ref<Node>& ref) override final {
    assert(!p_root_ref);
    assert(!_parent_info);
    ref = this;
    p_root_ref = &ref;
  }
  void handover_root(Ref<InternalNode> new_root) override final {
    assert(is_root());
    new_root->as_root(*p_root_ref);
    p_root_ref = nullptr;
  }
  bool is_root() const override final {
    assert((p_root_ref && !_parent_info.has_value()) ||
           (!p_root_ref && _parent_info.has_value()));
    return !_parent_info.has_value();
  }
  const parent_info_t& parent_info() const override final { return *_parent_info; }

  bool is_level_tail() const override final { return _is_level_tail; }
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final;
  level_t level() const override final;
  full_key_t<KeyT::VIEW> get_key_view(const search_position_t&) const override final;
  full_key_t<KeyT::VIEW> get_largest_key_view() const override final;
  const value_t* get_value_ptr(const search_position_t&);
  std::ostream& dump(std::ostream&) const override final;
  std::ostream& dump_brief(std::ostream& os) const override final;

#ifndef NDEBUG
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

  std::optional<parent_info_t> _parent_info;
  Ref<Node>* p_root_ref = nullptr;
  bool _is_level_tail;
  Ref<LogicalCachedExtent> _extent;
};

template <typename FieldType, typename ConcreteType>
class InternalNodeT : public InternalNode,
                      public NodeT<FieldType, node_type_t::INTERNAL, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::INTERNAL, ConcreteType>;
  using node_stage_t = typename parent_t::node_stage_t;

  virtual ~InternalNodeT() = default;

  Node::search_result_t do_lower_bound(
      const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;

  Ref<tree_cursor_t> lookup_smallest() override final {
    auto position = search_position_t::begin();
    laddr_t child_addr = *this->get_value_ptr(position);
    auto child = get_or_load_child(child_addr, position);
    return child->lookup_smallest();
  }

  Ref<tree_cursor_t> lookup_largest() override final {
    auto position = search_position_t::end();
    laddr_t child_addr = *this->get_value_ptr(position);
    auto child = get_or_load_child(child_addr, position);
    return child->lookup_largest();
  }

  Ref<Node> get_tracked_child(const search_position_t& pos) override final {
    assert(tracked_child_nodes.find(pos) != tracked_child_nodes.end());
    return tracked_child_nodes[pos];
  }

  void apply_child_split(const full_key_t<KeyT::VIEW>&,
                         Ref<Node>, Ref<Node>) override final;

#ifndef NDEBUG
  Ref<Node> test_clone(Ref<Node>&) const override final;
#endif

  void track_child(const search_position_t& pos, Ref<Node> child) {
    assert(tracked_child_nodes.find(pos) == tracked_child_nodes.end());
#ifndef NDEBUG
    if (pos == search_position_t::end()) {
      assert(this->is_level_tail());
      assert(child->is_level_tail());
    } else {
      assert(!child->is_level_tail());
      assert(get_key_view(pos) == child->get_largest_key_view());
    }
#endif
    tracked_child_nodes[pos] = child;
    child->as_child({pos, this});
  }

  void track_insert(const search_position_t&, match_stage_t, Ref<Node>);

  void track_split(const search_position_t&, Ref<Node>);

  static Ref<ConcreteType> allocate(level_t level, bool level_tail) {
    assert(level != 0u);
    return ConcreteType::_allocate(level, level_tail);
  }

 private:
  Ref<Node> get_or_load_child(laddr_t child_addr, const search_position_t& position);
  // TODO: intrusive
  // TODO: use weak ref
  // TODO: as transactions are isolated with each other, the in-memory tree
  // hierarchy needs to be attached to the specific transaction.
  std::map<search_position_t, Ref<Node>> tracked_child_nodes;
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

  virtual ~LeafNodeT() = default;

  search_result_t do_lower_bound(
      const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;
  Ref<tree_cursor_t> lookup_smallest() override final;
  Ref<tree_cursor_t> lookup_largest() override final;
  Ref<tree_cursor_t> insert_value(
      const full_key_t<KeyT::HOBJ>&, const onode_t&,
      const search_position_t&, const MatchHistory&) override final;

#ifndef NDEBUG
  Ref<Node> test_clone(Ref<Node>&) const override final;
#endif

  static Ref<ConcreteType> allocate(bool is_level_tail) {
    return ConcreteType::_allocate(0u, is_level_tail);
  }

 private:
  Ref<tree_cursor_t> get_or_create_cursor(
      const search_position_t& position, const onode_t* p_value);
  // TODO: intrusive
  // TODO: use weak ref
  // TODO: as transactions are isolated with each other, the in-memory tree
  // hierarchy needs to be attached to the specific transaction.
  //std::map<search_position_t, Ref<tree_cursor_t>> tracked_cursors;
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {
 public:
  static void allocate_root(Ref<Node>& ref) {
    auto root = allocate(true);
    root->as_root(ref);
  }
};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

}
