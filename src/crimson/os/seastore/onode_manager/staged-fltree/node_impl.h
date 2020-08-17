// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

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

  bool is_level_tail() const override final;
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final;
  level_t level() const override final;
  full_key_t<KeyT::VIEW> get_key_view(const search_position_t&) const override final;
  full_key_t<KeyT::VIEW> get_largest_key_view() const override final;
  std::ostream& dump(std::ostream&) const override final;
  std::ostream& dump_brief(std::ostream& os) const override final;

  const value_t* get_value_ptr(const search_position_t&) const;

#ifndef NDEBUG
  void test_make_destructable(context_t, SuperNodeURef&&) override final;
  void test_clone_non_root(context_t, Ref<InternalNode>) const override final {
    assert(false && "not implemented");
  }
#endif

 protected:
  node_stage_t stage() const;
  LogicalCachedExtent& extent();
  const LogicalCachedExtent& extent() const;
  static Ref<ConcreteType> _allocate(context_t, level_t level, bool level_tail);

 private:
  void init(Ref<LogicalCachedExtent>) override final;

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
      context_t, const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;

  Ref<tree_cursor_t> lookup_smallest(context_t c) override final {
    auto position = search_position_t::begin();
    laddr_t child_addr = *this->get_value_ptr(position);
    auto child = get_or_track_child(c, position, child_addr);
    return child->lookup_smallest(c);
  }

  Ref<tree_cursor_t> lookup_largest(context_t c) override final {
    // NOTE: unlike L_NODE_T::lookup_largest(), this only works for the tail
    // internal node to return the tail child address.
    auto position = search_position_t::end();
    laddr_t child_addr = *this->get_value_ptr(position);
    auto child = get_or_track_child(c, position, child_addr);
    return child->lookup_largest(c);
  }

  void apply_child_split(
      context_t, const search_position_t&, const full_key_t<KeyT::VIEW>&,
      Ref<Node>, Ref<Node>) override final;

#ifndef NDEBUG
  void test_clone_root(context_t, SuperNodeURef&&) const override final;
#endif

  static Ref<ConcreteType> allocate(context_t c, level_t level, bool level_tail) {
    assert(level != 0u);
    return ConcreteType::_allocate(c, level, level_tail);
  }

 private:
  const laddr_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }

};
class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {
 public:
  static Ref<InternalNode0> allocate_root(
      context_t, level_t, laddr_t, SuperNodeURef&&);
#ifndef NDEBUG
  static Ref<InternalNode0> test_allocate_cloned_root(
      context_t, level_t, SuperNodeURef&&, const LogicalCachedExtent&);
#endif
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
      context_t, const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;
  Ref<tree_cursor_t> lookup_smallest(context_t) override final;
  Ref<tree_cursor_t> lookup_largest(context_t) override final;
  Ref<tree_cursor_t> insert_value(
      context_t, const full_key_t<KeyT::HOBJ>&, const onode_t&,
      const search_position_t&, const MatchHistory&) override final;

#ifndef NDEBUG
  void test_clone_root(context_t, SuperNodeURef&&) const override final;
#endif

  static Ref<ConcreteType> allocate(context_t c, bool is_level_tail) {
    return ConcreteType::_allocate(c, 0u, is_level_tail);
  }

 private:
  const onode_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {
 public:
  static void mkfs(context_t c, SuperNodeURef&& super) {
    auto root = allocate(c, true);
    root->make_root_new(c, std::move(super));
  }
#ifndef NDEBUG
  static Ref<LeafNode0> test_allocate_cloned_root(
      context_t, SuperNodeURef&&, const LogicalCachedExtent&);
#endif
};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

}
