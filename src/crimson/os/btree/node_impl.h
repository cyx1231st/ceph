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

  bool is_level_tail() const override final { return _is_level_tail; }
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final;
  level_t level() const override final;
  full_key_t<KeyT::VIEW> get_key_view(const search_position_t&) const override final;
  full_key_t<KeyT::VIEW> get_largest_key_view() const override final;
  std::ostream& dump(std::ostream&) const override final;
  std::ostream& dump_brief(std::ostream& os) const override final;

  const value_t* get_value_ptr(const search_position_t&) const;

#ifndef NDEBUG
  void test_make_destructable() override final {
    set_level_tail(true);
    make_root(new Btree());
  }
  void test_clone_non_root(Ref<InternalNode>) const override final {
    assert(false && "not implemented");
  }
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

  void apply_child_split(const search_position_t&, const full_key_t<KeyT::VIEW>&,
                         Ref<Node>, Ref<Node>) override final;

#ifndef NDEBUG
  void test_clone_root(Ref<Btree>) const override final;
#endif

  static Ref<ConcreteType> allocate(level_t level, bool level_tail) {
    assert(level != 0u);
    return ConcreteType::_allocate(level, level_tail);
  }

 private:
  const laddr_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }

};
class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {
 public:
  static Ref<InternalNode0> allocate_root(
      level_t, laddr_t, Ref<Btree>, Ref<DummyRootBlock>);
#ifndef NDEBUG
  static Ref<InternalNode0> test_allocate_cloned_root(
      level_t, Ref<Btree>, const LogicalCachedExtent&);
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
      const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;
  Ref<tree_cursor_t> lookup_smallest() override final;
  Ref<tree_cursor_t> lookup_largest() override final;
  Ref<tree_cursor_t> insert_value(
      const full_key_t<KeyT::HOBJ>&, const onode_t&,
      const search_position_t&, const MatchHistory&) override final;

#ifndef NDEBUG
  void test_clone_root(Ref<Btree>) const override final;
#endif

  static Ref<ConcreteType> allocate(bool is_level_tail) {
    return ConcreteType::_allocate(0u, is_level_tail);
  }

 private:
  const onode_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {
 public:
  static void mkfs(/* transaction */Ref<Btree> btree) {
    auto root = allocate(true);
    root->make_root(/* transaction, */btree);
  }
#ifndef NDEBUG
  static Ref<LeafNode0> test_allocate_cloned_root(
      Ref<Btree>, const LogicalCachedExtent&);
#endif
};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

}
