// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <ostream>

#include "node.h"
#include "stages/node_layout.h"
// TODO remove
#include "stages/node_stage.h"

namespace crimson::os::seastore::onode {

class LogicalCachedExtent;

template <typename FieldType, node_type_t _NODE_TYPE, typename ConcreteType>
class NodeT : virtual public Node {
 public:
  // TODO: move out
  using node_t = node_extent_t<FieldType, _NODE_TYPE>;
  using value_t = value_type_t<_NODE_TYPE>;
  static constexpr auto FIELD_TYPE = FieldType::FIELD_TYPE;
  static constexpr auto NODE_TYPE = _NODE_TYPE;
  static constexpr auto EXTENT_SIZE = node_t::EXTENT_SIZE;

  virtual ~NodeT() = default;

  bool is_root() const override final { return !_parent_info.has_value(); }
  const parent_info_t& parent_info() const override final{ return *_parent_info; }
  bool is_level_tail() const override final { return node.is_level_tail(); }
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final;
  level_t level() const override final { return node.level(); }
  index_view_t get_index_view(const search_position_t&) const override final;
  std::ostream& dump(std::ostream&) const override final;
  std::ostream& dump_brief(std::ostream& os) const override final;

#ifndef NDEBUG
  void validate_unused() const {
    // node.fields().template validate_unused<NODE_TYPE>(is_level_tail());
  }

  Ref<Node> test_clone() const;
#endif

 protected:
  static Ref<ConcreteType> _allocate(level_t level, bool level_tail);
  const value_t* get_value_ptr(const search_position_t&);

 private:
  void init(Ref<LogicalCachedExtent> _extent,
            bool _is_level_tail,
            const parent_info_t* p_info) override final;

 protected:
  Ref<LogicalCachedExtent> extent;
  node_t node;

 private:
  std::optional<parent_info_t> _parent_info;
};

template <typename FieldType, typename ConcreteType>
class InternalNodeT : public NodeT<FieldType, node_type_t::INTERNAL, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::INTERNAL, ConcreteType>;
  // TODO: move out
  using node_t = typename parent_t::node_t;

  virtual ~InternalNodeT() = default;

  Node::search_result_t lower_bound(const onode_key_t&, MatchHistory&) override final;

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
class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {};
class InternalNode1 final : public InternalNodeT<node_fields_1_t, InternalNode1> {};
class InternalNode2 final : public InternalNodeT<node_fields_2_t, InternalNode2> {};
class InternalNode3 final : public InternalNodeT<internal_fields_3_t, InternalNode3> {};

template <typename FieldType, typename ConcreteType>
class LeafNodeT: public LeafNode, public NodeT<FieldType, node_type_t::LEAF, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::LEAF, ConcreteType>;
  // TODO: move out
  using node_t = typename parent_t::node_t;

  virtual ~LeafNodeT() = default;

  search_result_t lower_bound(const onode_key_t&, MatchHistory&) override final;
  Ref<tree_cursor_t> lookup_smallest() override final;
  Ref<tree_cursor_t> lookup_largest() override final;
  Ref<tree_cursor_t> insert_bottomup(
      const onode_key_t&, const onode_t&,
      const search_position_t&, const MatchHistory&) override final;

  static Ref<ConcreteType> allocate(bool is_level_tail) {
    return ConcreteType::_allocate(0u, is_level_tail);
  }

 private:
  // TODO: move out
  bool can_insert(
      const onode_key_t& key, const onode_t& value,
      const search_position_t& position, const MatchHistory& history,
      search_position_t& i_position, match_stage_t& i_stage,
      ns_oid_view_t::Type& dedup_type, node_offset_t& estimated_size);

  // TODO: move out
  const onode_t* proceed_insert(
      const onode_key_t& key, const onode_t& value,
      search_position_t& i_position, match_stage_t i_stage,
      ns_oid_view_t::Type dedup_type, node_offset_t estimated_size);

  Ref<tree_cursor_t> get_or_create_cursor(
      const search_position_t& position, const onode_t* p_value);
  // TODO: intrusive
  // TODO: use weak ref
  // TODO: as transactions are isolated with each other, the in-memory tree
  // hierarchy needs to be attached to the specific transaction.
  //std::map<search_position_t, Ref<tree_cursor_t>> tracked_cursors;
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

}
