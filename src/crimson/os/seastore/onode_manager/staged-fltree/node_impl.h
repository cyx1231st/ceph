// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "node.h"
#include "node_extent_visitor.h"
#include "stages/node_layout.h"

namespace crimson::os::seastore::onode {

// TODO: decouple NodeT with Node

template <typename FieldType, node_type_t _NODE_TYPE, typename ConcreteType>
class NodeT : virtual public Node {
 public:
  using extent_t = NodeExtentT<FieldType, _NODE_TYPE>;
  using node_ertr = Node::node_ertr;
  template <class... ValuesT>
  using node_future = Node::node_future<ValuesT...>;
  using node_stage_t = typename extent_t::node_stage_t;
  using position_t = typename extent_t::position_t;
  using value_t = typename extent_t::value_t;
  static constexpr auto FIELD_TYPE = extent_t::FIELD_TYPE;
  static constexpr auto NODE_TYPE = _NODE_TYPE;
  struct fresh_node_t {
    Ref<ConcreteType> node;
    NodeExtentMutable mut;
    std::pair<Ref<Node>, NodeExtentMutable> make_pair() {
      return std::make_pair(Ref<Node>(node), mut);
    }
  };

  virtual ~NodeT() = default;

  bool is_level_tail() const override final;
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final;
  level_t level() const override final;
  full_key_t<KeyT::VIEW> get_key_view(const search_position_t&) const override final;
  full_key_t<KeyT::VIEW> get_largest_key_view() const override final;
  std::ostream& dump(std::ostream&) const override final;
  std::ostream& dump_brief(std::ostream&) const override final;

  const value_t* get_value_ptr(const search_position_t&) const;

#ifndef NDEBUG
  void test_make_destructable(
      context_t, NodeExtentMutable&, Super::URef&&) override final;
  node_future<> test_clone_non_root(
      context_t, Ref<InternalNode>) const override final {
    assert(false && "not implemented");
  }
#endif

  static Ref<Node> load(NodeExtent::Ref, bool);

 protected:
  extent_t extent;
};

template <typename FieldType, typename ConcreteType>
class InternalNodeT : public InternalNode,
                      public NodeT<FieldType, node_type_t::INTERNAL, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::INTERNAL, ConcreteType>;
  using extent_t = typename parent_t::extent_t;
  using fresh_node_t = typename parent_t::fresh_node_t;
  using node_stage_t = typename parent_t::node_stage_t;
  using position_t = typename parent_t::position_t;

  virtual ~InternalNodeT() = default;

  node_future<search_result_t> do_lower_bound(
      context_t, const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;

  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t c) override final {
    auto position = search_position_t::begin();
    laddr_t child_addr = *this->get_value_ptr(position);
    return get_or_track_child(c, position, child_addr).safe_then([c](auto child) {
      return child->lookup_smallest(c);
    });
  }

  node_future<Ref<tree_cursor_t>> lookup_largest(context_t c) override final {
    // NOTE: unlike L_NODE_T::lookup_largest(), this only works for the tail
    // internal node to return the tail child address.
    auto position = search_position_t::end();
    laddr_t child_addr = *this->get_value_ptr(position);
    return get_or_track_child(c, position, child_addr).safe_then([c](auto child) {
      return child->lookup_largest(c);
    });
  }

  node_future<> apply_child_split(
      context_t, const search_position_t&, const full_key_t<KeyT::VIEW>&,
      Ref<Node>, Ref<Node>) override final;

#ifndef NDEBUG
  node_future<> test_clone_root(context_t, Super::URef&&) const override final;
#endif

  static node_future<fresh_node_t> allocate(context_t, level_t, bool is_level_tail);

 private:
  const laddr_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }

};
class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {
 public:
  static node_future<Ref<InternalNode0>> allocate_root(
      context_t, level_t, laddr_t, Super::URef&&);
#ifndef NDEBUG
  static node_future<Ref<InternalNode0>> test_allocate_cloned_root(
      context_t, level_t, Super::URef&&, const NodeExtent&);
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
  using extent_t = typename parent_t::extent_t;
  using fresh_node_t = typename parent_t::fresh_node_t;
  using node_stage_t = typename parent_t::node_stage_t;
  using position_t = typename parent_t::position_t;

  virtual ~LeafNodeT() = default;

  node_future<search_result_t> do_lower_bound(
      context_t, const full_key_t<KeyT::HOBJ>&, MatchHistory&) override final;
  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override final;
  node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override final;
  node_future<Ref<tree_cursor_t>> insert_value(
      context_t, const full_key_t<KeyT::HOBJ>&, const onode_t&,
      const search_position_t&, const MatchHistory&) override final;

#ifndef NDEBUG
  node_future<> test_clone_root(context_t, Super::URef&&) const override final;
#endif

  static node_future<fresh_node_t> allocate(context_t, bool is_level_tail);

 private:
  const onode_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {
 public:
  static node_future<> mkfs(context_t c, Super::URef&& super) {
    return allocate(c, true
    ).safe_then([c, super = std::move(super)](auto fresh_node) mutable {
      fresh_node.node->make_root_new(c, std::move(super));
    });
  }
#ifndef NDEBUG
  static node_future<Ref<LeafNode0>> test_allocate_cloned_root(
      context_t, Super::URef&&, const NodeExtent&);
#endif
};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

inline Node::node_future<Ref<Node>> load_node(
    context_t c, laddr_t addr, bool expect_is_level_tail) {
  // NOTE:
  // *option1: all types of node have the same length;
  // option2: length is defined by node/field types;
  // option3: length is totally flexible;
  return c.nm.read_extent(c.t, addr, NODE_BLOCK_SIZE
  ).safe_then([expect_is_level_tail](auto extent) {
    const auto header = reinterpret_cast<const node_header_t*>(extent->get_read());
    auto _field_type = header->get_field_type();
    if (!_field_type.has_value()) {
      throw std::runtime_error("load failed: bad field type");
    }
    auto _node_type = header->get_node_type();
    if (_field_type == field_type_t::N0) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode0::load(extent, expect_is_level_tail);
      } else {
        return InternalNode0::load(extent, expect_is_level_tail);
      }
    } else if (_field_type == field_type_t::N1) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode1::load(extent, expect_is_level_tail);
      } else {
        return InternalNode1::load(extent, expect_is_level_tail);
      }
    } else if (_field_type == field_type_t::N2) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode2::load(extent, expect_is_level_tail);
      } else {
        return InternalNode2::load(extent, expect_is_level_tail);
      }
    } else if (_field_type == field_type_t::N3) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode3::load(extent, expect_is_level_tail);
      } else {
        return InternalNode3::load(extent, expect_is_level_tail);
      }
    } else {
      assert(false);
    }
  });
}

}
