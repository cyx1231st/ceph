// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_impl.h"

// TODO: remove
#include <iostream>
#include "common/likely.h"

#define STAGE_T node_to_stage_t<node_stage_t>

namespace crimson::os::seastore::onode {

using node_ertr = Node::node_ertr;
template <class... ValuesT>
using node_future = Node::node_future<ValuesT...>;

#define NODE_T NodeT<FieldType, NODE_TYPE, ConcreteType>
template class NodeT<node_fields_0_t, node_type_t::LEAF, LeafNode0>;
template class NodeT<node_fields_1_t, node_type_t::LEAF, LeafNode1>;
template class NodeT<node_fields_2_t, node_type_t::LEAF, LeafNode2>;
template class NodeT<leaf_fields_3_t, node_type_t::LEAF, LeafNode3>;
template class NodeT<node_fields_0_t, node_type_t::INTERNAL, InternalNode0>;
template class NodeT<node_fields_1_t, node_type_t::INTERNAL, InternalNode1>;
template class NodeT<node_fields_2_t, node_type_t::INTERNAL, InternalNode2>;
template class NodeT<internal_fields_3_t, node_type_t::INTERNAL, InternalNode3>;

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
bool NODE_T::is_level_tail() const { return extent.read().is_level_tail(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
laddr_t NODE_T::laddr() const { return extent.get_laddr(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
level_t NODE_T::level() const { return extent.read().level(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
full_key_t<KeyT::VIEW> NODE_T::get_key_view(
    const search_position_t& position) const {
  full_key_t<KeyT::VIEW> ret;
  STAGE_T::get_key_view(extent.read(), cast_down<STAGE_T::STAGE>(position), ret);
  return ret;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
full_key_t<KeyT::VIEW> NODE_T::get_largest_key_view() const {
  full_key_t<KeyT::VIEW> key_view;
  STAGE_T::lookup_largest_index(extent.read(), key_view);
  return key_view;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
std::ostream& NODE_T::dump(std::ostream& os) const {
  auto& node_stage = extent.read();
  auto p_start = node_stage.p_start();
  os << *this << ":";
  os << "\n  header: " << node_stage_t::header_size() << "B";
  size_t size = 0u;
  if (node_stage.keys()) {
    STAGE_T::dump(node_stage, os, "  ", size, p_start);
  } else {
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      return os << " empty!";
    } else { // internal node
      if (!is_level_tail()) {
        return os << " empty!";
      } else {
        size += node_stage_t::header_size();
      }
    }
  }
  if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
    if (is_level_tail()) {
      size += sizeof(laddr_t);
      auto value_ptr = node_stage.get_end_p_laddr();
      int offset = reinterpret_cast<const char*>(value_ptr) - p_start;
      os << "\n  tail value: 0x"
         << std::hex << *value_ptr << std::dec
         << " " << size << "B"
         << "  @" << offset << "B";
    }
  }
  return os;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
std::ostream& NODE_T::dump_brief(std::ostream& os) const {
  auto& node_stage = extent.read();
  os << "Node" << NODE_TYPE << FIELD_TYPE
     << "@0x" << std::hex << laddr()
     << "+" << node_stage_t::EXTENT_SIZE << std::dec
     << (is_level_tail() ? "$" : "")
     << "(level=" << (unsigned)level()
     << ", filled=" << node_stage.total_size() - node_stage.free_size() << "B"
     << ", free=" << node_stage.free_size() << "B"
     << ")";
  return os;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
const typename NODE_T::value_t* NODE_T::get_value_ptr(
    const search_position_t& position) const {
  auto& node_stage = extent.read();
  if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
    if (position.is_end()) {
      assert(is_level_tail());
      return node_stage.get_end_p_laddr();
    }
  } else {
    assert(!position.is_end());
  }
  return STAGE_T::get_p_value(node_stage, cast_down<STAGE_T::STAGE>(position));
}

#ifndef NDEBUG
template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
void NODE_T::test_make_destructable(
    context_t c, NodeExtentMutable& mut, Super::URef&& _super) {
  node_stage_t::update_is_level_tail(mut, extent.read(), true);
  make_root(c, std::move(_super));
}
#endif

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
Ref<Node> NODE_T::load(NodeExtent::Ref extent, bool expect_is_level_tail) {
  Ref<ConcreteType> ret = new ConcreteType();
  ret->extent = extent_t::load(extent);
  assert(ret->is_level_tail() == expect_is_level_tail);
  return ret;
}

#define I_NODE_T InternalNodeT<FieldType, ConcreteType>
template class InternalNodeT<node_fields_0_t, InternalNode0>;
template class InternalNodeT<node_fields_1_t, InternalNode1>;
template class InternalNodeT<node_fields_2_t, InternalNode2>;
template class InternalNodeT<internal_fields_3_t, InternalNode3>;

template <typename FieldType, typename ConcreteType>
node_future<Node::search_result_t> I_NODE_T::do_lower_bound(
    context_t c, const full_key_t<KeyT::HOBJ>& key, MatchHistory& history) {
  auto result = STAGE_T::lower_bound_normalized(
      this->extent.read(), key, history);
  auto& position = result.position;
  laddr_t child_addr;
  if (position.is_end()) {
    assert(this->is_level_tail());
    child_addr = *this->get_value_ptr(position);
  } else {
    assert(result.p_value);
    child_addr = *result.p_value;
  }
  return get_or_track_child(c, position, child_addr
  ).safe_then([c, &key, &history](auto child) {
    // XXX(multi-type): pass result.mstat to child
    return child->do_lower_bound(c, key, history);
  });
}

template <typename FieldType, typename ConcreteType>
node_future<> I_NODE_T::apply_child_split(
    // TODO(cross-node string dedup)
    context_t c, const search_position_t& pos,
    const full_key_t<KeyT::VIEW>& left_key, Ref<Node> left_child,
    Ref<Node> right_child) {
  this->extent.prepare_mutate(c);
  auto& node_stage = this->extent.read();

  // update pos => l_addr to r_addr
  const laddr_t* p_rvalue = this->get_value_ptr(pos);
  auto left_laddr = left_child->laddr();
  auto right_laddr = right_child->laddr();
  assert(*p_rvalue == left_laddr);

  // TODO: must move to replayable interfaces
  this->extent.unreplayable_mutate().copy_in_absolute(
      const_cast<laddr_t*>(p_rvalue), right_laddr);

  this->replace_track(pos, left_child, right_child);

  // evaluate insertion
  position_t insert_pos = cast_down<STAGE_T::STAGE>(pos);
  match_stage_t insert_stage;
  node_offset_t insert_size;
  if (unlikely(!node_stage.keys())) {
    assert(insert_pos.is_end());
    insert_stage = STAGE_T::STAGE;
    insert_size = STAGE_T::template insert_size<KeyT::VIEW>(left_key, left_laddr);
  } else {
    std::tie(insert_stage, insert_size) =
      STAGE_T::evaluate_insert(node_stage, left_key, left_laddr, insert_pos, true);
  }

  // TODO: common part begin, move to NodeT
  auto free_size = node_stage.free_size();
  if (free_size >= insert_size) {
    auto p_value = this->extent.template insert_replayable<KeyT::VIEW>(
        left_key, left_laddr, insert_pos, insert_stage, insert_size);
    assert(node_stage.free_size() == free_size - insert_size);
    // TODO: common part end, move to NodeT

    assert(*p_value == left_laddr);
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    assert(insert_pos_normalized <= pos);
    assert(get_key_view(insert_pos_normalized) == left_key);
    track_insert(insert_pos_normalized, insert_stage, left_child, right_child);
    this->validate_tracked_children();
    return node_ertr::now();
  }

  std::cout << "  try insert at: " << insert_pos
            << ", insert_stage=" << (int)insert_stage
            << ", insert_size=" << insert_size
            << ", values=0x" << std::hex << left_laddr
            << ",0x" << right_laddr << std::dec << std::endl;

  Ref<I_NODE_T> this_ref = this;
  return (is_root() ? this->upgrade_root(c) : node_ertr::now()
  ).safe_then([this, c] {
    return ConcreteType::allocate(c, this->level(), this->is_level_tail());
  }).safe_then([this_ref, this, c, left_key, left_child, right_child, left_laddr,
                insert_pos, insert_stage, insert_size](auto fresh_right) mutable {
    auto& node_stage = this->extent.read();
    size_t empty_size = node_stage.size_before(0);
    size_t available_size = node_stage.total_size() - empty_size;
    size_t target_split_size = empty_size + (available_size + insert_size) / 2;
    // TODO adjust NODE_BLOCK_SIZE according to this requirement
    assert(insert_size < available_size / 2);
    typename STAGE_T::StagedIterator split_at;
    bool insert_left = STAGE_T::locate_split(
        node_stage, target_split_size, insert_pos, insert_stage, insert_size, split_at);

    std::cout << "  split at: " << split_at << ", insert_left=" << insert_left
              << ", now insert at: " << insert_pos
              << std::endl;

    auto append_at = split_at;
    // TODO(cross-node string dedup)
    typename STAGE_T::template StagedAppender<KeyT::VIEW> right_appender;
    right_appender.init(&fresh_right.mut, fresh_right.mut.get_write());
    const laddr_t* p_value = nullptr;
    if (!insert_left) {
      // right node: append [start(append_at), insert_pos)
      STAGE_T::template append_until<KeyT::VIEW>(
          append_at, right_appender, insert_pos, insert_stage);
      std::cout << "insert to right: " << insert_pos
                << ", insert_stage=" << (int)insert_stage << std::endl;
      // right node: append [insert_pos(key, value)]
      bool is_front_insert = (insert_pos == position_t::begin());
      bool is_end = STAGE_T::template append_insert<KeyT::VIEW>(
          left_key, left_laddr, append_at, right_appender,
          is_front_insert, insert_stage, p_value);
      assert(append_at.is_end() == is_end);
    }

    // right node: append (insert_pos, end)
    auto pos_end = position_t::end();
    STAGE_T::template append_until<KeyT::VIEW>(
        append_at, right_appender, pos_end, STAGE_T::STAGE);
    assert(append_at.is_end());
    right_appender.wrap();
    fresh_right.node->dump(std::cout) << std::endl;

    // mutate left node
    if (insert_left) {
      p_value = this->extent.template split_insert_replayable<KeyT::VIEW>(
          split_at, left_key, left_laddr, insert_pos, insert_stage, insert_size);
    } else {
      this->extent.split_replayable(split_at);
    }
    this->dump(std::cout) << std::endl;
    assert(p_value);
    // TODO: common part end, move to NodeT

    auto split_pos_normalized = normalize(split_at.get_pos());
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    std::cout << "split at " << split_pos_normalized
              << ", insert at " << insert_pos_normalized
              << ", insert_left=" << insert_left
              << ", insert_stage=" << (int)insert_stage << std::endl;
    track_split(split_pos_normalized, fresh_right.node);
    if (insert_left) {
      track_insert(insert_pos_normalized, insert_stage, left_child);
    } else {
      fresh_right.node->track_insert(insert_pos_normalized, insert_stage, left_child);
    }

    this->validate_tracked_children();
    fresh_right.node->validate_tracked_children();

    // propagate index to parent
    return this->insert_parent(c, fresh_right.node);
    // TODO (optimize)
    // try to acquire space from siblings before split... see btrfs
  });
}

#ifndef NDEBUG
template <typename FieldType, typename ConcreteType>
node_future<> I_NODE_T::test_clone_root(
    context_t c_other, Super::URef&& super_other) const {
  assert(is_root());
  assert(is_level_tail());
  assert(field_type() == field_type_t::N0);
  Ref<const I_NODE_T> this_ref = this;
  return InternalNode0::test_allocate_cloned_root(
      c_other, level(), std::move(super_other), this->extent.test_get()
  ).safe_then([this_ref, this, c_other](auto clone) {
    // In some unit tests, the children are stubbed out that they
    // don't exist in NodeExtentManager, and are only tracked in memory.
    return test_clone_children(c_other, clone);
  });
}
#endif

template <typename FieldType, typename ConcreteType>
node_future<typename I_NODE_T::fresh_node_t> I_NODE_T::allocate(
    context_t c, level_t level, bool is_level_tail) {
  assert(level != 0u);
  return extent_t::allocate(c, level, is_level_tail
  ).safe_then([](auto&& fresh_extent) {
    auto ret = Ref<ConcreteType>(new ConcreteType());
    ret->extent = std::move(fresh_extent.extent);
    return fresh_node_t{ret, fresh_extent.mut};
  });
}

// TODO: bootstrap extent
node_future<Ref<InternalNode0>> InternalNode0::allocate_root(
    context_t c, level_t old_root_level,
    laddr_t old_root_addr, Super::URef&& _super) {
  return allocate(c, old_root_level + 1, true
  ).safe_then([c, old_root_addr,
               _super = std::move(_super)](auto fresh_root) mutable {
    auto root = fresh_root.node;
    const laddr_t* p_value = root->get_value_ptr(search_position_t::end());
    fresh_root.mut.copy_in_absolute(
        const_cast<laddr_t*>(p_value), old_root_addr);
    root->make_root_from(c, std::move(_super), old_root_addr);
    return root;
  });
}

#ifndef NDEBUG
node_future<Ref<InternalNode0>> InternalNode0::test_allocate_cloned_root(
    context_t c, level_t level, Super::URef&& super,
    const NodeExtent& from_extent) {
  // NOTE: from_extent should be alive during allocate(...)
  return allocate(c, level, true
  ).safe_then([c, super = std::move(super), &from_extent](auto fresh_node) mutable {
    auto clone = fresh_node.node;
    clone->make_root_new(c, std::move(super));
    fresh_node.mut.test_copy_from(from_extent);
    return clone;
  });
}
#endif

#define L_NODE_T LeafNodeT<FieldType, ConcreteType>
template class LeafNodeT<node_fields_0_t, LeafNode0>;
template class LeafNodeT<node_fields_1_t, LeafNode1>;
template class LeafNodeT<node_fields_2_t, LeafNode2>;
template class LeafNodeT<leaf_fields_3_t, LeafNode3>;

template <typename FieldType, typename ConcreteType>
node_future<Node::search_result_t> L_NODE_T::do_lower_bound(
    context_t, const full_key_t<KeyT::HOBJ>& key, MatchHistory& history) {
  auto& node_stage = this->extent.read();
  if (unlikely(node_stage.keys() == 0)) {
    assert(this->is_root());
    history.set<STAGE_LEFT>(MatchKindCMP::NE);
    auto p_cursor = get_or_track_cursor(search_position_t::end(), nullptr);
    return node_ertr::make_ready_future<search_result_t>(
        search_result_t{p_cursor, MatchKindBS::NE});
  }

  auto result = STAGE_T::lower_bound_normalized(node_stage, key, history);
  if (result.is_end()) {
    assert(this->is_level_tail());
  } else {
    assert(result.p_value);
  }
  auto p_cursor = get_or_track_cursor(result.position, result.p_value);
  return node_ertr::make_ready_future<search_result_t>(
      search_result_t{p_cursor, result.match()});
}

template <typename FieldType, typename ConcreteType>
node_future<Ref<tree_cursor_t>> L_NODE_T::lookup_smallest(context_t) {
  auto& node_stage = this->extent.read();
  if (unlikely(node_stage.keys() == 0)) {
    assert(this->is_root());
    auto pos = search_position_t::end();
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        get_or_track_cursor(pos, nullptr));
  }

  auto pos = search_position_t::begin();
  const onode_t* p_value = this->get_value_ptr(pos);
  return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
      get_or_track_cursor(pos, p_value));
}

template <typename FieldType, typename ConcreteType>
node_future<Ref<tree_cursor_t>> L_NODE_T::lookup_largest(context_t) {
  auto& node_stage = this->extent.read();
  if (unlikely(node_stage.keys() == 0)) {
    assert(this->is_root());
    auto pos = search_position_t::end();
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        get_or_track_cursor(pos, nullptr));
  }

  search_position_t pos;
  const onode_t* p_value = nullptr;
  STAGE_T::lookup_largest_normalized(node_stage, pos, p_value);
  return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
      get_or_track_cursor(pos, p_value));
}

template <typename FieldType, typename ConcreteType>
node_future<Ref<tree_cursor_t>> L_NODE_T::insert_value(
    context_t c, const full_key_t<KeyT::HOBJ>& key, const onode_t& value,
    const search_position_t& pos, const MatchHistory& history) {
#ifndef NDEBUG
  if (pos.is_end()) {
    assert(this->is_level_tail());
  }
#endif
  this->extent.prepare_mutate(c);
  auto& node_stage = this->extent.read();

  position_t insert_pos = cast_down<STAGE_T::STAGE>(pos);
  auto [insert_stage, insert_size] =
    STAGE_T::evaluate_insert(key, value, history, insert_pos);

  // TODO: common part begin, move to NodeT
  auto free_size = node_stage.free_size();
  if (free_size >= insert_size) {
    auto p_value = this->extent.template insert_replayable<KeyT::HOBJ>(
        key, value, insert_pos, insert_stage, insert_size);
    assert(node_stage.free_size() == free_size - insert_size);
    // TODO: common part end, move to NodeT

    assert(p_value->size == value.size);
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    assert(insert_pos_normalized <= pos);
    assert(get_key_view(insert_pos_normalized) == key);
    auto ret = track_insert(insert_pos_normalized, insert_stage, p_value);
    this->validate_tracked_cursors();
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(ret);
  }

  std::cout << "  try insert at: " << insert_pos
            << ", insert_stage=" << (int)insert_stage
            << ", insert_size=" << insert_size
            << std::endl;

  Ref<L_NODE_T> this_ref = this;
  return (is_root() ? this->upgrade_root(c) : node_ertr::now()
  ).safe_then([this, c] {
    return ConcreteType::allocate(c, this->is_level_tail());
  }).safe_then([this_ref, this, c, &key, &value, &history,
                insert_pos, insert_stage, insert_size](auto fresh_right) mutable {
    auto& node_stage = this->extent.read();
    size_t empty_size = node_stage.size_before(0);
    size_t available_size = node_stage.total_size() - empty_size;
    size_t target_split_size = empty_size + (available_size + insert_size) / 2;
    // TODO adjust NODE_BLOCK_SIZE according to this requirement
    assert(insert_size < available_size / 2);
    typename STAGE_T::StagedIterator split_at;
    bool insert_left = STAGE_T::locate_split(
        node_stage, target_split_size, insert_pos, insert_stage, insert_size, split_at);

    std::cout << "  split at: " << split_at << ", insert_left=" << insert_left
              << ", now insert at: " << insert_pos
              << std::endl;

    auto append_at = split_at;
    // TODO(cross-node string dedup)
    typename STAGE_T::template StagedAppender<KeyT::HOBJ> right_appender;
    right_appender.init(&fresh_right.mut, fresh_right.mut.get_write());
    const onode_t* p_value = nullptr;
    if (!insert_left) {
      // right node: append [start(append_at), insert_pos)
      STAGE_T::template append_until<KeyT::HOBJ>(
          append_at, right_appender, insert_pos, insert_stage);
      std::cout << "insert to right: " << insert_pos
                << ", insert_stage=" << (int)insert_stage << std::endl;
      // right node: append [insert_pos(key, value)]
      bool is_front_insert = (insert_pos == position_t::begin());
      bool is_end = STAGE_T::template append_insert<KeyT::HOBJ>(
          key, value, append_at, right_appender,
          is_front_insert, insert_stage, p_value);
      assert(append_at.is_end() == is_end);
    }

    // right node: append (insert_pos, end)
    auto pos_end = position_t::end();
    STAGE_T::template append_until<KeyT::HOBJ>(
        append_at, right_appender, pos_end, STAGE_T::STAGE);
    assert(append_at.is_end());
    right_appender.wrap();
    fresh_right.node->dump(std::cout) << std::endl;

    // mutate left node
    if (insert_left) {
      p_value = this->extent.template split_insert_replayable<KeyT::HOBJ>(
          split_at, key, value, insert_pos, insert_stage, insert_size);
    } else {
      this->extent.split_replayable(split_at);
    }
    this->dump(std::cout) << std::endl;
    assert(p_value);
    // TODO: common part end, move to NodeT

    auto split_pos_normalized = normalize(split_at.get_pos());
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    std::cout << "split at " << split_pos_normalized
              << ", insert at " << insert_pos_normalized
              << ", insert_left=" << insert_left
              << ", insert_stage=" << (int)insert_stage << std::endl;
    track_split(split_pos_normalized, fresh_right.node);
    Ref<tree_cursor_t> ret;
    if (insert_left) {
      assert(this->get_key_view(insert_pos_normalized) == key);
      ret = track_insert(insert_pos_normalized, insert_stage, p_value);
    } else {
      assert(fresh_right.node->get_key_view(insert_pos_normalized) == key);
      ret = fresh_right.node->track_insert(insert_pos_normalized, insert_stage, p_value);
    }

    this->validate_tracked_cursors();
    fresh_right.node->validate_tracked_cursors();

    // propagate index to parent
    return this->insert_parent(c, fresh_right.node).safe_then([ret] {
      return ret;
    });
    // TODO (optimize)
    // try to acquire space from siblings before split... see btrfs
  });
}

#ifndef NDEBUG
template <typename FieldType, typename ConcreteType>
node_future<> L_NODE_T::test_clone_root(
    context_t c_other, Super::URef&& super_other) const {
  assert(this->is_root());
  assert(is_level_tail());
  assert(field_type() == field_type_t::N0);
  Ref<const L_NODE_T> this_ref = this;
  return LeafNode0::test_allocate_cloned_root(
      c_other, std::move(super_other), this->extent.test_get()
  ).safe_then([this_ref](auto clone) {});
}

node_future<Ref<LeafNode0>> LeafNode0::test_allocate_cloned_root(
    context_t c, Super::URef&& super, const NodeExtent& from_extent) {
  // NOTE: from_extent should be alive during allocate(...)
  return allocate(c, true
  ).safe_then([c, super = std::move(super), &from_extent](auto fresh_node) mutable {
    auto clone = fresh_node.node;
    clone->make_root_new(c, std::move(super));
    fresh_node.mut.test_copy_from(from_extent);
    return clone;
  });
}
#endif

template <typename FieldType, typename ConcreteType>
node_future<typename L_NODE_T::fresh_node_t> L_NODE_T::allocate(
    context_t c, bool is_level_tail) {
  return extent_t::allocate(c, 0u, is_level_tail
  ).safe_then([](auto&& fresh_extent) {
    auto ret = Ref<ConcreteType>(new ConcreteType());
    ret->extent = std::move(fresh_extent.extent);
    return fresh_node_t{ret, fresh_extent.mut};
  });
}

}
