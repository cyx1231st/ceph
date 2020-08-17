// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_impl.h"

// TODO: remove
#include <iostream>
#include "common/likely.h"

#include "dummy_transaction_manager.h"
#include "stages/node_stage.h"
#include "stages/stage.h"

#define STAGE_T node_to_stage_t<node_stage_t>

namespace crimson::os::seastore::onode {

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
bool NODE_T::is_level_tail() const { return stage().is_level_tail(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
laddr_t NODE_T::laddr() const { return _extent->get_laddr(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
level_t NODE_T::level() const { return stage().level(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
full_key_t<KeyT::VIEW> NODE_T::get_key_view(
    const search_position_t& position) const {
  auto _stage = stage();
  full_key_t<KeyT::VIEW> ret;
  STAGE_T::get_key_view(_stage, cast_down<STAGE_T::STAGE>(position), ret);
  return ret;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
full_key_t<KeyT::VIEW> NODE_T::get_largest_key_view() const {
  auto _stage = stage();
  full_key_t<KeyT::VIEW> key_view;
  STAGE_T::lookup_largest_index(_stage, key_view);
  return key_view;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
std::ostream& NODE_T::dump(std::ostream& os) const {
  auto _stage = stage();
  auto p_start = _stage.p_start();
  os << *this << ":";
  os << "\n  header: " << node_stage_t::header_size() << "B";
  size_t size = 0u;
  if (_stage.keys()) {
    STAGE_T::dump(_stage, os, "  ", size, p_start);
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
      auto value_ptr = _stage.get_end_p_laddr();
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
  auto _stage = stage();
  os << "Node" << NODE_TYPE << FIELD_TYPE
     << "@0x" << std::hex << laddr()
     << "+" << node_stage_t::EXTENT_SIZE << std::dec
     << (is_level_tail() ? "$" : "")
     << "(level=" << (unsigned)level()
     << ", filled=" << _stage.total_size() - _stage.free_size() << "B"
     << ", free=" << _stage.free_size() << "B"
     << ")";
  return os;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
typename NODE_T::node_stage_t NODE_T::stage() const {
  return node_stage_t(_extent->get_ptr<FieldType>(0u));
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
LogicalCachedExtent& NODE_T::extent() {
  return *_extent;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
const LogicalCachedExtent& NODE_T::extent() const {
  return *_extent;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
const value_type_t<NODE_TYPE>* NODE_T::get_value_ptr(
    const search_position_t& position) const {
  auto _stage = stage();
  if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
    if (position.is_end()) {
      assert(is_level_tail());
      return _stage.get_end_p_laddr();
    }
  } else {
    assert(!position.is_end());
  }
  return STAGE_T::get_p_value(_stage, cast_down<STAGE_T::STAGE>(position));
}

#ifndef NDEBUG
template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
void NODE_T::test_make_destructable(context_t c, SuperNodeURef&& _super) {
  node_stage_t::update_is_level_tail(extent(), stage(), true);
  make_root(c, std::move(_super));
}
#endif

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
Ref<ConcreteType> NODE_T::_allocate(context_t c, level_t level, bool level_tail) {
  // TODO: bootstrap extent
  auto extent = c.tm.alloc_extent(c.t, node_stage_t::EXTENT_SIZE);
  node_stage_t::bootstrap_extent(
      *extent, FIELD_TYPE, NODE_TYPE, level_tail, level);
  auto ret = Ref<ConcreteType>(new ConcreteType());
  ret->init(extent);
  return ret;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
void NODE_T::init(Ref<LogicalCachedExtent> block_extent) {
  assert(!_extent);
  assert(node_stage_t::EXTENT_SIZE == block_extent->get_length());
  node_stage_t::validate(*block_extent->get_ptr<FieldType>(0u));
  _extent = block_extent;
}

#define I_NODE_T InternalNodeT<FieldType, ConcreteType>
template class InternalNodeT<node_fields_0_t, InternalNode0>;
template class InternalNodeT<node_fields_1_t, InternalNode1>;
template class InternalNodeT<node_fields_2_t, InternalNode2>;
template class InternalNodeT<internal_fields_3_t, InternalNode3>;

template <typename FieldType, typename ConcreteType>
Node::search_result_t I_NODE_T::do_lower_bound(
    context_t c, const full_key_t<KeyT::HOBJ>& key, MatchHistory& history) {
  auto stage = this->stage();
  auto result = STAGE_T::lower_bound_normalized(stage, key, history);
  auto& position = result.position;
  laddr_t child_addr;
  if (position.is_end()) {
    assert(this->is_level_tail());
    child_addr = *this->get_value_ptr(position);
  } else {
    assert(result.p_value);
    child_addr = *result.p_value;
  }
  Ref<Node> child = get_or_track_child(c, position, child_addr);
  // XXX(multi-type): pass result.mstat to child
  return child->do_lower_bound(c, key, history);
}

template <typename FieldType, typename ConcreteType>
void I_NODE_T::apply_child_split(
    // TODO(cross-node string dedup)
    context_t c, const search_position_t& pos,
    const full_key_t<KeyT::VIEW>& left_key, Ref<Node> left_child,
    Ref<Node> right_child) {
  // update pos => l_addr to r_addr
  const laddr_t* p_rvalue = this->get_value_ptr(pos);
  auto left_laddr = left_child->laddr();
  auto right_laddr = right_child->laddr();
  assert(*p_rvalue == left_laddr);
  this->extent().copy_in_mem(right_laddr, const_cast<laddr_t*>(p_rvalue));

  this->replace_track(pos, left_child, right_child);

  // evaluate insertion
  typename STAGE_T::position_t insert_pos = cast_down<STAGE_T::STAGE>(pos);
  match_stage_t insert_stage;
  node_offset_t insert_size;
  auto stage = this->stage();
  if (unlikely(!stage.keys())) {
    assert(insert_pos.is_end());
    insert_stage = STAGE_T::STAGE;
    insert_size = STAGE_T::template insert_size<KeyT::VIEW>(left_key, left_laddr);
  } else {
    std::tie(insert_stage, insert_size) =
      STAGE_T::evaluate_insert(stage, left_key, left_laddr, insert_pos, true);
  }

  // TODO: common part begin, move to NodeT
  auto free_size = stage.free_size();
  if (free_size >= insert_size) {
    // TODO: delta(INSERT, left_key, left_addr, insert_pos, insert_stage, insert_size)
    auto p_value = STAGE_T::template proceed_insert<KeyT::VIEW, false>(
        this->extent(), stage, left_key, left_laddr,
        insert_pos, insert_stage, insert_size);
    assert(stage.free_size() == free_size - insert_size);
    // TODO: common part end, move to NodeT

    assert(*p_value == left_laddr);
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    assert(insert_pos_normalized <= pos);
    assert(get_key_view(insert_pos_normalized) == left_key);
    track_insert(insert_pos_normalized, insert_stage, left_child, right_child);
    this->validate_tracked_children();
    return;
  }

  std::cout << "  try insert at: " << insert_pos
            << ", insert_stage=" << (int)insert_stage << ", insert_size=" << insert_size
            << ", values=0x" << std::hex << left_laddr
            << ",0x" << right_laddr << std::dec << std::endl;

  size_t empty_size = stage.size_before(0);
  size_t available_size = stage.total_size() - empty_size;
  size_t target_split_size = empty_size + (available_size + insert_size) / 2;
  // TODO adjust NODE_BLOCK_SIZE according to this requirement
  assert(insert_size < available_size / 2);
  typename STAGE_T::StagedIterator split_at;
  bool insert_left = STAGE_T::locate_split(
      stage, target_split_size, insert_pos, insert_stage, insert_size, split_at);

  std::cout << "  split at: " << split_at << ", insert_left=" << insert_left
            << ", now insert at: " << insert_pos
            << std::endl;

  if (is_root()) {
    this->upgrade_root(c);
  }
  auto right_node = ConcreteType::allocate(c, this->level(), this->is_level_tail());

  auto append_at = split_at;
  // TODO(cross-node string dedup)
  typename STAGE_T::template StagedAppender<KeyT::VIEW> appender;
  appender.init(&right_node->extent(),
                const_cast<char*>(right_node->stage().p_start()));
  const laddr_t* p_value = nullptr;
  if (!insert_left) {
    // right node: append [start(append_at), insert_pos)
    STAGE_T::template append_until<KeyT::VIEW>(
        append_at, appender, insert_pos, insert_stage);
    std::cout << "insert to right: " << insert_pos
              << ", insert_stage=" << (int)insert_stage << std::endl;
    // right node: append [insert_pos(key, value)]
    bool is_front_insert = (insert_pos == STAGE_T::position_t::begin());
    bool is_end = STAGE_T::template append_insert<KeyT::VIEW>(
        left_key, left_laddr, append_at, appender,
        is_front_insert, insert_stage, p_value);
    assert(append_at.is_end() == is_end);
  }

  // right node: append (insert_pos, end)
  auto pos_end = STAGE_T::position_t::end();
  STAGE_T::template append_until<KeyT::VIEW>(
      append_at, appender, pos_end, STAGE_T::STAGE);
  assert(append_at.is_end());
  appender.wrap();
  right_node->dump(std::cout) << std::endl;

  // TODO: delta(SPLIT, split_at)
  // TODO: delta(SPLIT_INSERT, split_at,
  //             left_key, left_laddr, insert_pos, insert_stage, insert_size)
  // left node: trim
  node_stage_t::update_is_level_tail(this->extent(), stage, false);
  STAGE_T::trim(this->extent(), split_at);

  if (insert_left) {
    // left node: insert
    std::cout << "insert to left: " << insert_pos
              << ", insert_stage=" << (int)insert_stage << std::endl;
    p_value = STAGE_T::template proceed_insert<KeyT::VIEW, true>(
        this->extent(), stage, left_key, left_laddr,
        insert_pos, insert_stage, insert_size);
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
  track_split(split_pos_normalized, right_node);
  if (insert_left) {
    track_insert(insert_pos_normalized, insert_stage, left_child);
  } else {
    right_node->track_insert(insert_pos_normalized, insert_stage, left_child);
  }

  this->validate_tracked_children();
  right_node->validate_tracked_children();

  // propagate index to parent
  this->insert_parent(c, right_node);

  // TODO (optimize)
  // try to acquire space from siblings before split... see btrfs
}

#ifndef NDEBUG
template <typename FieldType, typename ConcreteType>
void I_NODE_T::test_clone_root(
    context_t c_other, SuperNodeURef&& super_other) const {
  assert(is_root());
  assert(is_level_tail());
  assert(field_type() == field_type_t::N0);
  auto clone = InternalNode0::test_allocate_cloned_root(
      c_other, level(), std::move(super_other), this->extent());
  // In some unit tests, the children are stubbed out that they
  // don't exist in TransactionManager, and are only tracked in memory.
  test_clone_children(c_other, clone);
}
#endif

// TODO: bootstrap extent
Ref<InternalNode0> InternalNode0::allocate_root(
    context_t c, level_t old_root_level,
    laddr_t old_root_addr, SuperNodeURef&& _super) {
  auto root = allocate(c, old_root_level + 1, true);
  const laddr_t* p_value = root->get_value_ptr(search_position_t::end());
  root->extent().copy_in_mem(old_root_addr, const_cast<laddr_t*>(p_value));
  root->make_root_from(c, std::move(_super), old_root_addr);
  return root;
}

#ifndef NDEBUG
Ref<InternalNode0> InternalNode0::test_allocate_cloned_root(
    context_t c, level_t level, SuperNodeURef&& super,
    const LogicalCachedExtent& from_extent) {
  auto clone = allocate(c, level, true);
  clone->make_root_new(c, std::move(super));
  clone->extent().copy_from(from_extent);
  return clone;
}
#endif

#define L_NODE_T LeafNodeT<FieldType, ConcreteType>
template class LeafNodeT<node_fields_0_t, LeafNode0>;
template class LeafNodeT<node_fields_1_t, LeafNode1>;
template class LeafNodeT<node_fields_2_t, LeafNode2>;
template class LeafNodeT<leaf_fields_3_t, LeafNode3>;

template <typename FieldType, typename ConcreteType>
Node::search_result_t L_NODE_T::do_lower_bound(
    context_t, const full_key_t<KeyT::HOBJ>& key, MatchHistory& history) {
  auto stage = this->stage();
  if (unlikely(stage.keys() == 0)) {
    assert(this->is_root());
    history.set<STAGE_LEFT>(MatchKindCMP::NE);
    auto p_cursor = get_or_track_cursor(search_position_t::end(), nullptr);
    return {p_cursor, MatchKindBS::NE};
  }

  auto result = STAGE_T::lower_bound_normalized(stage, key, history);
  if (result.is_end()) {
    assert(this->is_level_tail());
  } else {
    assert(result.p_value);
  }
  auto p_cursor = get_or_track_cursor(result.position, result.p_value);
  return {p_cursor, result.match()};
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::lookup_smallest(context_t) {
  auto stage = this->stage();
  if (unlikely(stage.keys() == 0)) {
    assert(this->is_root());
    auto pos = search_position_t::end();
    return get_or_track_cursor(pos, nullptr);
  }

  auto pos = search_position_t::begin();
  const onode_t* p_value = this->get_value_ptr(pos);
  return get_or_track_cursor(pos, p_value);
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::lookup_largest(context_t) {
  auto stage = this->stage();
  if (unlikely(stage.keys() == 0)) {
    assert(this->is_root());
    auto pos = search_position_t::end();
    return get_or_track_cursor(pos, nullptr);
  }

  search_position_t pos;
  const onode_t* p_value = nullptr;
  STAGE_T::lookup_largest_normalized(stage, pos, p_value);
  return get_or_track_cursor(pos, p_value);
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::insert_value(
    context_t c, const full_key_t<KeyT::HOBJ>& key, const onode_t& value,
    const search_position_t& pos, const MatchHistory& history) {
#ifndef NDEBUG
  if (pos.is_end()) {
    assert(this->is_level_tail());
  }
#endif

  typename STAGE_T::position_t insert_pos = cast_down<STAGE_T::STAGE>(pos);
  auto [insert_stage, insert_size] =
    STAGE_T::evaluate_insert(key, value, history, insert_pos);

  auto stage = this->stage();

  // TODO: common part begin, move to NodeT
  auto free_size = stage.free_size();
  if (free_size >= insert_size) {
    // TODO: delta(INSERT, key, value, insert_pos, insert_stage, insert_size)
    auto p_value = STAGE_T::template proceed_insert<KeyT::HOBJ, false>(
        this->extent(), stage, key, value,
        insert_pos, insert_stage, insert_size);
    assert(stage.free_size() == free_size - insert_size);
    // TODO: common part end, move to NodeT

    assert(p_value->size == value.size);
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    assert(insert_pos_normalized <= pos);
    assert(get_key_view(insert_pos_normalized) == key);
    auto ret = track_insert(insert_pos_normalized, insert_stage, p_value);
    this->validate_tracked_cursors();
    return ret;
  }

  std::cout << "  try insert at: " << insert_pos
            << ", insert_stage=" << (int)insert_stage << ", insert_size=" << insert_size
            << std::endl;

  size_t empty_size = stage.size_before(0);
  size_t available_size = stage.total_size() - empty_size;
  size_t target_split_size = empty_size + (available_size + insert_size) / 2;
  // TODO adjust NODE_BLOCK_SIZE according to this requirement
  assert(insert_size < available_size / 2);
  typename STAGE_T::StagedIterator split_at;
  bool insert_left = STAGE_T::locate_split(
      stage, target_split_size, insert_pos, insert_stage, insert_size, split_at);

  std::cout << "  split at: " << split_at << ", insert_left=" << insert_left
            << ", now insert at: " << insert_pos
            << std::endl;

  if (is_root()) {
    this->upgrade_root(c);
  }
  auto right_node = ConcreteType::allocate(c, this->is_level_tail());

  auto append_at = split_at;
  // TODO(cross-node string dedup)
  typename STAGE_T::template StagedAppender<KeyT::HOBJ> appender;
  appender.init(&right_node->extent(),
                const_cast<char*>(right_node->stage().p_start()));
  const onode_t* p_value = nullptr;
  if (!insert_left) {
    // right node: append [start(append_at), insert_pos)
    STAGE_T::template append_until<KeyT::HOBJ>(
        append_at, appender, insert_pos, insert_stage);
    std::cout << "insert to right: " << insert_pos
              << ", insert_stage=" << (int)insert_stage << std::endl;
    // right node: append [insert_pos(key, value)]
    bool is_front_insert = (insert_pos == STAGE_T::position_t::begin());
    bool is_end = STAGE_T::template append_insert<KeyT::HOBJ>(
        key, value, append_at, appender,
        is_front_insert, insert_stage, p_value);
    assert(append_at.is_end() == is_end);
  }

  // right node: append (insert_pos, end)
  auto pos_end = STAGE_T::position_t::end();
  STAGE_T::template append_until<KeyT::HOBJ>(
      append_at, appender, pos_end, STAGE_T::STAGE);
  assert(append_at.is_end());
  appender.wrap();
  right_node->dump(std::cout) << std::endl;

  // TODO: delta(SPLIT, split_at)
  // TODO: delta(SPLIT_INSERT, split_at,
  //             key, value, insert_pos, insert_stage, insert_size)
  // left node: trim
  node_stage_t::update_is_level_tail(this->extent(), stage, false);
  STAGE_T::trim(this->extent(), split_at);

  if (insert_left) {
    // left node: insert
    std::cout << "insert to left: " << insert_pos
              << ", insert_stage=" << (int)insert_stage << std::endl;
    p_value = STAGE_T::template proceed_insert<KeyT::HOBJ, true>(
        this->extent(), stage, key, value,
        insert_pos, insert_stage, insert_size);
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
  track_split(split_pos_normalized, right_node);
  Ref<tree_cursor_t> ret;
  if (insert_left) {
    assert(this->get_key_view(insert_pos_normalized) == key);
    ret = track_insert(insert_pos_normalized, insert_stage, p_value);
  } else {
    assert(right_node->get_key_view(insert_pos_normalized) == key);
    ret = right_node->track_insert(insert_pos_normalized, insert_stage, p_value);
  }

  this->validate_tracked_cursors();
  right_node->validate_tracked_cursors();

  // propagate index to parent
  this->insert_parent(c, right_node);

  return ret;

  // TODO (optimize)
  // try to acquire space from siblings before split... see btrfs
}

#ifndef NDEBUG
template <typename FieldType, typename ConcreteType>
void L_NODE_T::test_clone_root(context_t c_other, SuperNodeURef&& super_other) const {
  assert(this->is_root());
  assert(is_level_tail());
  assert(field_type() == field_type_t::N0);
  auto clone = LeafNode0::test_allocate_cloned_root(
      c_other, std::move(super_other), this->extent());
}

Ref<LeafNode0> LeafNode0::test_allocate_cloned_root(
    context_t c, SuperNodeURef&& super, const LogicalCachedExtent& from_extent) {
  auto clone = allocate(c, true);
  clone->make_root_new(c, std::move(super));
  clone->extent().copy_from(from_extent);
  return clone;
}
#endif

}
