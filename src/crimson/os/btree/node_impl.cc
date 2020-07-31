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
laddr_t NODE_T::laddr() const { return _extent->get_laddr(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
level_t NODE_T::level() const { return stage().level(); }

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
full_key_t<KeyT::VIEW> NODE_T::get_key_view(
    const search_position_t& position) const {
  auto _stage = stage();
  full_key_t<KeyT::VIEW> ret;
  STAGE_T::get_key_view_normalized(_stage, position, ret);
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
  return node_stage_t(_extent->get_ptr<FieldType>(0u), &_is_level_tail);
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
  return STAGE_T::get_p_value_normalized(_stage, position);
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
Ref<ConcreteType> NODE_T::_allocate(level_t level, bool level_tail) {
  // might be asynchronous
  auto extent = get_transaction_manager().alloc_extent(node_stage_t::EXTENT_SIZE);
  extent->copy_in(node_header_t{FIELD_TYPE, NODE_TYPE, level}, 0u);
  extent->copy_in(typename FieldType::num_keys_t(0u), sizeof(node_header_t));
  auto ret = Ref<ConcreteType>(new ConcreteType());
  ret->init(extent, level_tail);
#ifndef NDEBUG
  // ret->stage().fields().template fill_unused<NODE_TYPE>(is_level_tail, *extent);
#endif
  return ret;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
void NODE_T::init(
    Ref<LogicalCachedExtent> block_extent, bool b_level_tail) {
  assert(!_extent);
  assert(node_stage_t::EXTENT_SIZE == block_extent->get_length());
  _extent = block_extent;
  _is_level_tail = b_level_tail;
#ifndef NDEBUG
  stage();
#endif
}

#define I_NODE_T InternalNodeT<FieldType, ConcreteType>
template class InternalNodeT<node_fields_0_t, InternalNode0>;
template class InternalNodeT<node_fields_1_t, InternalNode1>;
template class InternalNodeT<node_fields_2_t, InternalNode2>;
template class InternalNodeT<internal_fields_3_t, InternalNode3>;

template <typename FieldType, typename ConcreteType>
Node::search_result_t I_NODE_T::do_lower_bound(
    const full_key_t<KeyT::HOBJ>& key, MatchHistory& history) {
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
  Ref<Node> child = get_or_track_child(position, child_addr);
  // XXX(multi-type): pass result.mstat to child
  return child->do_lower_bound(key, history);
}

template <typename FieldType, typename ConcreteType>
void I_NODE_T::apply_child_split(
    // TODO(cross-node string dedup)
    const full_key_t<KeyT::VIEW>& left_key,
    Ref<Node> left_child, Ref<Node> right_child) {
  auto [pos, ptr] = left_child->parent_info();
  assert(ptr.get() == this);
  assert(left_key == left_child->get_largest_key_view());

  // update pos => l_addr to r_addr
  const laddr_t* p_rvalue = this->get_value_ptr(pos);
  auto left_laddr = left_child->laddr();
  auto right_laddr = right_child->laddr();
  assert(*p_rvalue == left_laddr);
  this->extent().copy_in_mem(right_laddr, const_cast<laddr_t*>(p_rvalue));

  // update track pos => left_child to right_child
  assert(tracked_child_nodes[pos] == left_child);
  tracked_child_nodes.erase(pos);
  track_child(pos, right_child);

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
    auto p_value = STAGE_T::template proceed_insert<KeyT::VIEW, false>(
        this->extent(), stage, left_key, left_laddr,
        insert_pos, insert_stage, insert_size);
    assert(stage.free_size() == free_size - insert_size);
    // TODO: common part end, move to NodeT

    assert(*p_value == left_laddr);
    auto insert_pos_normalized = normalize(std::move(insert_pos));
    assert(insert_pos_normalized <= pos);
    assert(get_key_view(insert_pos_normalized) == left_key);
    track_insert(insert_pos_normalized, insert_stage, left_child);

#ifndef NDEBUG
    // verify correctness of tracked children
    for (auto& kv : tracked_child_nodes) {
      assert(this == kv.second->parent_info().ptr);
      assert(kv.first == kv.second->parent_info().position);
      assert(*this->get_value_ptr(kv.first) == kv.second->laddr());
      if (kv.first.is_end()) {
        assert(this->is_level_tail());
        assert(kv.second->is_level_tail());
      } else {
        assert(!kv.second->is_level_tail());
        assert(get_key_view(kv.first) == kv.second->get_largest_key_view());
      }
    }

    // validate left_child is before right_child
    auto iter = tracked_child_nodes.find(insert_pos_normalized);
    ++iter;
    assert(iter->second == right_child);
    if (!right_child->is_level_tail()) {
      assert(!iter->first.is_end());
      assert(get_key_view(iter->first) == right_child->get_largest_key_view());
    } else {
      assert(iter->first.is_end());
    }
#endif
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
    InternalNode0::upgrade_root(this);
  }
  auto right_node = ConcreteType::allocate(this->level(), this->is_level_tail());

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

  // left node: trim
  this->set_level_tail(false);
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
#ifndef NDEBUG
  // verify correctness of tracked children
  for (auto& kv : tracked_child_nodes) {
    assert(this == kv.second->parent_info().ptr);
    assert(kv.first == kv.second->parent_info().position);
    assert(*this->get_value_ptr(kv.first) == kv.second->laddr());
    assert(get_key_view(kv.first) == kv.second->get_largest_key_view());
  }
  for (auto& kv : right_node->tracked_child_nodes) {
    assert(right_node == kv.second->parent_info().ptr);
    assert(kv.first == kv.second->parent_info().position);
    assert(*right_node->get_value_ptr(kv.first) == kv.second->laddr());
    if (kv.first.is_end()) {
      assert(right_node->is_level_tail());
      assert(kv.second->is_level_tail());
    } else {
      assert(!kv.second->is_level_tail());
      assert(right_node->get_key_view(kv.first) == kv.second->get_largest_key_view());
    }
  }
#endif

  // propagate index to parent
  // TODO(cross-node string dedup)
  auto key_view = get_largest_key_view();
  this->parent_info().ptr->apply_child_split(key_view, this, right_node);

  // TODO (optimize)
  // try to acquire space from siblings before split... see btrfs
}

#ifndef NDEBUG
template <typename FieldType, typename ConcreteType>
Ref<Node> I_NODE_T::test_clone(Ref<Node>& parent) const {
  Ref<ConcreteType> ret = ConcreteType::allocate(level(), is_level_tail());
  if (this->is_root()) {
    assert(!parent);
    ret->as_root(parent);
  } else {
    assert(parent);
    auto _parent = boost::dynamic_pointer_cast<InternalNode>(parent);
    ret->as_child({this->parent_info().position, _parent});
  }
  ret->extent().copy_from(this->extent());
  for (auto& kv : tracked_child_nodes) {
    Ref<Node> _ret = ret;
    Ref<Node> child = kv.second->test_clone(_ret);
    ret->tracked_child_nodes[kv.first] = child;
  }
  return ret;
}
#endif

template <typename FieldType, typename ConcreteType>
void I_NODE_T::track_insert(
    const search_position_t& insert_pos, match_stage_t insert_stage,
    Ref<Node> insert_child) {
  // update tracks
  auto pos_upper_bound = insert_pos;
  pos_upper_bound.index_by_stage(insert_stage) = INDEX_END;
  auto first = tracked_child_nodes.lower_bound(insert_pos);
  auto last = tracked_child_nodes.lower_bound(pos_upper_bound);
  std::vector<Ref<Node>> nodes;
  std::for_each(first, last, [&nodes](auto& kv) {
    nodes.push_back(kv.second);
  });
  tracked_child_nodes.erase(first, last);
  for (auto& node : nodes) {
    auto _pos = node->parent_info().position;
    ++_pos.index_by_stage(insert_stage);
    assert(!node->is_level_tail());
    assert(get_key_view(_pos) == node->get_largest_key_view());
    track_child(_pos, node);
  }
  // track insert
  track_child(insert_pos, insert_child);
}

template <typename FieldType, typename ConcreteType>
void I_NODE_T::track_split(
    const search_position_t& split_pos, Ref<ConcreteType> right_node) {
  auto first = tracked_child_nodes.lower_bound(split_pos);
  auto iter = first;
  while (iter != tracked_child_nodes.end()) {
    search_position_t new_pos = iter->first;
    new_pos -= split_pos;
    // right_node->track_child(new_pos, iter->second);
    right_node->tracked_child_nodes[new_pos] = iter->second;
    iter->second->as_child({new_pos, right_node});
    ++iter;
  }
  tracked_child_nodes.erase(first, tracked_child_nodes.end());
}

template <typename FieldType, typename ConcreteType>
Ref<Node> I_NODE_T::get_or_track_child(
    const search_position_t& position, laddr_t child_addr) {
  Ref<Node> child;
  auto found = tracked_child_nodes.find(position);
  if (found == tracked_child_nodes.end()) {
    child = Node::load(child_addr,
                       position.is_end());
    track_child(position, child);
  } else {
    child = found->second;
  }
  assert(child_addr == child->laddr());
  assert(position == child->parent_info().position);
  assert(this == child->parent_info().ptr);
  assert(this->level() - 1 == child->level());
  // XXX(multi-type)
  assert(this->field_type() <= child->field_type());
#ifndef NDEBUG
  if (position.is_end()) {
    assert(this->is_level_tail());
    assert(child->is_level_tail());
  } else {
    assert(!child->is_level_tail());
    assert(get_key_view(position) == child->get_largest_key_view());
  }
#endif
  return child;
}

void InternalNode0::upgrade_root(Ref<Node> root) {
  assert(root->is_root());
  auto new_root = allocate(root->level() + 1, true);
  auto pos = search_position_t::end();
  const laddr_t* p_value = new_root->get_value_ptr(pos);
  new_root->extent().copy_in_mem(root->laddr(), const_cast<laddr_t*>(p_value));
  root->handover_root(new_root);
  new_root->track_child(pos, root);
}

#define L_NODE_T LeafNodeT<FieldType, ConcreteType>
template class LeafNodeT<node_fields_0_t, LeafNode0>;
template class LeafNodeT<node_fields_1_t, LeafNode1>;
template class LeafNodeT<node_fields_2_t, LeafNode2>;
template class LeafNodeT<leaf_fields_3_t, LeafNode3>;

template <typename FieldType, typename ConcreteType>
Node::search_result_t L_NODE_T::do_lower_bound(
    const full_key_t<KeyT::HOBJ>& key, MatchHistory& history) {
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
Ref<tree_cursor_t> L_NODE_T::lookup_smallest() {
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
Ref<tree_cursor_t> L_NODE_T::lookup_largest() {
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
    const full_key_t<KeyT::HOBJ>& key, const onode_t& value,
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

#ifndef NDEBUG
    // verify correctness of tracked cursors
    for (auto& kv : tracked_cursors) {
      assert(this == kv.second->get_leaf_node());
      assert(kv.first == kv.second->get_position());
      assert(this->get_value_ptr(kv.first) == kv.second->get_p_value());
    }
#endif
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
    InternalNode0::upgrade_root(this);
  }
  auto right_node = ConcreteType::allocate(this->is_level_tail());

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

  // left node: trim
  this->set_level_tail(false);
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
#ifndef NDEBUG
  // verify correctness of tracked cursors
  for (auto& kv : tracked_cursors) {
    assert(this == kv.second->get_leaf_node());
    assert(kv.first == kv.second->get_position());
    assert(this->get_value_ptr(kv.first) == kv.second->get_p_value());
  }
  for (auto& kv : right_node->tracked_cursors) {
    assert(right_node == kv.second->get_leaf_node());
    assert(kv.first == kv.second->get_position());
    assert(right_node->get_value_ptr(kv.first) == kv.second->get_p_value());
  }
#endif

  // propagate index to parent
  // TODO(cross-node string dedup)
  auto key_view = get_largest_key_view();
  this->parent_info().ptr->apply_child_split(key_view, this, right_node);

  return ret;

  // TODO (optimize)
  // try to acquire space from siblings before split... see btrfs
}

#ifndef NDEBUG
template <typename FieldType, typename ConcreteType>
Ref<Node> L_NODE_T::test_clone(Ref<Node>& parent) const {
  auto ret = ConcreteType::allocate(is_level_tail());
  if (this->is_root()) {
    assert(!parent);
    ret->as_root(parent);
  } else {
    assert(parent);
    auto _parent = boost::dynamic_pointer_cast<InternalNode>(parent);
    ret->as_child({this->parent_info().position, _parent});
  }
  ret->extent().copy_from(this->extent());
  return ret;
}
#endif

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::track_insert(
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

template <typename FieldType, typename ConcreteType>
void L_NODE_T::track_split(
    const search_position_t& split_pos, Ref<ConcreteType> right_node) {
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

template <typename FieldType, typename ConcreteType>
const onode_t* L_NODE_T::get_p_value(const search_position_t& pos) const {
  return this->get_value_ptr(pos);
}

template <typename FieldType, typename ConcreteType>
void L_NODE_T::do_track_cursor(tree_cursor_t& cursor) {
  assert(!cursor.get_position().is_end());
  assert(tracked_cursors.find(cursor.get_position()) == tracked_cursors.end());
  tracked_cursors[cursor.get_position()] = &cursor;
}

template <typename FieldType, typename ConcreteType>
void L_NODE_T::do_untrack_cursor(const tree_cursor_t& cursor) {
  auto removed = tracked_cursors.erase(cursor.get_position());
  assert(removed);
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::get_or_track_cursor(
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

}
