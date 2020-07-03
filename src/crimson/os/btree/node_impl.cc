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
index_view_t NODE_T::get_index_view(
    const search_position_t& position) const {
  auto _stage = stage();
  index_view_t ret;
  STAGE_T::get_index_view_normalized(_stage, position, ret);
  return ret;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
std::ostream& NODE_T::dump(std::ostream& os) const {
  auto _stage = stage();
  os << *this << ":";
  if (_stage.keys()) {
    os << "\nheader: " << _stage.size_before(0u) << "B";
    size_t size = 0u;
    return STAGE_T::dump(_stage, os, "", size);
  } else {
    return os << " empty!";
  }
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

#ifndef NDEBUG
template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
Ref<Node> NODE_T::test_clone() const {
  auto ret = ConcreteType::_allocate(level(), is_level_tail());
  ret->extent().copy_in(_extent->get_ptr<void>(0u), 0u, node_stage_t::EXTENT_SIZE);
  return ret;
}
#endif

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
typename NODE_T::node_stage_t NODE_T::stage() const {
  return node_stage_t(_extent->get_ptr<FieldType>(0u), &_is_level_tail);
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
LogicalCachedExtent& NODE_T::extent() {
  return *_extent;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
const value_type_t<NODE_TYPE>* NODE_T::get_value_ptr(
    const search_position_t& position) {
  auto _stage = stage();
  if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
    if (position.is_end()) {
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
  ret->init(extent, level_tail, nullptr);
#ifndef NDEBUG
  // ret->stage().fields().template fill_unused<NODE_TYPE>(is_level_tail, *extent);
#endif
  return ret;
}

template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
void NODE_T::init(Ref<LogicalCachedExtent> block_extent,
                  bool b_level_tail,
                  const parent_info_t* p_info) {
  assert(!_parent_info.has_value());
  if (p_info) {
   _parent_info = *p_info;
  }
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
Node::search_result_t I_NODE_T::lower_bound(
    const onode_key_t& key, MatchHistory& history) {
  auto stage = this->stage();
  auto ret = STAGE_T::lower_bound_normalized(stage, key, history);

  auto& position = ret.position;
  laddr_t child_addr;
  if (position.is_end()) {
    assert(this->is_level_tail());
    child_addr = *this->get_value_ptr(position);
  } else {
    assert(ret.p_value);
    child_addr = *ret.p_value;
  }

  Ref<Node> child = get_or_load_child(child_addr, position);
  match_stat_t mstat = ret.mstat;
  if (matchable(child->field_type(), mstat)) {
    return child->lower_bound(key, history);
  } else {
    // out of lookup range due to prefix compression
    auto&& ret = child->lookup_smallest();
    return {std::move(ret), MatchKindBS::NE};
  }
}

template <typename FieldType, typename ConcreteType>
Ref<Node> I_NODE_T::get_or_load_child(
    laddr_t child_addr, const search_position_t& position) {
  Ref<Node> child;
  auto found = tracked_child_nodes.find(position);
  if (found == tracked_child_nodes.end()) {
    child = Node::load(child_addr,
                       position.is_end(),
                       {position, this});
    tracked_child_nodes.insert({position, child});
  } else {
    child = found->second;
    assert(child_addr == child->laddr());
    assert(position == child->parent_info().position);
    assert(this == child->parent_info().ptr);
#ifndef NDEBUG
    if (position.is_end()) {
      assert(child->is_level_tail());
    }
#endif
  }
  assert(this->level() - 1 == child->level());
  assert(this->field_type() <= child->field_type());
  assert(child->get_index_view(search_position_t::begin()).match_parent(
        this->get_index_view(position)));
  return child;
}

#define L_NODE_T LeafNodeT<FieldType, ConcreteType>
template class LeafNodeT<node_fields_0_t, LeafNode0>;
template class LeafNodeT<node_fields_1_t, LeafNode1>;
template class LeafNodeT<node_fields_2_t, LeafNode2>;
template class LeafNodeT<leaf_fields_3_t, LeafNode3>;

template <typename FieldType, typename ConcreteType>
Node::search_result_t L_NODE_T::lower_bound(
    const onode_key_t& key, MatchHistory& history) {
  auto stage = this->stage();
  if (unlikely(stage.keys() == 0)) {
    assert(this->is_root());
    history.set<STAGE_LEFT>(MatchKindCMP::NE);
    auto p_cursor = get_or_create_cursor(search_position_t::end(), nullptr);
    return {p_cursor, MatchKindBS::NE};
  } else {
    auto result = STAGE_T::lower_bound_normalized(stage, key, history);
    if (result.is_end()) {
      assert(this->is_level_tail());
    }
    auto p_cursor = get_or_create_cursor(result.position, result.p_value);
    return {p_cursor, result.match()};
  }
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::lookup_smallest() {
  auto stage = this->stage();
  search_position_t pos;
  const onode_t* p_value = nullptr;
  if (unlikely(stage.keys() == 0)) {
    assert(this->is_root());
    pos = search_position_t::end();
  } else {
    pos = search_position_t::begin();
    p_value = this->get_value_ptr(pos);
  }
  return get_or_create_cursor(pos, p_value);
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::lookup_largest() {
  auto stage = this->stage();
  search_position_t pos;
  const onode_t* p_value = nullptr;
  if (unlikely(stage.keys() == 0)) {
    assert(this->is_root());
    pos = search_position_t::end();
  } else {
    STAGE_T::lookup_largest_normalized(stage, pos, p_value);
  }
  return get_or_create_cursor(pos, p_value);
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::insert_bottomup(
    const onode_key_t& key, const onode_t& value,
    const search_position_t& position, const MatchHistory& history) {
  auto stage = this->stage();
  search_position_t i_position;
  match_stage_t i_stage;
  ns_oid_view_t::Type i_dedup_type;
  node_offset_t i_estimated_size;
  if (can_insert(key, value, position, history,
                 i_position, i_stage, i_dedup_type, i_estimated_size)) {
#ifndef NDEBUG
    auto free_size_before = stage.free_size();
#endif
    auto p_value = proceed_insert<false>(
        key, value, i_position, i_stage, i_dedup_type, i_estimated_size);
#ifndef NDEBUG
    assert(stage.free_size() == free_size_before - i_estimated_size);
#endif
    return get_or_create_cursor(i_position, p_value);
  }

  // TODO: no need to cast after insert is generalized
  auto& _i_position = cast_down<STAGE_T::STAGE>(i_position);

  std::cout << "  try insert at: " << _i_position
            << ", i_stage=" << (int)i_stage << ", size=" << i_estimated_size
            << std::endl;

  size_t empty_size = stage.size_before(0);
  size_t available_size = stage.total_size() - empty_size;
  size_t target_split_size = empty_size + (available_size + i_estimated_size) / 2;
  // TODO adjust NODE_BLOCK_SIZE according to this requirement
  assert(i_estimated_size < available_size / 2);
  typename STAGE_T::StagedIterator split_at;
  bool i_to_left = STAGE_T::locate_split(
      stage, target_split_size, _i_position, i_stage, i_estimated_size, split_at);

  std::cout << "  split at: " << split_at << ", is_left=" << i_to_left
            << ", now insert at: " << _i_position
            << std::endl;

  auto append_at = split_at;
  auto right_node = ConcreteType::allocate(this->is_level_tail());
  // TODO: identify conditions for cross-node string deduplication
  typename STAGE_T::StagedAppender appender;
  appender.init(&right_node->extent(),
                const_cast<char*>(right_node->stage().p_start()));
  const onode_t* p_value = nullptr;
  if (!i_to_left) {
    // right node: append [start(append_at), i_position)
    STAGE_T::append_until(append_at, appender, _i_position, i_stage);
    std::cout << "insert to right: " << _i_position
              << ", i_stage=" << (int)i_stage << std::endl;
    // right node: append [i_position(key, value)]
    bool is_end = STAGE_T::append_insert(key, value, append_at, appender, i_stage, p_value);
    assert(append_at.is_end() == is_end);
  }

  // right node: append (i_position, end)
  auto pos_end = STAGE_T::position_t::end();
  STAGE_T::append_until(append_at, appender, pos_end, STAGE_T::STAGE);
  assert(append_at.is_end());
  appender.wrap();
  right_node->dump(std::cout) << std::endl;

  // left node: trim
  this->set_level_tail(false);
  STAGE_T::trim(this->extent(), split_at);

  if (i_to_left) {
    // left node: insert
    p_value = proceed_insert<true>(
        key, value, i_position, i_stage, i_dedup_type, i_estimated_size);
    std::cout << "insert to left: " << i_position
              << ", i_stage=" << (int)i_stage << std::endl;
  }
  this->dump(std::cout) << std::endl << std::endl;
  assert(p_value);

  // TODO: propagate index to parent

  if (i_to_left) {
    assert(this->get_index_view(i_position).match(key));
    return get_or_create_cursor(i_position, p_value);
  } else {
    assert(right_node->get_index_view(i_position).match(key));
    return right_node->get_or_create_cursor(i_position, p_value);
  }

  // TODO (optimize)
  // try to acquire space from siblings before split... see btrfs
}

template <typename FieldType, typename ConcreteType>
bool L_NODE_T::can_insert(
    const onode_key_t& key, const onode_t& value,
    const search_position_t& position, const MatchHistory& history,
    search_position_t& i_position, match_stage_t& i_stage,
    ns_oid_view_t::Type& dedup_type, node_offset_t& estimated_size) {
  // TODO: should be generalized and move out
  if constexpr (parent_t::FIELD_TYPE == field_type_t::N0) {
    // calculate the stage where insertion happens
    i_stage = STAGE_LEFT;
    bool is_PO = history.is_PO();
    if (position == search_position_t::begin() && is_PO) {
      assert(*history.get<STAGE_LEFT>() == MatchKindCMP::PO);
    } else {
      while (*history.get_by_stage(i_stage) == MatchKindCMP::EQ) {
        assert(i_stage != STAGE_RIGHT);
        --i_stage;
      }
    }
#ifndef NDEBUG
    if (position.is_end()) {
      assert(this->is_level_tail());
    }
#endif

    // calculate i_position
    i_position = position;
    if (is_PO) {
      if (i_position != search_position_t::begin() &&
          !i_position.is_end()) {
        switch (i_stage) {
         size_t* p_index;
         case STAGE_RIGHT:
          p_index = &i_position.nxt.nxt.index;
          assert(*p_index != INDEX_END);
          if (*p_index > 0) {
            --*p_index;
            break;
          }
          *p_index = INDEX_END;
          [[fallthrough]];
         case STAGE_STRING:
          p_index = &i_position.nxt.index;
          assert(*p_index != INDEX_END);
          if (*p_index > 0) {
            --*p_index;
            break;
          }
          *p_index = INDEX_END;
          [[fallthrough]];
         case STAGE_LEFT:
          p_index = &i_position.index;
          assert(*p_index != INDEX_END);
          assert(*p_index > 0);
          --*p_index;
        }
      }
    }

    // take ns/oid deduplication into consideration
    dedup_type = ns_oid_view_t::Type::STR;
    auto& s_match = history.get<STAGE_STRING>();
    if (s_match.has_value() && *s_match == MatchKindCMP::EQ) {
      if (is_PO) {
        dedup_type = ns_oid_view_t::Type::MIN;
      } else {
        dedup_type = ns_oid_view_t::Type::MAX;
      }
    }

    // estimated size for insertion
    switch (i_stage) {
     case STAGE_LEFT:
      estimated_size = node_stage_t::estimate_insert_one(key, value, dedup_type);
      break;
     case STAGE_STRING:
      estimated_size = item_iterator_t<parent_t::NODE_TYPE>::
        estimate_insert_one(key, value, dedup_type);
      break;
     case STAGE_RIGHT:
      estimated_size = leaf_sub_items_t::estimate_insert_one(value);
      break;
    }

    if (this->stage().free_size() < estimated_size) {
      return false;
    } else {
      return true;
    }
  } else {
    // not implemented
    assert(false);
  }
}

template <typename FieldType, typename ConcreteType>
template <bool SPLIT>
const onode_t* L_NODE_T::proceed_insert(
    const onode_key_t& key, const onode_t& value,
    search_position_t& i_position, match_stage_t i_stage,
    ns_oid_view_t::Type dedup_type, node_offset_t estimated_size) {
  // TODO: should be generalized and move out
  if constexpr (parent_t::FIELD_TYPE == field_type_t::N0) {
    auto stage = this->stage();
    // modify block at STAGE_LEFT
    const char* left_bound = stage.p_start() +
                             stage.fields().get_item_end_offset(stage.keys());
    size_t& index_2 = i_position.index;
    bool do_insert_2 = (i_stage == STAGE_LEFT);
    if constexpr (SPLIT) {
      if (!do_insert_2 && index_2 == stage.keys()) {
        do_insert_2 = true;
        estimated_size = node_stage_t::estimate_insert_one(key, value, dedup_type);
        // TODO: dedup type is changed
      }
    }
    if (do_insert_2) {
      if (index_2 == INDEX_END) {
        index_2 = stage.keys();
      }
      assert(index_2 <= stage.keys());
      i_position.nxt = search_position_t::nxt_t::begin();

      auto estimated_size_right = estimated_size - FieldType::estimate_insert_one();
      auto p_value = item_iterator_t<parent_t::NODE_TYPE>::insert(
          this->extent(), key, value,
          left_bound,
          const_cast<char*>(stage.p_start()) +
            stage.fields().get_item_end_offset(index_2),
          estimated_size_right,
          dedup_type);

      FieldType::insert_at(this->extent(), key,
                           stage.fields(), index_2, estimated_size_right);
      return p_value;
    }
    if (index_2 == INDEX_END) {
      index_2 = stage.keys() - 1;
    }
    assert(index_2 < stage.keys());

    // modify block at STAGE_STRING
    auto range = stage.get_nxt_container(index_2);
    item_iterator_t<parent_t::NODE_TYPE> iter(range);
    size_t& index_1 = i_position.nxt.index;
    bool is_end = false;
    // TODO: SPLIT == true
    if (index_1 == INDEX_END) {
      // reuse staged::_iterator_t::seek_last()
      while (iter.has_next()) {
        ++iter;
      }
    } else {
      // reuse staged::_iterator_t::seek_at()
      auto index = index_1;
      while (index > 0) {
        if (!iter.has_next()) {
          assert(index == 1);
          is_end = true;
          break;
        }
        ++iter;
        --index;
      }
    }
    bool do_insert_1 = (i_stage == STAGE_STRING);
    if constexpr (SPLIT) {
      if (!do_insert_1 && is_end) {
        do_insert_1 = true;
        estimated_size = item_iterator_t<parent_t::NODE_TYPE>::
          estimate_insert_one(key, value, dedup_type);
        // TODO: dedup type is changed
      }
    }
    if (do_insert_1) {
      char* p_insert;
      if (index_1 == INDEX_END) {
        index_1 = iter.index() + 1;
        is_end = true;
      }
      if (is_end) {
        p_insert = const_cast<char*>(iter.p_start());
      } else {
        p_insert = const_cast<char*>(iter.p_end());
      }
      i_position.nxt.nxt =
        search_position_t::nxt_t::nxt_t::begin();

      auto p_value = item_iterator_t<parent_t::NODE_TYPE>::insert(
          this->extent(), key, value,
          left_bound, p_insert, estimated_size, dedup_type);

      FieldType::update_size_at(this->extent(), stage.fields(), index_2, estimated_size);
      return p_value;
    }
    if (index_1 == INDEX_END) {
      index_1 = iter.index();
    }
    assert(!is_end);
    item_iterator_t<parent_t::NODE_TYPE>::update_size(
        this->extent(), iter, estimated_size);

    // modify block at STAGE_RIGHT
    assert(i_stage == STAGE_RIGHT);
    leaf_sub_items_t sub_items = iter.get_nxt_container();
    size_t& index_0 =  i_position.nxt.nxt.index;
    if (index_0 == INDEX_END) {
      index_0 = sub_items.keys();
    }
    assert(index_0 <= sub_items.keys());
    auto p_value = leaf_sub_items_t::insert_at(
        this->extent(), key, value,
        index_0, sub_items, left_bound, estimated_size);

    FieldType::update_size_at(this->extent(), stage.fields(), index_2, estimated_size);
    return p_value;
  } else {
    // not implemented
    assert(false);
  }
}

template <typename FieldType, typename ConcreteType>
Ref<tree_cursor_t> L_NODE_T::get_or_create_cursor(
    const search_position_t& position, const onode_t* p_value) {
  /*
  Ref<tree_cursor_t> p_cursor;
  auto found = tracked_cursors.find(position);
  if (found == tracked_cursors.end()) {
    p_cursor = new tree_cursor_t(this, position, p_value);
    tracked_cursors.insert({position, p_cursor});
  } else {
    p_cursor = found->second;
    assert(p_cursor->get_leaf_node() == this);
    assert(p_cursor->get_position() == position);
    // TODO: set p_value
  }
  return p_cursor;
  */
  return new tree_cursor_t(this, position, p_value);
}

}
