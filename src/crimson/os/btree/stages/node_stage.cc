// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_stage.h"

#include "crimson/os/btree/dummy_transaction_manager.h"
#include "item_iterator_stage.h"
#include "node_layout.h"
#include "sub_items_stage.h"

namespace crimson::os::seastore::onode {

#define NODE_T node_extent_t<FieldType, NODE_TYPE>
template class node_extent_t<node_fields_0_t, node_type_t::INTERNAL>;
template class node_extent_t<node_fields_1_t, node_type_t::INTERNAL>;
template class node_extent_t<node_fields_2_t, node_type_t::INTERNAL>;
template class node_extent_t<internal_fields_3_t, node_type_t::INTERNAL>;
template class node_extent_t<node_fields_0_t, node_type_t::LEAF>;
template class node_extent_t<node_fields_1_t, node_type_t::LEAF>;
template class node_extent_t<node_fields_2_t, node_type_t::LEAF>;
template class node_extent_t<leaf_fields_3_t, node_type_t::LEAF>;

template <typename FieldType, node_type_t NODE_TYPE>
size_t NODE_T::size_to_nxt_at(size_t index) const {
  assert(index < keys());
  auto ret = FieldType::estimate_insert_one();
  if (index == 0) {
    ret += size_before(0);
  }
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    return ret;
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    auto p_end = p_start() + p_fields->get_item_end_offset(index);
    return ret + ns_oid_view_t(p_end).size();
  } else {
    // N3 node is not nested
    assert(false);
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
size_t NODE_T::total_size() const {
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    if (is_level_tail()) {
      return FieldType::SIZE - sizeof(snap_gen_t);
    }
  }
  return FieldType::SIZE;
}

template <typename FieldType, node_type_t NODE_TYPE>
memory_range_t NODE_T::get_nxt_container(size_t index) const {
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    // N3 internal node doesn't have left and right parts
    assert(false);
  } else {
    node_offset_t item_start_offset = p_fields->get_item_start_offset(index);
    node_offset_t item_end_offset = p_fields->get_item_end_offset(index);
    assert(item_start_offset < item_end_offset);
    auto item_p_start = p_start() + item_start_offset;
    auto item_p_end = p_start() + item_end_offset;
    if constexpr (FIELD_TYPE == field_type_t::N2) {
      // range for sub_items_t<NODE_TYPE>
      item_p_end = ns_oid_view_t(item_p_end).p_start();
      assert(item_p_start < item_p_end);
    } else {
      // range for item_iterator_t<NODE_TYPE>
    }
    return {item_p_start, item_p_end};
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
node_offset_t NODE_T::estimate_insert_one(
    const onode_key_t* p_key, const value_t& value) {
  node_offset_t left_size = FieldType::estimate_insert_one();
  node_offset_t right_size;
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    right_size = item_iterator_t<NODE_TYPE>::estimate_insert_new(p_key, value);
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    right_size = sub_items_t<NODE_TYPE>::estimate_insert_new(value) +
                 ns_oid_view_t::estimate_size(p_key);
  } else {
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      right_size = value.size;
    } else {
      right_size = 0u;
    }
  }
  return left_size + right_size;
}

#define APPEND_T node_extent_t<FieldType, NODE_TYPE>::Appender

template <typename FieldType, node_type_t NODE_TYPE>
void APPEND_T::append(const node_extent_t& src, size_t from, size_t items) {
  assert(from <= src.keys());
  if (items == 0) {
    return;
  }
  assert(from < src.keys());
  assert(from + items <= src.keys());
  num_keys += items;
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    assert(false);
  } else {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(false);
    }

    // append left part forwards
    node_offset_t offset_left_start = src.fields().get_key_start_offset(from);
    node_offset_t offset_left_end = src.fields().get_key_start_offset(from + items);
    node_offset_t left_size = offset_left_end - offset_left_start;
    if (num_keys == 0) {
      // no need to adjust offset
      assert(from == 0);
      assert(p_start + offset_left_start == p_append_left);
      p_dst->copy_in_mem(src.p_start() + offset_left_start,
                         p_append_left, left_size);
    } else {
      node_offset_t step_size = FieldType::estimate_insert_one();
      node_offset_t offset_base = src.fields().get_item_end_offset(from);
      int offset_change = p_append_right - p_start - offset_base;
      auto p_offset_dst = p_append_left;
      if constexpr (FIELD_TYPE != field_type_t::N2) {
        // copy keys
        p_dst->copy_in_mem(src.p_start() + offset_left_start,
                           p_append_left, left_size);
        // point to offset for update
        p_offset_dst += sizeof(typename FieldType::key_t);
      }
      for (auto i = from; i < from + items; ++i) {
        p_dst->copy_in_mem(node_offset_t(src.fields().get_item_start_offset(i) + offset_change),
                           p_offset_dst);
        p_offset_dst += step_size;
      }
      assert(p_append_left + left_size + sizeof(typename FieldType::key_t) ==
             p_offset_dst);
    }
    p_append_left += left_size;

    // append right part backwards
    node_offset_t offset_right_start = src.fields().get_item_end_offset(from + items);
    node_offset_t offset_right_end = src.fields().get_item_end_offset(from);
    node_offset_t right_size = offset_right_end - offset_right_start;
    p_append_right -= right_size;
    p_dst->copy_in_mem(src.p_start() + offset_right_start,
                       p_append_right, right_size);
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
void APPEND_T::append(const onode_key_t& key, const onode_t& value) {
  if constexpr (FIELD_TYPE == field_type_t::N3) {
    // TODO: not implemented
    assert(false);
  } else {
    assert(false);
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
std::tuple<LogicalCachedExtent*, char*>
APPEND_T::open_nxt(const key_get_type& partial_key) {
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    FieldType::append_key(*p_dst, partial_key, p_append_left);
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    FieldType::append_key(*p_dst, partial_key, p_append_right);
  } else {
    assert(false);
  }
  return {p_dst, p_append_right};
}

template <typename FieldType, node_type_t NODE_TYPE>
std::tuple<LogicalCachedExtent*, char*>
APPEND_T::open_nxt(const onode_key_t& key) {
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    FieldType::append_key(
        *p_dst, key, p_append_left);
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    FieldType::append_key(
        *p_dst, key, p_append_right);
  } else {
    assert(false);
  }
  return {p_dst, p_append_right};
}

template <typename FieldType, node_type_t NODE_TYPE>
char* APPEND_T::wrap() {
  assert(p_append_left <= p_append_right);
  p_dst->copy_in_mem(num_keys, p_start + offsetof(FieldType, num_keys));
  return p_append_left;
}

}
