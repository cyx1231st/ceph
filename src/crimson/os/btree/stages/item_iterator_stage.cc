// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "item_iterator_stage.h"

#include "crimson/os/btree/dummy_transaction_manager.h"
#include "sub_items_stage.h"

namespace crimson::os::seastore::onode {

#define ITER_T item_iterator_t<NODE_TYPE>
template class item_iterator_t<node_type_t::LEAF>;
template class item_iterator_t<node_type_t::INTERNAL>;

template <node_type_t NODE_TYPE>
node_offset_t ITER_T::estimate_insert_one(
    const onode_key_t* p_key, const value_t& value) {
  return (sub_items_t<NODE_TYPE>::estimate_insert_new(value) +
          ns_oid_view_t::estimate_size(p_key) + sizeof(node_offset_t));
}

template <node_type_t NODE_TYPE>
void ITER_T::update_size(
    LogicalCachedExtent& dst, const ITER_T& iter, int change) {
  node_offset_t offset = iter.get_back_offset();
  assert(change + offset > 0);
  assert(change + offset < NODE_BLOCK_SIZE);
  dst.copy_in_mem(node_offset_t(offset + change),
                  (void*)iter.get_item_range().p_end);
}

template <node_type_t NODE_TYPE>
const typename ITER_T::value_t* ITER_T::insert(
    LogicalCachedExtent& dst, const onode_key_t& key, const onode_t& value,
    const char* left_bound, char* p_insert,
    node_offset_t estimated_size, const ns_oid_view_t::Type& dedup_type) {
  if constexpr (NODE_TYPE == node_type_t::LEAF) {
    const char* p_shift_start = left_bound;
    const char* p_shift_end = p_insert;
    dst.shift_mem(p_shift_start,
                  p_shift_end - p_shift_start,
                  -(int)estimated_size);

    const char* p_insert_start = p_insert - estimated_size;
    p_insert -= sizeof(node_offset_t);
    node_offset_t back_offset = (p_insert - p_insert_start);
    dst.copy_in_mem(back_offset, p_insert);

    ns_oid_view_t::append(dst, key, dedup_type, p_insert);

    auto p_value = leaf_sub_items_t::insert_new(dst, key, value, p_insert);
    assert(p_insert == p_insert_start);
    return p_value;
  } else {
    // TODO: not implemented
    assert(false);
  }
}

#define APPEND_T item_iterator_t<NODE_TYPE>::Appender

template <node_type_t NODE_TYPE>
bool APPEND_T::append(const ITER_T& src, size_t& items, index_t type) {
  auto p_end = src.p_end();
  if (items != INDEX_END) {
    for (auto i = 1u; i <= items; ++i) {
      if (!src.has_next()) {
        assert(i == items);
        type = index_t::end;
        break;
      }
      ++src;
    }
  } else if (type != index_t::none) {
    items = 0;
    while (src.has_next()) {
      ++src;
      ++items;
    }
    if (type == index_t::end) {
      ++items;
    }
  } else {
    assert(false);
  }
  const char* p_start;
  if (type == index_t::end) {
    // include last
    p_start = src.p_start();
  } else {
    // exclude last
    p_start = src.p_end();
  }
  assert(p_end >= p_start);
  size_t append_size = p_end - p_start;
  p_append -= append_size;
  p_dst->copy_in_mem(p_start, p_append, append_size);
  return type == index_t::end;
}

template <node_type_t NODE_TYPE>
std::tuple<LogicalCachedExtent*, char*>
APPEND_T::open_nxt(const key_get_type& partial_key) {
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append(*p_dst, partial_key, p_append);
  return {p_dst, p_append};
}

template <node_type_t NODE_TYPE>
std::tuple<LogicalCachedExtent*, char*>
APPEND_T::open_nxt(const onode_key_t& key) {
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append(*p_dst, key, ns_oid_view_t::Type::STR, p_append);
  return {p_dst, p_append};
}

template <node_type_t NODE_TYPE>
void APPEND_T::wrap_nxt(char* _p_append) {
  assert(_p_append < p_append);
  p_dst->copy_in_mem(node_offset_t(p_offset_while_open - _p_append),
                     p_offset_while_open);
  p_append = _p_append;
}

}
