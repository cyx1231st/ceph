// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "item_iterator_stage.h"

#include "crimson/os/btree/dummy_transaction_manager.h"

namespace crimson::os::seastore::onode {

#define ITER_T item_iterator_t<NODE_TYPE>
template class item_iterator_t<node_type_t::LEAF>;
template class item_iterator_t<node_type_t::INTERNAL>;

template <node_type_t NODE_TYPE>
memory_range_t ITER_T::insert_prefix(
    LogicalCachedExtent& dst, const item_iterator_t<NODE_TYPE>& iter,
    const full_key_t<KeyT::HOBJ>& key, ns_oid_view_t::Type type,
    bool is_end, node_offset_t size, const char* p_left_bound) {
  if constexpr (NODE_TYPE == node_type_t::LEAF) {
    // 1. insert range
    char* p_insert;
    if (is_end) {
      assert(!iter.has_next());
      p_insert = const_cast<char*>(iter.p_start());
    } else {
      p_insert = const_cast<char*>(iter.p_end());
    }
    char* p_insert_front = p_insert - size;

    // 2. shift memory
    const char* p_shift_start = p_left_bound;
    const char* p_shift_end = p_insert;
    dst.shift_mem(p_shift_start,
                  p_shift_end - p_shift_start,
                  -(int)size);

    // 3. append header
    p_insert -= sizeof(node_offset_t);
    node_offset_t back_offset = (p_insert - p_insert_front);
    dst.copy_in_mem(back_offset, p_insert);
    ns_oid_view_t::append<KeyT::HOBJ>(dst, key, p_insert);

    return {p_insert_front, p_insert};
  } else {
    assert(false && "not implemented");
  }
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
size_t ITER_T::trim_until(LogicalCachedExtent& extent, const ITER_T& iter) {
  assert(iter.index() != 0);
  return iter.p_end() - iter.p_items_start;
}

template <node_type_t NODE_TYPE>
size_t ITER_T::trim_at(
    LogicalCachedExtent& extent, const ITER_T& iter, size_t trimmed) {
  size_t trim_size = iter.p_start() - iter.p_items_start + trimmed;
  assert(iter.get_back_offset() > trimmed);
  node_offset_t new_offset = iter.get_back_offset() - trimmed;
  extent.copy_in_mem(new_offset, (void*)iter.item_range.p_end);
  return trim_size;
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
APPEND_T::open_nxt(const full_key_t<KeyT::HOBJ>& key) {
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append<KeyT::HOBJ>(*p_dst, key, p_append);
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
