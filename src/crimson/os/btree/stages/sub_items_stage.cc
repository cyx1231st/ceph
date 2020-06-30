// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sub_items_stage.h"

#include "crimson/os/btree/dummy_transaction_manager.h"

namespace crimson::os::seastore::onode {

// helper type for the visitor
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

char* leaf_sub_items_t::Appender::wrap() {
  auto p_cur = p_append;
  num_keys_t num_keys = 0;
  for (auto i = 0u; i < cnt; ++i) {
    auto& a = appends[i];
    std::visit(overloaded {
      [&] (const range_items_t& arg) { num_keys += arg.items; },
      [&] (const kv_item_t& arg) { ++num_keys; }
    }, a);
  }
  assert(num_keys);
  p_cur -= sizeof(num_keys_t);
  p_dst->copy_in_mem(num_keys, p_cur);

  node_offset_t last_offset = 0;
  for (auto i = 0u; i < cnt; ++i) {
    auto& a = appends[i];
    std::visit(overloaded {
      [&] (const range_items_t& arg) {
        int compensate = (last_offset - op_src->get_offset_to_end(arg.from));
        node_offset_t offset;
        for (auto i = arg.from; i < arg.from + arg.items; ++i) {
          offset = op_src->get_offset(i) + compensate;
          p_cur -= sizeof(node_offset_t);
          p_dst->copy_in_mem(offset, p_cur);
        }
        last_offset = offset;
      },
      [&] (const kv_item_t& arg) {
        last_offset += sizeof(snap_gen_t) + arg.p_value->size;
        p_cur -= sizeof(node_offset_t);
        p_dst->copy_in_mem(last_offset, p_cur);
      }
    }, a);
  }

  for (auto i = 0u; i < cnt; ++i) {
    auto& a = appends[i];
    std::visit(overloaded {
      [&] (const range_items_t& arg) {
        auto _p_start = op_src->get_item_end(arg.from + arg.items);
        size_t _len = op_src->get_item_end(arg.from) - _p_start;
        p_cur -= _len;
        p_dst->copy_in_mem(_p_start, p_cur, _len);
      },
      [&] (const kv_item_t& arg) {
        p_cur -= sizeof(snap_gen_t);
        p_dst->copy_in_mem(snap_gen_t::from_key(*arg.p_key), p_cur);
        p_cur -= arg.p_value->size;
        p_dst->copy_in_mem(arg.p_value, p_cur, arg.p_value->size);
      }
    }, a);
  }
  return p_cur;
}

const onode_t* leaf_sub_items_t::insert_new(
    LogicalCachedExtent& dst, const onode_key_t& key, const onode_t& value,
    char*& p_insert) {
  Appender appender(&dst, p_insert);
  appender.append(key, value);
  p_insert = appender.wrap();
  return reinterpret_cast<const onode_t*>(p_insert);
}

const onode_t* leaf_sub_items_t::insert_at(
    LogicalCachedExtent& dst, const onode_key_t& key, const onode_t& value,
    size_t index, leaf_sub_items_t& sub_items, const char* p_left_bound, size_t estimated_size) {
  assert(estimated_size == estimate_insert_one(value));

  // a. [... item(index)] << estimated_size
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = sub_items.get_item_end(index);
  dst.shift_mem(p_shift_start, p_shift_end - p_shift_start, -(int)estimated_size);

  // b. insert item
  auto p_insert = const_cast<char*>(p_shift_end - estimated_size);
  auto p_value = reinterpret_cast<const onode_t*>(p_insert);
  dst.copy_in_mem(&value, p_insert, value.size);
  p_insert += value.size;
  dst.copy_in_mem(snap_gen_t::from_key(key), p_insert);
  assert(p_insert + sizeof(snap_gen_t) + sizeof(node_offset_t) == p_shift_end);

  // c. compensate affected offsets
  auto item_size = value.size + sizeof(snap_gen_t);
  for (auto i = index; i < sub_items.keys(); ++i) {
    const node_offset_t& offset_i = sub_items.get_offset(i);
    dst.copy_in_mem(node_offset_t(offset_i + item_size), (void*)&offset_i);
  }

  // d. [item(index-1) ... item(0) ... offset(index)] <<< sizeof(node_offset_t)
  const char* p_offset = (index == 0 ?
                          (const char*)&sub_items.get_offset(0) + sizeof(node_offset_t) :
                          (const char*)&sub_items.get_offset(index - 1));
  p_shift_start = p_shift_end;
  p_shift_end = p_offset;
  dst.shift_mem(p_shift_start, p_shift_end - p_shift_start, -(int)sizeof(node_offset_t));

  // e. insert offset
  node_offset_t offset_to_item_start = item_size + sub_items.get_offset_to_end(index);
  dst.copy_in_mem(offset_to_item_start,
                  const_cast<char*>(p_shift_end) - sizeof(node_offset_t));

  // f. update num_sub_keys
  dst.copy_in_mem(num_keys_t(sub_items.keys() + 1), (void*)&sub_items.keys());

  return p_value;
}

}
