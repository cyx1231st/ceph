// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/btree/node_types.h"
#include "key_layout.h"
#include "stage_types.h"

namespace crimson::os::seastore::onode {

template <typename FieldType, node_type_t _NODE_TYPE>
class node_extent_t {
 public:
  using value_t = value_type_t<_NODE_TYPE>;
  using num_keys_t = typename FieldType::num_keys_t;
  static constexpr node_type_t NODE_TYPE = _NODE_TYPE;
  static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;
  static constexpr node_offset_t EXTENT_SIZE =
    (FieldType::SIZE + BLOCK_SIZE - 1u) / BLOCK_SIZE * BLOCK_SIZE;

  node_extent_t(const FieldType* p_fields, const bool* p_is_level_tail)
      : p_fields{p_fields}, p_is_level_tail{p_is_level_tail} {
    assert(p_fields->header.get_node_type() == NODE_TYPE);
    assert(p_fields->header.get_field_type() == FieldType::FIELD_TYPE);
#ifndef NDEBUG
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(level() > 0u);
    } else {
      assert(level() == 0u);
    }
#endif
  }

  // TODO: hide
  const char* p_start() const { return fields_start(*p_fields); }
  const FieldType& fields() const { return *p_fields; }

  level_t level() const { return p_fields->header.level; }
  size_t free_size() const {
    return p_fields->template free_size_before<NODE_TYPE>(
        is_level_tail(), keys());
  }
  size_t total_size() const;
  const char* p_left_bound() const;
  template <node_type_t T = NODE_TYPE>
  std::enable_if_t<T == node_type_t::INTERNAL, const laddr_t*>
  get_end_p_laddr() {
    assert(is_level_tail());
    if constexpr (FIELD_TYPE == field_type_t::N3) {
      #pragma GCC diagnostic ignored "-Waddress-of-packed-member"
      return &p_fields->child_addrs[keys()];
    } else {
      auto offset_start = p_fields->get_item_end_offset(keys());
      assert(offset_start <= FieldType::SIZE);
      offset_start -= sizeof(laddr_t);
      auto p_addr = p_start() + offset_start;
      return reinterpret_cast<const laddr_t*>(p_addr);
    }
  }

  // container type system
  using key_get_type = typename FieldType::key_get_type;
  static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
  size_t keys() const { return p_fields->num_keys; }
  key_get_type operator[] (size_t index) const { return p_fields->get_key(index); }
  size_t size_before(size_t index) const {
    auto free_size = p_fields->template free_size_before<NODE_TYPE>(
        is_level_tail(), index);
    assert(total_size() >= free_size);
    return total_size() - free_size;
  }
  size_t size_to_nxt_at(size_t index) const;
  memory_range_t get_nxt_container(size_t index) const;

  template <typename T = FieldType>
  std::enable_if_t<T::FIELD_TYPE == field_type_t::N3, const value_t*>
  get_p_value(size_t index) const {
    assert(index < keys());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      #pragma GCC diagnostic ignored "-Waddress-of-packed-member"
      return &p_fields->child_addrs[index];
    } else {
      auto range = get_nxt_container(index);
      auto ret = reinterpret_cast<const onode_t*>(range.p_start);
      assert(range.p_start + ret->size == range.p_end);
      return ret;
    }
  }

  static node_offset_t header_size() { return FieldType::HEADER_SIZE; }

  template <typename T = node_offset_t>
  static std::enable_if_t<_NODE_TYPE == node_type_t::INTERNAL, T>
  estimate_insert(const index_view_t& key) {
    auto size = FieldType::estimate_insert_one();
    if constexpr (FIELD_TYPE == field_type_t::N2) {
      size += key.p_ns_oid->size();
    }
    return size;
  }

  template <typename T = node_offset_t>
  static std::enable_if_t<_NODE_TYPE == node_type_t::LEAF, T>
  estimate_insert(const onode_key_t& key, const ns_oid_view_t::Type& type, const onode_t& value) {
    auto size = FieldType::estimate_insert_one();
    if constexpr (FIELD_TYPE == field_type_t::N2) {
      size += ns_oid_view_t::estimate_size(key, type);
    } else if constexpr (FIELD_TYPE == field_type_t::N3) {
      size += value.size;
    }
    return size;
  }

  static const value_t* insert_at(
      LogicalCachedExtent& dst, const node_extent_t&,
      const onode_key_t& key, ns_oid_view_t::Type type, const value_t& value,
      size_t index, node_offset_t size, const char* p_left_bound);

  static memory_range_t insert_prefix_at(
      LogicalCachedExtent& dst, const node_extent_t&,
      const onode_key_t& key, ns_oid_view_t::Type type,
      size_t index, node_offset_t size, const char* p_left_bound);

  static void update_size_at(
      LogicalCachedExtent& dst, const node_extent_t&, size_t index, int change);

  static size_t trim_until(LogicalCachedExtent&, const node_extent_t&, size_t index);
  static size_t trim_at(LogicalCachedExtent&, const node_extent_t&,
                        size_t index, size_t trimmed);

  class Appender;

 private:
  bool is_level_tail() const { return *p_is_level_tail; }

  const FieldType* p_fields;
  const bool* p_is_level_tail;
};

template <typename FieldType, node_type_t NODE_TYPE>
class node_extent_t<FieldType, NODE_TYPE>::Appender {
 public:
  Appender(LogicalCachedExtent* p_dst, char* p_append)
    : p_dst{p_dst}, p_start{p_append} {
#ifndef NDEBUG
    auto p_fields = reinterpret_cast<const FieldType*>(p_append);
    assert(*(p_fields->header.get_field_type()) == FIELD_TYPE);
    assert(p_fields->header.get_node_type() == NODE_TYPE);
    assert(p_fields->num_keys == 0);
#endif
    p_append_left = p_start + FieldType::HEADER_SIZE;
    p_append_right = p_start + FieldType::SIZE;
  }
  void append(const node_extent_t& src, size_t from, size_t items);
  void append(const onode_key_t&, const onode_t&, const onode_t*&);
  char* wrap();
  std::tuple<LogicalCachedExtent*, char*> open_nxt(const key_get_type&);
  std::tuple<LogicalCachedExtent*, char*> open_nxt(const onode_key_t&);
  void wrap_nxt(char* p_append) {
    if constexpr (FIELD_TYPE != field_type_t::N3) {
      assert(p_append < p_append_right);
      assert(p_append_left < p_append);
      p_append_right = p_append;
      FieldType::append_offset(*p_dst, p_append - p_start, p_append_left);
      ++num_keys;
    } else {
      assert(false);
    }
  }
 private:
  LogicalCachedExtent* p_dst;
  char* p_start;
  char* p_append_left;
  char* p_append_right;
  num_keys_t num_keys = 0;
};

}
