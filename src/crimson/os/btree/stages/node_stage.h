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

  // TODO: hide
  const char* p_start() const { return fields_start(*p_fields); }
  const FieldType& fields() const { return *p_fields; }

  bool is_level_tail() const { return _is_level_tail; }
  void set_level_tail(bool value) { _is_level_tail = value; }
  level_t level() const { return p_fields->header.level; }
  void init(const FieldType* _p_fields, bool level_tail) {
    assert(!p_fields);
    p_fields = _p_fields;
    set_level_tail(level_tail);

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

  size_t free_size() const {
    return p_fields->template free_size_before<NODE_TYPE>(
        is_level_tail(), keys());
  }
  size_t total_size() const;

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

  static node_offset_t estimate_insert_one(const onode_key_t* p_key, const value_t& value);

  class Appender;

 private:
  const FieldType* p_fields = nullptr;
  bool _is_level_tail;
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
  void append(const onode_key_t& key, const onode_t& value);
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
