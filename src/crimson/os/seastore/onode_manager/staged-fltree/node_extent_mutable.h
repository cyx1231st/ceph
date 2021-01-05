// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "fwd.h"

#pragma once

namespace crimson::os::seastore::onode {

/**
 * NodeExtentMutable
 *
 * A thin wrapper of NodeExtent to make sure that only the newly allocated
 * or the duplicated NodeExtent is mutable, and the memory modifications are
 * safe within the extent range.
 */
class NodeExtentMutable {
 public:
  void copy_in_absolute(void* dst, const void* src, extent_len_t len) {
    assert((char*)dst >= get_write());
    assert((char*)dst + len <= buf_upper_bound());
    std::memcpy(dst, src, len);
  }
  template <typename T>
  void copy_in_absolute(void* dst, const T& src) {
    copy_in_absolute(dst, &src, sizeof(T));
  }

  const void* copy_in_relative(
      extent_len_t dst_offset, const void* src, extent_len_t len) {
    auto dst = get_write() + dst_offset;
    copy_in_absolute(dst, src, len);
    return dst;
  }
  template <typename T>
  const T* copy_in_relative(
      extent_len_t dst_offset, const T& src) {
    auto dst = copy_in_relative(dst_offset, &src, sizeof(T));
    return static_cast<const T*>(dst);
  }

  void shift_absolute(const void* src, extent_len_t len, int offset) {
    assert((const char*)src >= get_write());
    assert((const char*)src + len <= buf_upper_bound());
    char* to = (char*)src + offset;
    assert(to >= get_write());
    assert(to + len <= buf_upper_bound());
    if (len != 0) {
      std::memmove(to, src, len);
    }
  }
  void shift_relative(extent_len_t src_offset, extent_len_t len, int offset) {
    shift_absolute(get_write() + src_offset, len, offset);
  }

  template <typename T>
  void validate_inplace_update(const T& updated) {
    assert((const char*)&updated >= get_write());
    assert((const char*)&updated + sizeof(T) <= buf_upper_bound());
  }

  const char* get_read() const { return p_start; }
  char* get_write() { return p_start; }
  extent_len_t get_length() const { return length; }
  node_offset_t get_node_offset() const { return node_offset; }

  NodeExtentMutable get_mutable(node_offset_t offset, node_offset_t len) const {
    assert(node_offset == 0);
    assert(offset != 0);
    assert(offset + len <= length);
    auto ret = *this;
    ret.p_start += offset;
    ret.length = len;
    ret.node_offset = offset;
    return ret;
  }

 private:
  explicit NodeExtentMutable(char* p_start, extent_len_t length)
    : p_start{p_start}, length{length} {}
  const char* buf_upper_bound() const { return p_start + length; }

  char* p_start;
  extent_len_t length;
  node_offset_t node_offset = 0;

  friend class NodeExtent;
};

}
