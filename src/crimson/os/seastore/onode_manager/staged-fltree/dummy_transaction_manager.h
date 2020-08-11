// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstring>
#include <map>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "crimson/common/type_helpers.h"

#include "fwd.h"

namespace crimson::os::seastore::onode {

// memory-based, synchronous and simplified version of
// crimson::os::seastore::LogicalCachedExtent
class LogicalCachedExtent
  : public boost::intrusive_ref_counter<LogicalCachedExtent,
                                        boost::thread_unsafe_counter> {
 public:
  LogicalCachedExtent(const LogicalCachedExtent&) = delete;

  laddr_t get_laddr() const {
    assert(valid);
    static_assert(sizeof(void*) == sizeof(laddr_t));
    return reinterpret_cast<laddr_t>(ptr);
  }
  loff_t get_length() const {
    assert(valid);
    return length;
  }
  template <typename T>
  const T* get_ptr(loff_t block_offset) const {
    assert(valid);
    if constexpr (!std::is_same_v<T, void>) {
      assert(block_offset + sizeof(T) <= length);
    }
    return static_cast<const T*>(ptr_offset(block_offset));
  }

  void copy_in_mem(const void* from, void* to, loff_t len) {
    assert(valid);
#ifndef NDEBUG
    auto to_ = static_cast<const char*>(to);
    auto ptr_ = static_cast<const char*>(ptr);
    assert(ptr_ <= to_);
    assert(to_ + len <= ptr_ + length);
#endif
    std::memcpy(to, from, len);
  }
  template <typename T>
  void copy_in_mem(const T& from, void* to) {
    copy_in_mem(&from, to, sizeof(T));
  }
  const void* copy_in(const void* from, loff_t to_block_offset, loff_t len) {
    auto to = ptr_offset(to_block_offset);
    copy_in_mem(from, to, len);
    return to;
  }
  template <typename T>
  const T* copy_in(const T& from, loff_t to_block_offset) {
    auto ret = copy_in(&from, to_block_offset, sizeof(from));
    return (const T*)ret;
  }
  void copy_from(const LogicalCachedExtent& from) {
    assert(length == from.length);
    std::memcpy(ptr, from.ptr, length);
  }
  void shift_mem(const char* from, loff_t len, int shift_offset) {
    assert(valid);
    assert(from + len <= (const char*)ptr + length);
    assert(from + shift_offset >= (const char*)ptr);
    if (len == 0) {
      return;
    }
    const char* to = from + shift_offset;
    assert(to + len <= (const char*)ptr + length);
    std::memmove(const_cast<char*>(to), from, len);
  }
  void shift(loff_t from_block_offset, loff_t len, int shift_offset) {
    shift_mem((const char*)ptr_offset(from_block_offset), len, shift_offset);
  }

 private:
  LogicalCachedExtent(void* ptr, loff_t len) : ptr{ptr}, length{len} {}

  void invalidate() {
    assert(valid);
    valid = false;
    ptr = nullptr;
    length = 0;
  }

  const void* ptr_offset(loff_t offset) const {
    assert(valid);
    assert(offset < length);
    return static_cast<const char*>(ptr) + offset;
  }
  void* ptr_offset(loff_t offset) {
    return const_cast<void*>(
        const_cast<const LogicalCachedExtent*>(this)->ptr_offset(offset));
  }

  bool valid = true;
  void* ptr;
  loff_t length;

  friend class DummyTransactionManager;
};

// memory-based, synchronous and simplified version of
// crimson::os::seastore::TransactionManager
class DummyTransactionManager {
 public:
  // currently ignore delta machinary, and modify memory inplace
  Ref<LogicalCachedExtent> alloc_extent(loff_t len) {
    constexpr size_t ALIGNMENT = 4096;
    assert(len % ALIGNMENT == 0);
    auto mem_block = std::aligned_alloc(len, ALIGNMENT);
    auto extent = Ref<LogicalCachedExtent>(new LogicalCachedExtent(mem_block, len));
    assert(allocate_map.find(extent->get_laddr()) == allocate_map.end());
    allocate_map.insert({extent->get_laddr(), extent});
    return extent;
  }
  void free_all() {
    for (auto& [addr, extent] : allocate_map) {
      std::free(extent->ptr);
      extent->invalidate();
    }
    allocate_map.clear();
  }
  Ref<LogicalCachedExtent> read_extent(laddr_t addr) {
    auto iter = allocate_map.find(addr);
    assert(iter != allocate_map.end());
    return iter->second;
  }

 private:
  std::map<laddr_t, Ref<LogicalCachedExtent>> allocate_map;
};

inline DummyTransactionManager& get_transaction_manager() {
  static DummyTransactionManager transaction_manager;
  return transaction_manager;
}

}
