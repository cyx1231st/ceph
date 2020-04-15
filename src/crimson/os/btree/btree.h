// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace crimson::os::seastore::onode {
  /*
   * stubs
   */
  using laddr_t = uint64_t;
  using loff_t = uint32_t;

  // might be managed by an Onode class
  struct onode_t {
    // onode should be smaller than a node
    uint16_t size; // address up to 64 KiB sized node
    // omap, extent_map, inline data
  };

  // memory-based, synchronous and simplified version of
  // crimson::os::seastore::LogicalCachedExtent
  class LogicalCachedExtent
    : public boost::intrusive_ref_counter<LogicalCachedExtent, boost::thread_unsafe_counter> {
   public:
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
      assert(block_offset + sizeof(T) <= length);
      const char* logical_offset = (static_cast<const char*>(ptr) + block_offset);
      return reinterpret_cast<const T*>(logical_offset);
    }
    void copy_in(const void* from, loff_t block_offset, loff_t len) {
      assert(valid);
      assert(block_offset + len <= length);
      char* logical_offset = (static_cast<char*>(ptr) + block_offset);
      memcpy(logical_offset, from, len);
    }
    template <typename T>
    void copy_in(const T& from, loff_t block_offset) {
      copy_in(&from, block_offset, sizeof(from));
    }

   private:
    LogicalCachedExtent(void* ptr, loff_t len) : ptr{ptr}, length{len} {}

    void invalidate() {
      assert(valid);
      valid = false;
      ptr = nullptr;
      length = 0;
    }

    bool valid = true;
    void* ptr;
    loff_t length;

    friend class DummyTransactionManager;
  };
  using LogicalCachedExtentRef = boost::intrusive_ptr<LogicalCachedExtent>;

  // memory-based, synchronous and simplified version of
  // crimson::os::seastore::TransactionManager
  class DummyTransactionManager {
   public:
    // currently ignore delta machinary, and modify memory inplace
    LogicalCachedExtentRef alloc_extent(loff_t len) {
      assert(len % 4096 == 0);
      auto mem_block = std::aligned_alloc(len, 4096);
      auto extent = LogicalCachedExtentRef(new LogicalCachedExtent(mem_block, len));
      assert(allocate_map.find(extent->get_laddr()) == allocate_map.end());
      allocate_map.insert({extent->get_laddr(), extent});
    }
    void free_extent(LogicalCachedExtentRef extent) {
      std::free(extent->ptr);
      auto size = allocate_map.erase(extent->get_laddr());
      assert(size == 1u);
      extent->invalidate();
    }
    LogicalCachedExtentRef read_extent(laddr_t offset) {
      auto iter = allocate_map.find(offset);
      assert(iter != allocate_map.end());
      return iter->second;
    }

   private:
    std::map<laddr_t, LogicalCachedExtentRef> allocate_map;

  } transaction_manager;

  /*
   * onode indexes
   */
  using shard_t = int8_t;
  using pool_t = int64_t;
  using crush_hash_t = uint32_t;
  // nspace, oid (variable)
  using snap_t = uint64_t;
  using gen_t = uint64_t;

  /*
   * fixed keys
   */
  // TODO: consider alignments
  struct fixed_key_0_t {
    shard_t shard;
    pool_t pool;
    crush_hash_t crush_hash;
  } __attribute__((packed));

  struct fixed_key_1_t {
    crush_hash_t crush_hash;
  } __attribute__((packed));

  struct fixed_key_3_t {
    snap_t snap;
    gen_t gen;
  } __attribute__((packed));

  /*
   * btree block layouts
   */
  constexpr loff_t NODE_BLOCK_SIZE = 1u << 12;
  // TODO: decide by NODE_BLOCK_SIZE
  using node_offset_t = uint16_t;

  // allowed fixed_key_type: fixed_key_0_t, fixed_key_1_t
  template <typename fixed_key_type>
  struct slot_t {
    fixed_key_type key;
    node_offset_t right_offset;
  } __attribute__((packed));

  template <typename fixed_key_type>
  struct node_fields_01_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(slot_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_slots_t = uint8_t;
    num_slots_t num_slots = 0u;
    slot_t<fixed_key_type> slots[];
  } __attribute__((packed));

  struct node_fields_2_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(node_off_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_offsets_t = uint8_t;
    num_offsets_t num_offsets = 0u;
    node_offset_t offsets[];
  } __attribute__((packed));

  // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
  static constexpr unsigned MAX_NUM_KEYS_I3 = 170;
  template <unsigned MAX_NUM_KEYS>
  struct _internal_fields_3_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
    using num_keys_t = uint8_t;
    num_keys_t num_keys = 0u;
    fixed_key_3_t keys[MAX_NUM_KEYS];
    laddr_t child_addrs[MAX_NUM_KEYS + 1];
  } __attribute__((packed));
  static_assert(sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3>) <= NODE_BLOCK_SIZE &&
                sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3 + 1>) > NODE_BLOCK_SIZE);
  using internal_fields_3_t = _internal_fields_3_t<MAX_NUM_KEYS_I3>;

  struct leaf_fields_3_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
    using num_values_t = uint8_t;
    num_values_t num_values = 0u;
    fixed_key_3_t keys[];
  } __attribute__((packed));

  // ...

  /*
   * btree interfaces
   * requirements are based on:
   *   ceph::os::Transaction::create/touch/remove()
   *   ceph::ObjectStore::collection_list()
   *   ceph::BlueStore::get_onode()
   *   db->get_iterator(PREFIIX_OBJ) by ceph::BlueStore::fsck()
   */
  struct onode_key_t {
    shard_t shard;
    pool_t pool;
    crush_hash_t crush_hash;
    std::string nspace;
    std::string oid;
    snap_t snap;
    gen_t gen;
  };

  class Btree {
    class Cursor {
     public:
      Cursor() = default;
      Cursor(const Cursor& x) = default;
      ~Cursor() = default;

      onode_key_t key() const { return {}; }
      // might return Onode class to track the changing onode_t pointer
      onode_t* value() const { return nullptr; }
      bool operator==(const Cursor& x) const { return false; }
      bool operator!=(const Cursor& x) const { return !(*this == x); }
      Cursor& operator++() { return *this; }
      Cursor operator++(int) {
        Cursor tmp = *this;
        ++*this;
        return tmp;
      }
      Cursor& operator--() { return *this; }
      Cursor operator--(int) {
        Cursor tmp = *this;
        --*this;
        return tmp;
      }
    };

   public:
    // TODO: transaction
    // lookup
    Cursor begin() { return {}; }
    Cursor end() { return {}; }
    Cursor find(const onode_key_t& key) { return {}; }
    Cursor lower_bound(const onode_key_t& key) { return {}; }
    // modifiers
    std::pair<Cursor, bool>
    insert_or_assign(const onode_key_t& key, onode_t&& value) {
      return {{}, false};
    }
    std::pair<Cursor, bool>
    insert_or_assign(const Cursor& hint, const onode_key_t& key, onode_t&& value) {
      return {{}, false};
    }
    size_t erase(const onode_key_t& key) { return 0u; }
    Cursor erase(Cursor& pos) { return {}; }
    Cursor erase(Cursor& first, Cursor& last) { return {}; }
  };

}
