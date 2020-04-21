// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "crimson/common/type_helpers.h"

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
      assert(block_offset + sizeof(T) <= length);
      return static_cast<const T*>(ptr_offset(block_offset));
    }
    void copy_in(const void* from, loff_t block_offset, loff_t len) {
      assert(valid);
      assert(block_offset + len <= length);
      memcpy(ptr_offset(block_offset), from, len);
    }
    template <typename T>
    void copy_in(const T& from, loff_t block_offset) {
      copy_in(&from, block_offset, sizeof(from));
    }
    void zero(loff_t block_offset, loff_t len) {
      assert(valid);
      assert(block_offset + len <= length);
      memset(ptr_offset(block_offset), 0, len);
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
    void free_extent(Ref<LogicalCachedExtent> extent) {
      std::free(extent->ptr);
      auto size = allocate_map.erase(extent->get_laddr());
      assert(size == 1u);
      extent->invalidate();
    }
    void free_all() {
      for (auto& [addr, extent] : allocate_map) {
        free_extent(extent);
      }
      assert(allocate_map.empty());
    }
    Ref<LogicalCachedExtent> read_extent(laddr_t offset) {
      auto iter = allocate_map.find(offset);
      assert(iter != allocate_map.end());
      return iter->second;
    }

   private:
    std::map<laddr_t, Ref<LogicalCachedExtent>> allocate_map;
  } transaction_manager;

  enum class MatchKindKey : int8_t { NE = -1, EQ = 0, PO };
  /*
   * onode indexes
   */
  using shard_t = int8_t;
  using pool_t = int64_t;
  using crush_hash_t = uint32_t;
  // nspace, oid (variable)
  using snap_t = uint64_t;
  using gen_t = uint64_t;

  struct onode_key_t {
    shard_t shard;
    pool_t pool;
    crush_hash_t crush_hash;
    std::string nspace;
    std::string oid;
    snap_t snap;
    gen_t gen;
  };
  template <typename T>
  MatchKindKey _compare_crush(const onode_key_t& key, const T& target) {
    if (key.crush_hash < target.crush_hash)
      return MatchKindKey::NE;
    if (key.crush_hash > target.crush_hash)
      return MatchKindKey::PO;
    return MatchKindKey::EQ;
  }
  template <typename T>
  MatchKindKey _compare_shard_pool_crush(const onode_key_t& key, const T& target) {
    if (key.shard < target.shard)
      return MatchKindKey::NE;
    if (key.shard > target.shard)
      return MatchKindKey::PO;
    if (key.pool < target.pool)
      return MatchKindKey::NE;
    if (key.pool > target.pool)
      return MatchKindKey::PO;
    return _compare_crush(key, target);
  }
  template <typename T>
  MatchKindKey _compare_snap_gen(const onode_key_t& key, const T& target) {
    if (key.snap < target.snap)
      return MatchKindKey::NE;
    if (key.snap > target.snap)
      return MatchKindKey::PO;
    if (key.gen < target.gen)
      return MatchKindKey::NE;
    if (key.gen > target.gen)
      return MatchKindKey::PO;
    return MatchKindKey::EQ;
  }
  MatchKindKey compare_to(const onode_key_t& key, const onode_key_t& target) {
    auto ret = _compare_shard_pool_crush(key, target);
    if (ret != MatchKindKey::EQ)
      return ret;
    if (key.nspace < target.nspace)
      return MatchKindKey::NE;
    if (key.nspace > target.nspace)
      return MatchKindKey::PO;
    if (key.oid < target.oid)
      return MatchKindKey::NE;
    if (key.oid > target.oid)
      return MatchKindKey::PO;
    return _compare_snap_gen(key, target);
  }

  /*
   * fixed keys
   */
  // TODO: consider alignments
  struct fixed_key_0_t {
    shard_t shard;
    pool_t pool;
    crush_hash_t crush_hash;
  } __attribute__((packed));
  MatchKindKey compare_to(const onode_key_t& key, const fixed_key_0_t& target) {
    return _compare_shard_pool_crush(key, target);
  }

  struct fixed_key_1_t {
    crush_hash_t crush_hash;
  } __attribute__((packed));
  MatchKindKey compare_to(const onode_key_t& key, const fixed_key_1_t& target) {
    return _compare_crush(key, target);
  }

  struct fixed_key_3_t {
    snap_t snap;
    gen_t gen;
  } __attribute__((packed));
  MatchKindKey compare_to(const onode_key_t& key, const fixed_key_3_t& target) {
    return _compare_snap_gen(key, target);
  }

  /*
   * btree block layouts
   */
  constexpr loff_t NODE_BLOCK_SIZE = 1u << 12;
  // TODO: decide by NODE_BLOCK_SIZE
  using node_offset_t = uint16_t;

  enum class field_type_t : uint8_t {
    N0 = 0x3e,
    N1,
    N2,
    N3,
    _MAX
  };
  enum class node_type_t : uint8_t {
    LEAF = 0,
    INTERNAL
  };
  using level_t = uint8_t;
  struct node_header_t {
    uint8_t field_type : 7;
    uint8_t node_type : 1;
    level_t level;

    node_header_t() {}
    node_header_t(field_type_t field_type, node_type_t node_type, level_t _level) {
      set_field_type(field_type);
      set_node_type(node_type);
      level = _level;
    }
    field_type_t get_field_type() const {
      return static_cast<field_type_t>(field_type);
    }
    void set_field_type(field_type_t type) {
      field_type = static_cast<uint8_t>(type);
    }
    node_type_t get_node_type() const {
      return static_cast<node_type_t>(node_type);
    }
    void set_node_type(node_type_t type) {
      node_type = static_cast<uint8_t>(type);
    }
  } __attribute__((packed));
  static_assert(static_cast<uint8_t>(field_type_t::_MAX) <= 1u<<8);

  template <typename fixed_key_type>
  struct _slot_t {
    fixed_key_type key;
    node_offset_t right_offset;
  } __attribute__((packed));
  using slot_0_t = _slot_t<fixed_key_0_t>;
  using slot_1_t = _slot_t<fixed_key_1_t>;
  using slot_3_t = _slot_t<fixed_key_3_t>;

  enum class MatchKindItem : int8_t { NE = -1, EQ = 0 };
  struct search_result_item_t {
    size_t position;
    MatchKindItem match;
  };
  search_result_item_t binary_search(
      const onode_Key_t& key, const T* array, size_t begin, size_t end) {
    assert(begin <= end);
    while (begin < end) {
      auto mid = (begin + end) >> 1;
      const T& target = *(array + mid);
      auto match = compare_to(key, target);
      if (match == MatchKindKey::NE) {
        end = mid;
      } else if (match == MatchKindKey::OP) {
        begin = mid + 1;
      } else {
        return {mid, MatchKindItem::EQ};
      }
    }
    return {begin , MatchKindItem::NE};
  }

  template <typename slot_type, field_type_t _field_type>
  struct _node_fields_013_t {
    static constexpr field_type_t field_type = _field_type;

    node_header_t header;
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(slot_type), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    num_keys_t num_keys = 0u;
    slot_type slots[];

    //lookup_internal(const onode_key_t& key);
    //lookup_leaf(const onode_key_t& key);
  } __attribute__((packed));
  using node_fields_0_t = _node_fields_013_t<slot_0_t, field_type_t::N0>;
  using node_fields_1_t = _node_fields_013_t<slot_1_t, field_type_t::N1>;

  struct node_fields_2_t {
    static constexpr field_type_t field_type = field_type_t::N2;

    node_header_t header;
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(node_off_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    num_keys_t num_keys = 0u;
    node_offset_t offsets[];

    //lookup_internal(const onode_key_t& key);
    //lookup_leaf(const onode_key_t& key);
  } __attribute__((packed));

  // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
  static constexpr unsigned MAX_NUM_KEYS_I3 = 170;
  template <unsigned MAX_NUM_KEYS>
  struct _internal_fields_3_t {
    static constexpr field_type_t field_type = field_type_t::N3;

    node_header_t header;
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
    using num_keys_t = uint8_t;
    num_keys_t num_keys = 0u;
    fixed_key_3_t keys[MAX_NUM_KEYS];
    laddr_t child_addrs[MAX_NUM_KEYS + 1];

    //lookup_internal(const onode_key_t& key);
  } __attribute__((packed));
  static_assert(sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3>) <= NODE_BLOCK_SIZE &&
                sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3 + 1>) > NODE_BLOCK_SIZE);
  using internal_fields_3_t = _internal_fields_3_t<MAX_NUM_KEYS_I3>;

  using leaf_fields_3_t = _node_fields_013_t<slot_3_t, field_type_t::N3>;

  class Node
    : public boost::intrusive_ref_counter<Node, boost::thread_unsafe_counter> {
   public:
    virtual node_type_t node_type() const = 0;
    virtual field_type_t field_type() const = 0;
    laddr_t laddr() const {
      return extent->get_laddr();
    }
    level_t level() const {
      return extent->get_ptr<node_header_t>(0u)->level;
    }

   protected:
    Node() {}

    //virtual SearchResult lower_bound(const onode_key_t& key) = 0;

    // might be asynchronous
    void alloc_extent(size_t num_keys_size, level_t level) {
      extent = transaction_manager.alloc_extent(NODE_BLOCK_SIZE);
      extent->copy_in(node_header_t{field_type(), node_type(), level}, 0u);
      extent->zero(sizeof(node_header_t), num_keys_size);
    }

    Ref<LogicalCachedExtent> extent;
  };

  template <typename FieldType, typename ConcreteType>
  class InternalNodeT : public Node {
   public:
    node_type_t node_type() const override { return node_type_t::INTERNAL; }
    field_type_t field_type() const override { return FieldType::field_type; }

    static Ref<ConcreteType> allocate(level_t level) {
      assert(level != 0u);
      auto ret = Ref<ConcreteType>(new ConcreteType());
      ret->alloc_extent(sizeof(FieldType::num_keys), level);
      return ret;
    }
  };

  class InternalNode0 : public InternalNodeT<node_fields_0_t, InternalNode0> {
  };

  class InternalNode1 : public InternalNodeT<node_fields_1_t, InternalNode1> {
  };

  class InternalNode2 : public InternalNodeT<node_fields_2_t, InternalNode2> {
  };

  class InternalNode3 : public InternalNodeT<internal_fields_3_t, InternalNode3> {
  };

  class LeafNode : public Node {
  };

  template <typename FieldType, typename ConcreteType>
  class LeafNodeT: public LeafNode {
   public:
    node_type_t node_type() const override { return node_type_t::LEAF; }
    field_type_t field_type() const override { return FieldType::field_type; }

    static Ref<ConcreteType> allocate() {
      auto ret = Ref<ConcreteType>(new ConcreteType());
      ret->alloc_extent(sizeof(FieldType::num_keys), 0u);
      return ret;
    }
  };

  class LeafNode0 : public LeafNodeT<node_fields_0_t, LeafNode0> {
  };

  class LeafNode1 : public LeafNodeT<node_fields_1_t, LeafNode1> {
  };

  class LeafNode2 : public LeafNodeT<node_fields_2_t, LeafNode2> {
  };

  class LeafNode3 : public LeafNodeT<leaf_fields_3_t, LeafNode3> {
  };

  /*
   * btree interfaces
   * requirements are based on:
   *   ceph::os::Transaction::create/touch/remove()
   *   ceph::ObjectStore::collection_list()
   *   ceph::BlueStore::get_onode()
   *   db->get_iterator(PREFIIX_OBJ) by ceph::BlueStore::fsck()
   */
  class Btree {
    class Cursor {
     public:
      Cursor() = default;
      Cursor(const Cursor& x) = default;
      ~Cursor() = default;

      const onode_key_t& key() const { return {}; }
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
    bool contains(const onode_key_t& key) { return false; }
    Cursor find(const onode_key_t& key) { return {}; }
    Cursor lower_bound(const onode_key_t& key) {
      //return Cursor(root_node->lower_bound(key));
      return {};
    }
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

    static Btree& get() {
      static std::unique_ptr<Btree> singleton;
      if (!singleton) {
        singleton.reset(new Btree(LeafNode0::allocate()));
      }
      return *singleton;
    }

   private:
    Btree(Ref<Node> root_node) : root_node{root_node} {}
    Btree(const Btree&) = delete;
    Btree(Btree&&) = delete;
    Btree& operator=(const Btree&) = delete;

    Ref<Node> root_node;
  };

}
