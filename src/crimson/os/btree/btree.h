// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <type_traits>
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
      auto size = allocate_map.erase(extent->get_laddr());
      assert(size == 1u);
      std::free(extent->ptr);
      extent->invalidate();
    }
    void free_all() {
      for (auto& [addr, extent] : allocate_map) {
        free_extent(extent);
      }
      assert(allocate_map.empty());
    }
    Ref<LogicalCachedExtent> read_extent(laddr_t addr) {
      auto iter = allocate_map.find(addr);
      assert(iter != allocate_map.end());
      return iter->second;
    }

   private:
    std::map<laddr_t, Ref<LogicalCachedExtent>> allocate_map;
  } transaction_manager;

  enum class MatchKindCMP : int8_t { NE = -1, EQ = 0, PO };
  MatchKindCMP toMatchKindCMP(int value) {
    if (value > 0) {
      return MatchKindCMP::PO;
    } else if (value < 0) {
      return MatchKindCMP::NE;
    } else {
      return MatchKindCMP::EQ;
    }
  }

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
  MatchKindCMP _compare_crush(const onode_key_t& key, const T& target) {
    if (key.crush_hash < target.crush_hash)
      return MatchKindCMP::NE;
    if (key.crush_hash > target.crush_hash)
      return MatchKindCMP::PO;
    return MatchKindCMP::EQ;
  }
  template <typename T>
  MatchKindCMP _compare_shard_pool_crush(const onode_key_t& key, const T& target) {
    if (key.shard < target.shard)
      return MatchKindCMP::NE;
    if (key.shard > target.shard)
      return MatchKindCMP::PO;
    if (key.pool < target.pool)
      return MatchKindCMP::NE;
    if (key.pool > target.pool)
      return MatchKindCMP::PO;
    return _compare_crush(key, target);
  }
  template <typename T>
  MatchKindCMP _compare_snap_gen(const onode_key_t& key, const T& target) {
    if (key.snap < target.snap)
      return MatchKindCMP::NE;
    if (key.snap > target.snap)
      return MatchKindCMP::PO;
    if (key.gen < target.gen)
      return MatchKindCMP::NE;
    if (key.gen > target.gen)
      return MatchKindCMP::PO;
    return MatchKindCMP::EQ;
  }
  MatchKindCMP compare_to(const onode_key_t& key, const onode_key_t& target) {
    auto ret = _compare_shard_pool_crush(key, target);
    if (ret != MatchKindCMP::EQ)
      return ret;
    if (key.nspace < target.nspace)
      return MatchKindCMP::NE;
    if (key.nspace > target.nspace)
      return MatchKindCMP::PO;
    if (key.oid < target.oid)
      return MatchKindCMP::NE;
    if (key.oid > target.oid)
      return MatchKindCMP::PO;
    return _compare_snap_gen(key, target);
  }

  /*
   * btree block layouts
   */
  // TODO: decide by NODE_BLOCK_SIZE
  using node_offset_t = uint16_t;
  constexpr node_offset_t BLOCK_SIZE = 1u << 12;
  constexpr node_offset_t NODE_BLOCK_SIZE = BLOCK_SIZE * 1u;

  constexpr uint8_t FIELD_TYPE_MAGIC = 0x3e;
  enum class field_type_t : uint8_t {
    N0 = FIELD_TYPE_MAGIC,
    N1,
    N2,
    N3,
    _MAX
  };
  std::ostream& operator<<(std::ostream &os, const field_type_t& type) {
    const char* const names[] = {"0", "1", "2", "3"};
    auto index = static_cast<uint8_t>(type) - FIELD_TYPE_MAGIC;
    assert(index < static_cast<uint8_t>(field_type_t::_MAX));
    os << names[index];
    return os;
  }

  enum class node_type_t : uint8_t {
    LEAF = 0,
    INTERNAL
  };
  std::ostream& operator<<(std::ostream &os, const node_type_t& type) {
    const char* const names[] = {"L", "I"};
    auto index = static_cast<uint8_t>(type);
    assert(index <= 1u);
    os << names[index];
    return os;
  }

  using level_t = uint8_t;
  constexpr unsigned FIELD_BITS = 7u;
  struct node_header_t {
    node_header_t() {}
    node_header_t(field_type_t field_type, node_type_t node_type, level_t _level) {
      set_field_type(field_type);
      set_node_type(node_type);
      level = _level;
    }
    std::optional<field_type_t> get_field_type() const {
      if (field_type >= FIELD_TYPE_MAGIC &&
          field_type < static_cast<uint8_t>(field_type_t::_MAX)) {
        return static_cast<field_type_t>(field_type);
      } else {
        return std::nullopt;
      }
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

    uint8_t field_type : FIELD_BITS;
    uint8_t node_type : 8u - FIELD_BITS;
    level_t level;
  } __attribute__((packed));
  static_assert(static_cast<uint8_t>(field_type_t::_MAX) <= 1u << FIELD_BITS);

  // TODO: consider alignments
  struct fixed_key_0_t {
    static constexpr field_type_t FIELD_TYPE = field_type_t::N0;
    shard_t shard;
    pool_t pool;
    crush_hash_t crush_hash;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const fixed_key_0_t& target) {
    return _compare_shard_pool_crush(key, target);
  }

  struct fixed_key_1_t {
    static constexpr field_type_t FIELD_TYPE = field_type_t::N1;
    crush_hash_t crush_hash;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const fixed_key_1_t& target) {
    return _compare_crush(key, target);
  }

  struct fixed_key_3_t {
    static constexpr field_type_t FIELD_TYPE = field_type_t::N3;
    snap_t snap;
    gen_t gen;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const fixed_key_3_t& target) {
    return _compare_snap_gen(key, target);
  }

  struct string_key_view_t {
    // presumably the maximum string size is 2KiB
    using string_size_t = uint16_t;
    string_key_view_t(const char* p_end) {
      auto p_size = p_end - sizeof(string_size_t);
      size = reinterpret_cast<const string_size_t*>(p_size);
      if (*size && *size != std::numeric_limits<string_size_t>::max()) {
        auto p_key = p_size - *size;
        key = static_cast<const char*>(p_key);
      } else {
        key = nullptr;
      }
    }
    bool is_smallest() const { return *size == 0u; }
    bool is_largest() const { return *size == std::numeric_limits<string_size_t>::max(); }
    const char* p_start() const {
      if (key) {
        return key;
      } else {
        return reinterpret_cast<const char*>(size);
      }
    }
    const char* p_next_end() const {
      if (key) {
        return p_start();
      } else {
        return reinterpret_cast<const char*>(size) + sizeof(string_size_t);
      }
    }

    const char* key;
    const string_size_t* size;
  };
  MatchKindCMP compare_to(const std::string& key, const string_key_view_t& target) {
    assert(key.length());
    if (target.is_smallest()) {
      return MatchKindCMP::PO;
    }
    if (target.is_largest()) {
      return MatchKindCMP::NE;
    }
    assert(target.key);
    return toMatchKindCMP(key.compare(0u, key.length(), target.key, *target.size));
  }

  struct variable_key_t {
    variable_key_t(const char* p_end) : nspace(p_end), oid(nspace.p_next_end()) {}
    const char* p_start() const { return nspace.p_start(); }

    string_key_view_t nspace;
    string_key_view_t oid;
  };
  MatchKindCMP compare_to(const onode_key_t& key, const variable_key_t& target) {
    auto ret = compare_to(key.nspace, target.nspace);
    if (ret != MatchKindCMP::EQ)
      return ret;
    return compare_to(key.oid, target.oid);
  }

  enum class MatchKindBS : int8_t { NE = -1, EQ = 0 };
  template  <typename IndexType>
  struct search_result_bs_t {
    IndexType position;
    MatchKindBS match;
  };
  template <typename FGetKey, typename IndexType>
  search_result_bs_t<IndexType> binary_search(
      const onode_key_t& key, IndexType begin, IndexType end, FGetKey&& f_get_key) {
    assert(begin <= end);
    while (begin < end) {
      unsigned total = begin + end;
      IndexType mid = total >> 1;
      decltype(f_get_key(mid)) target = f_get_key(mid);
      auto match = compare_to(key, target);
      if (match == MatchKindCMP::NE) {
        end = mid;
      } else if (match == MatchKindCMP::PO) {
        begin = mid + 1;
      } else {
        return {mid, MatchKindBS::EQ};
      }
    }
    return {begin , MatchKindBS::NE};
  }

  template <typename FixedKeyType>
  struct _slot_t {
    using key_t = FixedKeyType;
    static constexpr field_type_t FIELD_TYPE = FixedKeyType::FIELD_TYPE;

    key_t key;
    node_offset_t right_offset;
  } __attribute__((packed));
  using slot_0_t = _slot_t<fixed_key_0_t>;
  using slot_1_t = _slot_t<fixed_key_1_t>;
  using slot_3_t = _slot_t<fixed_key_3_t>;

  enum class MatchKindStr { UNEQ, EQNE, EQPO };

  template <typename FieldType>
  search_result_bs_t<typename FieldType::num_keys_t>
  fields_lower_bound(const FieldType& node, const onode_key_t& key, MatchKindStr& s_match) {
    using num_keys_t = typename FieldType::num_keys_t;
    if constexpr (FieldType::FIELD_TYPE == field_type_t::N2) {
      // TODO: string key dedup for N2
      assert(false);
    }

    auto ret = binary_search(key, num_keys_t(0u), node.num_keys,
        [&node] (num_keys_t index) { return node.get_key(index); });

#ifndef NDEBUG
    if constexpr (FieldType::FIELD_TYPE <= field_type_t::N1) {
      if (s_match == MatchKindStr::EQNE) {
        assert(ret.position == (node.num_keys - 1) && ret.match == MatchKindBS::EQ);
      } else if (s_match == MatchKindStr::EQPO) {
        assert(ret.position == 0u);
      }
    }
#endif
    assert(ret.position <= node.num_keys);
    return ret;
  }

  template <typename FieldType>
  const char* fields_start(const FieldType& node) {
    return reinterpret_cast<const char*>(&node);
  }

  struct item_range_t {
    const char* p_start;
    const char* p_end;
  };
  template <typename FieldType>
  item_range_t fields_item_range(
      const FieldType& node, typename FieldType::num_keys_t index) {
    node_offset_t item_start_offset = node.get_item_start_offset(index);
    node_offset_t item_end_offset =
      (index == 0u ? FieldType::SIZE : node.get_item_start_offset(index - 1));
    assert(item_start_offset < item_end_offset);
    return {fields_start(node) + item_start_offset,
            fields_start(node) + item_end_offset};
  }

  template <node_type_t NodeType, typename FieldType>
  node_offset_t fields_free_size(const FieldType& node, bool is_level_tail) {
    node_offset_t offset_start = node.get_key_start_offset(node.num_keys);
    node_offset_t offset_end =
      (node.num_keys == 0 ? FieldType::SIZE
                          : node.get_item_start_offset(node.num_keys - 1));
    if constexpr (NodeType == node_type_t::INTERNAL) {
      if (is_level_tail) {
        offset_end -= sizeof(laddr_t);
      }
    }
    assert(offset_start <= offset_end);
    auto free = offset_end - offset_start;
    assert(free < FieldType::SIZE);
    return free;
  }

  template <typename FieldType>
  const laddr_t& fields_last_child_addr(const FieldType& node) {
    node_offset_t offset_start = node.get_item_start_offset(node.num_keys - 1);
    assert(offset_start <= FieldType::SIZE);
    offset_start -= sizeof(laddr_t);
    auto p_addr = fields_start(node) + offset_start;
    return *reinterpret_cast<const laddr_t*>(p_addr);
  }

  template <typename SlotType>
  struct _node_fields_013_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(SlotType), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    using key_t = typename SlotType::key_t;
    using my_type_t = _node_fields_013_t<SlotType>;
    static constexpr field_type_t FIELD_TYPE = SlotType::FIELD_TYPE;
    static constexpr node_offset_t SIZE = NODE_BLOCK_SIZE;

    const key_t& get_key(num_keys_t index) const {
      assert(index < num_keys);
      return slots[index].key;
    }
    node_offset_t get_key_start_offset(num_keys_t index) const {
      assert(index <= num_keys);
      auto offset = offsetof(my_type_t, slots) + sizeof(SlotType) * index;
      assert(offset < SIZE);
      return offset;
    }
    node_offset_t get_item_start_offset(num_keys_t index) const {
      assert(index < num_keys);
      auto offset = slots[index].right_offset;
      assert(offset <= SIZE);
      return offset;
    }
    template <node_type_t NodeType>
    node_offset_t free_size(bool is_level_tail) const {
      return fields_free_size<NodeType>(*this, is_level_tail);
    }

    node_header_t header;
    num_keys_t num_keys = 0u;
    SlotType slots[];
  } __attribute__((packed));
  using node_fields_0_t = _node_fields_013_t<slot_0_t>;
  using node_fields_1_t = _node_fields_013_t<slot_1_t>;

  struct node_fields_2_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(node_off_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    using key_t = variable_key_t;
    static constexpr field_type_t FIELD_TYPE = field_type_t::N2;
    static constexpr node_offset_t SIZE = NODE_BLOCK_SIZE;

    key_t get_key(num_keys_t index) const {
      assert(index < num_keys);
      node_offset_t item_end_offset =
        (index == 0 ? SIZE : offsets[index - 1]);
      assert(item_end_offset <= SIZE);
      const char* p_start = fields_start(*this);
      return key_t(p_start + item_end_offset);
    }
    node_offset_t get_key_start_offset(num_keys_t index) const {
      assert(index <= num_keys);
      auto offset = offsetof(node_fields_2_t, offsets) +
                    sizeof(node_offset_t) * num_keys;
      assert(offset <= SIZE);
      return offset;
    }
    node_offset_t get_item_start_offset(num_keys_t index) const {
      assert(index < num_keys);
      auto offset = offsets[index];
      assert(offset <= SIZE);
      return offset;
    }
    template <node_type_t NodeType>
    node_offset_t free_size(bool is_level_tail) const {
      return fields_free_size<NodeType>(*this, is_level_tail);
    }

    node_header_t header;
    num_keys_t num_keys = 0u;
    node_offset_t offsets[];
  } __attribute__((packed));

  // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
  static constexpr unsigned MAX_NUM_KEYS_I3 = 170u;
  template <unsigned MAX_NUM_KEYS>
  struct _internal_fields_3_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
    using num_keys_t = uint8_t;
    using key_t = fixed_key_3_t;
    using my_type_t = _internal_fields_3_t<MAX_NUM_KEYS>;
    static constexpr field_type_t FIELD_TYPE = field_type_t::N3;
    static constexpr node_offset_t SIZE = sizeof(my_type_t);

    const key_t& get_key(num_keys_t index) const {
      assert(index < num_keys);
      return keys[index];
    }
    template <node_type_t NodeType,
              typename = std::enable_if_t<NodeType == node_type_t::INTERNAL>>
    node_offset_t free_size(bool is_level_tail) const {
      auto allowed_num_keys = is_level_tail ? MAX_NUM_KEYS - 1 : MAX_NUM_KEYS;
      assert(num_keys <= allowed_num_keys);
      auto free = (allowed_num_keys - num_keys) * (sizeof(key_t) + sizeof(laddr_t));
      assert(free < SIZE);
      return free;
    }

    node_header_t header;
    num_keys_t num_keys = 0u;
    key_t keys[MAX_NUM_KEYS];
    laddr_t child_addrs[MAX_NUM_KEYS];
  } __attribute__((packed));
  static_assert(_internal_fields_3_t<MAX_NUM_KEYS_I3>::SIZE <= NODE_BLOCK_SIZE &&
                _internal_fields_3_t<MAX_NUM_KEYS_I3 + 1>::SIZE > NODE_BLOCK_SIZE);
  using internal_fields_3_t = _internal_fields_3_t<MAX_NUM_KEYS_I3>;

  using leaf_fields_3_t = _node_fields_013_t<slot_3_t>;

  /*
   * block layout of a variable-sized item (right-side)
   *
   * for internal node type 0, 1:
   * previous off (block boundary) -----------------------------+
   * current off --+                                            |
   *               |                                            |
   *               V                                            V
   *        <==== |   sub |fix|sub |fix|oid char|ns char|colli-|
   *  (next-item) |...addr|key|addr|key|array & |array &|-sion |(prv-item)...
   *        <==== |   1   |1  |0   |0  |len     |len    |offset|
   *                ^                                      |
   *                |                                      |
   *                +------------ next collision ----------+
   * see internal_item_iterator_t
   *
   * for internal node type 2:
   * previous off (block boundary) ----------------------+
   * current off --+                                     |
   *               |                                     |
   *               V                                     V
   *        <==== |   sub |fix|sub |fix|oid char|ns char|
   *  (next-item) |...addr|key|addr|key|array & |array &|(prv-item)...
   *        <==== |   1   |1  |0   |0  |len     |len    |
   * see internal_item_t
   *
   * for leaf node type 0, 1:
   * previous off (block boundary) ----------------------------------------+
   * current off --+                                                       |
   *               |                                                       |
   *               V                                                       V
   *        <==== |   fix|o-  |fix|   off|off|num |oid char|ns char|colli-|
   *  (next-item) |...key|node|key|...set|set|sub |array & |array &|-sion |(prv-item)
   *        <==== |   1  |0   |0  |   1  |0  |keys|len     |len    |offset|
   *                ^                                                  |
   *                |                                                  |
   *                +------------ next collision ----------------------+
   * see leaf_item_iterator_t
   *
   * for leaf node type 2:
   * previous off (block boundary) ---------------------------------+
   * current off --+                                                |
   *               |                                                |
   *               V                                                V
   *        <==== |   fix|o-  |fix|   off|off|num |oid char|ns char|
   *  (next-item) |...key|node|key|...set|set|sub |array & |array &|(prv-item)
   *        <==== |   1  |0   |0  |   1  |0  |keys|len     |len    |
   * see leaf_item_t
   */

  struct internal_sub_item_t {
    fixed_key_3_t key;
    laddr_t child_addr;
  } __attribute__((packed));

  class internal_sub_items_t {
   public:
    using num_keys_t = size_t;

    internal_sub_items_t(const char* p_start, const char* p_end) {
      assert(p_start < p_end);
      assert((p_end - p_start) % sizeof(internal_sub_item_t) == 0);
      num_items = (p_end - p_start) / sizeof(internal_sub_item_t);
      assert(num_items > 0);
    }

    num_keys_t size() const { return num_items; }

    const internal_sub_item_t& operator[](size_t index) {
      assert(index < num_items);
      return *(first_item - index);
    }

   private:
    size_t num_items;
    const internal_sub_item_t* first_item;
  };

  struct leaf_sub_item_t {
    const fixed_key_3_t* key;
    const onode_t* value;

    leaf_sub_item_t(const char* p_start, const char* p_end) {
      assert(p_start < p_end);
      auto p_key = p_end - sizeof(fixed_key_3_t);
      assert(p_start < p_key);
      key = reinterpret_cast<const fixed_key_3_t*>(p_key);
      value = reinterpret_cast<const onode_t*>(p_start);
      assert(p_start + value->size == p_key);
    }
  };

  class leaf_sub_items_t {
   public:
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t),
    //       and the minimal size of onode_t
    using num_keys_t = uint8_t;

    leaf_sub_items_t(const char* p_start, const char* p_end) {
      assert(p_start < p_end);
      auto p_num_keys = p_end - sizeof(num_keys_t);
      assert(p_start < p_num_keys);
      num_keys = reinterpret_cast<const num_keys_t*>(p_num_keys);
      auto p_offsets = p_num_keys - sizeof(node_offset_t);
      assert(p_start < p_offsets);
      offsets = reinterpret_cast<const node_offset_t*>(p_offsets);
      p_items_end = reinterpret_cast<const char*>(&get_offset(size() - 1));
      assert(p_start < p_items_end);
      assert(p_start == get_item_start(size() - 1));
    }

    num_keys_t size() const { return *num_keys; }

    leaf_sub_item_t operator[](size_t index) {
      assert(index < size());
      return leaf_sub_item_t(get_item_start(index), get_item_end(index));
    }

   private:
    const char* get_item_start(size_t index) {
      assert(index < size());
      return p_items_end - get_offset(index);
    }

    const char* get_item_end(size_t index) {
      assert(index < size());
      return index == 0 ? p_items_end : p_items_end - get_offset(index - 1);
    }

    const node_offset_t& get_offset(size_t index) {
      assert(index < size());
      return *(offsets - index);
    }

    const num_keys_t* num_keys;
    const node_offset_t* offsets;
    const char* p_items_end;
  };

  template <typename SubItemsType>
  struct _item_t {
    _item_t(const char* p_start, const char* p_end)
      : key(p_end), sub_items(p_start, key.p_start()) {
      assert(p_start < p_end);
    }

    variable_key_t key;
    SubItemsType sub_items;
  };
  using internal_item_t = _item_t<internal_sub_items_t>;
  using leaf_item_t = _item_t<leaf_sub_items_t>;

  template <typename ItemType>
  class _item_iterator_t {
   public:
    _item_iterator_t(const char* p_start, const char* _p_end)
      : collision_offset(reinterpret_cast<const node_offset_t*>(
            _p_end - sizeof(node_offset_t))),
        p_start(p_start) { assert(p_start < p_end()); }

    bool has_next() {
      if (collision_offset != nullptr) {
        return true;
      } else {
        return false;
      }
    }

    ItemType next() {
      assert(has_next());
      auto p_item_end = p_end();
      auto back_offset = *collision_offset;
      const char* p_item_start;
      if (back_offset) {
        p_item_start = p_item_end - back_offset;
        assert(p_start < p_item_start);
        collision_offset = reinterpret_cast<const node_offset_t*>(
            p_item_start - sizeof(node_offset_t));
        assert(p_start < p_end());
      } else {
        p_item_start = p_start;
        collision_offset = nullptr;
      }
      return ItemType(p_item_start, p_item_end);
    }

   private:
    const char* p_end() const {
      return reinterpret_cast<const char*>(collision_offset);
    }

    const node_offset_t* collision_offset;
    const char* p_start;
  };
  using internal_item_iterator_t = _item_iterator_t<internal_item_t>;
  using leaf_item_iterator_t = _item_iterator_t<leaf_item_t>;

  struct search_position_item_t {
    bool is_end() const {
      return pos_collision == std::numeric_limits<size_t>::max();
    }

    bool operator==(const search_position_item_t& x) const {
      return pos_collision == x.pos_collision && pos_sub_item == x.pos_sub_item;
    }
    bool operator!=(const search_position_item_t& x) const { return !(*this == x); }
    bool operator<(const search_position_item_t& x) const {
      return std::make_pair(pos_collision, pos_sub_item) <
             std::make_pair(x.pos_collision, x.pos_sub_item);
    }

    static search_position_item_t end() {
      return {std::numeric_limits<size_t>::max(), 0u};
    }

    size_t pos_collision;
    size_t pos_sub_item;
  };

  template <node_type_t> struct item_type;
  template<> struct item_type<node_type_t::INTERNAL> { using type = const laddr_t*; };
  template<> struct item_type<node_type_t::LEAF> { using type = const onode_t*; };
  template <node_type_t NodeType>
  using item_type_t = typename item_type<NodeType>::type;

  template <node_type_t NodeType>
  struct search_result_item_t {
    bool is_end() const { return position.is_end(); }

    static search_result_item_t end() {
      return {search_position_item_t::end(),
              MatchKindStr::UNEQ, MatchKindBS::NE, nullptr};
    }

    search_position_item_t position;
    MatchKindStr s_match;
    MatchKindBS match;
    item_type_t<NodeType> value;
  };

  template <node_type_t NodeType, field_type_t FieldType,
            typename = std::enable_if_t<FieldType != field_type_t::N3>>
  search_result_item_t<NodeType> item_lower_bound(
      const item_range_t& item_range,
      const onode_key_t& key,
      MatchKindStr s_match) {
    return {};
  }

  template <node_type_t NodeType, field_type_t FieldType,
            typename = std::enable_if_t<FieldType != field_type_t::N3>>
  item_type_t<NodeType> item_get_value_ptr(
      const item_range_t& item_range, const search_position_item_t& position) {
    return nullptr;
  }

  struct search_position_t {
    bool is_end() const {
      return pos_key == std::numeric_limits<size_t>::max();
    }

    bool operator==(const search_position_t& x) const {
      return pos_key == x.pos_key && pos_item == x.pos_item;
    }
    bool operator!=(const search_position_t& x) const { return !(*this == x); }
    bool operator<(const search_position_t& x) const {
      return std::make_pair(pos_key, pos_item) <
             std::make_pair(x.pos_key, x.pos_item);
    }

    static search_position_t end() {
      return {std::numeric_limits<size_t>::max(), {0u, 0u}};
    }

    template <typename IndexType>
    static search_position_t from(const search_result_bs_t<IndexType>& result_left) {
      return {result_left.position, {0u, 0u}};
    }

    template <typename IndexType, node_type_t NodeType>
    static search_position_t from(const search_result_bs_t<IndexType>& result_left,
                                  const search_result_item_t<NodeType>& result_right) {
      assert(!result_right.is_end());
      return {result_left.position, result_right.position};
    }

    size_t pos_key;
    search_position_item_t pos_item;
  };

  class LeafNode;
  struct search_result_t {
    // TODO: deref LeafNode if destroyed with leaf_node available
    // TODO: make sure to deref LeafNode if is_end()
    template <typename IndexType>
    search_result_t(LeafNode* node,
                    const search_result_bs_t<IndexType>& result_left,
                    const onode_t* value)
      : leaf_node(node),
        position(search_position_t::from(result_left)),
        match(result_left.match),
        value(value) {}

    template <typename IndexType>
    search_result_t(LeafNode* node,
                    const search_result_bs_t<IndexType>& result_left,
                    const search_result_item_t<node_type_t::LEAF>& result_right)
      : leaf_node(node),
        position(search_position_t::from(result_left, result_right)),
        value(reinterpret_cast<const onode_t*>(result_right.value)) {}

    bool is_end() const { return position.is_end(); }

    static search_result_t end(Ref<LeafNode> node) { return {node}; }

    Ref<LeafNode> leaf_node;
    search_position_t position;
    MatchKindBS match;
    const onode_t* value;

   private:
    search_result_t(Ref<LeafNode> node)
      : leaf_node(node), position(search_position_t::end()),
        match(MatchKindBS::NE), value(nullptr) {}
  };

  class Node
    : public boost::intrusive_ref_counter<Node, boost::thread_unsafe_counter> {
   public:
    struct parent_info_t {
      search_position_t position;
      // TODO: Ref<InternalNode>
      Ref<Node> ptr;
    };

    virtual ~Node() = default;

    bool is_root() const { return !_parent_info.has_value(); }
    bool is_level_tail() const { return _is_level_tail; }
    const parent_info_t& parent_info() const { return *_parent_info; }
    virtual node_type_t node_type() const = 0;
    virtual field_type_t field_type() const = 0;
    virtual size_t items() const = 0;
    virtual size_t keys() const = 0;
    virtual size_t free_size() const = 0;
    virtual size_t total_size() const = 0;
    size_t filled_size() const { return total_size() - free_size(); }
    size_t extent_size() const { return extent->get_length(); }
    virtual search_result_t lower_bound(
        const onode_key_t& key, MatchKindStr s_match = MatchKindStr::UNEQ) = 0;

    laddr_t laddr() const { return extent->get_laddr(); }
    level_t level() const { return extent->get_ptr<node_header_t>(0u)->level; }

    static Ref<Node> load(laddr_t, bool is_level_tail, const parent_info_t&);

   protected:
    Node() {}

    void init(Ref<LogicalCachedExtent> _extent, bool _is_level_tail_) {
      assert(!extent);
      extent = _extent;
      assert(extent->get_ptr<node_header_t>(0u)->get_node_type() == node_type());
      assert(*extent->get_ptr<node_header_t>(0u)->get_field_type() == field_type());
#ifndef NDEBUG
      if (node_type() == node_type_t::INTERNAL) {
        assert(extent->get_ptr<node_header_t>(0u)->level > 0u);
      } else {
        assert(extent->get_ptr<node_header_t>(0u)->level == 0u);
      }
#endif
      _is_level_tail = _is_level_tail_;
    }

    void init(Ref<LogicalCachedExtent> _extent, bool _is_level_tail_, const parent_info_t& info) {
      assert(!_parent_info.has_value());
      _parent_info = info;
      init(_extent, _is_level_tail_);
    }

    Ref<LogicalCachedExtent> extent;

   private:
    std::optional<parent_info_t> _parent_info;
    bool _is_level_tail;

    friend std::ostream& operator<<(std::ostream&, const Node&);
  };
  std::ostream& operator<<(std::ostream& os, const Node& node) {
    os << "Node" << node.node_type() << node.field_type()
       << "@0x" << std::hex << node.laddr()
       << "+" << node.extent_size() << std::dec
       << (node.is_level_tail() ? "$" : "")
       << "(level=" << (unsigned)node.level()
       << ", keys=" << node.keys()
       << ", filled=" << node.filled_size() << "B"
       << ", free=" << node.free_size() << "B"
       << ")";
    return os;
  }

  class LeafNode : virtual public Node {
   public:
    virtual ~LeafNode() = default;

    size_t items() const override final { return keys(); }
  };

  template <typename FieldType, typename ConcreteType>
  class NodeT : virtual public Node {
   protected:
    using num_keys_t = typename FieldType::num_keys_t;
    static constexpr node_type_t NODE_TYPE = ConcreteType::NODE_TYPE;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;
    static constexpr node_offset_t TOTAL_SIZE = FieldType::SIZE;
    static constexpr node_offset_t EXTENT_SIZE =
      (TOTAL_SIZE + BLOCK_SIZE - 1u) / BLOCK_SIZE * BLOCK_SIZE;

   public:
    virtual ~NodeT() = default;

    node_type_t node_type() const override final { return NODE_TYPE; }
    field_type_t field_type() const override final { return FIELD_TYPE; }
    size_t keys() const override final { return fields().num_keys; }
    size_t free_size() const override final {
      return fields().template free_size<NODE_TYPE>(is_level_tail());
    }
    size_t total_size() const override final { return TOTAL_SIZE; }

   protected:
    const FieldType& fields() const {
      return *extent->get_ptr<FieldType>(0u);
    }

    static Ref<ConcreteType> _allocate(level_t level, bool is_level_tail) {
      // might be asynchronous
      auto extent = transaction_manager.alloc_extent(EXTENT_SIZE);
      extent->copy_in(node_header_t{FIELD_TYPE, NODE_TYPE, level}, 0u);
      extent->copy_in(num_keys_t(0u), sizeof(node_header_t));
      auto ret = Ref<ConcreteType>(new ConcreteType());
      ret->init(extent, is_level_tail);
      return ret;
    }
  };

  template <typename FieldType, typename ConcreteType>
  class InternalNodeT : public NodeT<FieldType, ConcreteType> {
   public:
    static constexpr node_type_t NODE_TYPE = node_type_t::INTERNAL;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;

    virtual ~InternalNodeT() = default;

    size_t items() const override final { return this->keys() + 1; }

    search_result_t
    lower_bound(const onode_key_t& key, MatchKindStr s_match) override final {
      auto result_left = fields_lower_bound(this->fields(), key, s_match);
      search_position_t position;
      laddr_t child_addr;
      if (result_left.position == this->keys()) {
        assert(result_left.match == MatchKindBS::NE);
        assert(this->is_level_tail());
        position = search_position_t::from(result_left);
        if constexpr (FIELD_TYPE == field_type_t::N3) {
          child_addr = this->fields().child_addrs[result_left.position];
        } else {
          child_addr = fields_last_child_addr(this->fields());
        }
      } else {
        if constexpr (FIELD_TYPE == field_type_t::N3) {
          position = search_position_t::from(result_left);
          child_addr = this->fields().child_addrs[result_left.position];
        } else {
          auto item_range =
            fields_item_range(this->fields(), result_left.position);
          if (result_left.match == MatchKindBS::NE) {
            position = search_position_t::from(result_left);
            child_addr = *item_get_value_ptr<NODE_TYPE, FIELD_TYPE>(
                item_range, position.pos_item);
          } else {
            auto result_right =
              item_lower_bound<NODE_TYPE, FIELD_TYPE>(item_range, key, s_match);
            if (result_right.is_end()) {
              ++result_left.position;
              // result_left.match = MatchKindBS::NE;
              position = search_position_t::from(result_left);
              if (position.pos_key == this->keys()) {
                assert(this->is_level_tail());
                child_addr = fields_last_child_addr(this->fields());
              } else {
                item_range = fields_item_range(this->fields(), position.pos_key);
                child_addr = *item_get_value_ptr<NODE_TYPE, FIELD_TYPE>(
                    item_range, position.pos_item);
              }
            } else {
              assert(result_right.value);
              position = search_position_t::from(result_left, result_right);
              child_addr = *static_cast<const laddr_t*>(result_right.value);
            }
          }
        }
      }

      Ref<Node> child;
      auto found = tracked_child_nodes.find(position);
      if (found != tracked_child_nodes.end()) {
        child = found->second;
        assert(child_addr == child->laddr());
        assert(position == child->parent_info().position);
        assert(this == child->parent_info().ptr);
#ifndef NDEBUG
        if (position.pos_key == this->keys()) {
          assert(this->is_level_tail() && child->is_level_tail());
        } else {
          assert(!child->is_level_tail());
        }
#endif
      } else {
        child = Node::load(child_addr,
                           this->is_level_tail() && position.pos_key == this->keys(),
                           {position, this});
        tracked_child_nodes.insert({position, child});
      }
      assert(this->level() - 1 == child->level());
      assert(this->field_type() <= child->field_type());
      // TODO: assert the right-most key of the child matches the parent index
      // TODO: optimize the lookup when the index is already matched
      return child->lower_bound(key, s_match);
    }

    static Ref<ConcreteType> allocate(level_t level, bool is_level_tail) {
      assert(level != 0u);
      return ConcreteType::_allocate(level, is_level_tail);
    }

   private:
    // TODO: intrusive
    std::map<search_position_t, Ref<Node>> tracked_child_nodes;
  };
  class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {};
  class InternalNode1 final : public InternalNodeT<node_fields_1_t, InternalNode1> {};
  class InternalNode2 final : public InternalNodeT<node_fields_2_t, InternalNode2> {};
  class InternalNode3 final : public InternalNodeT<internal_fields_3_t, InternalNode3> {};

  template <typename FieldType, typename ConcreteType>
  class LeafNodeT: public LeafNode, public NodeT<FieldType, ConcreteType> {
   public:
    static constexpr node_type_t NODE_TYPE = node_type_t::LEAF;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;

    virtual ~LeafNodeT() = default;

    search_result_t
    lower_bound(const onode_key_t& key, MatchKindStr s_match) override final {
      auto result_left = fields_lower_bound(this->fields(), key, s_match);
      if (result_left.position == keys()) {
        assert(result_left.match == MatchKindBS::NE);
        assert(this->is_level_tail());
        return search_result_t::end(this);
      } else {
        if constexpr (FIELD_TYPE == field_type_t::N3) {
          auto value_offset =
            this->fields().get_item_start_offset(result_left.position);
          auto value = reinterpret_cast<const onode_t*>(
              fields_start(this->fields()) + value_offset);
          assert(value_offset + value->size <= FieldType::SIZE);
          return search_result_t(this, result_left, value);
        } else {
          if (result_left.match == MatchKindBS::NE) {
            return search_result_t(this, result_left, nullptr);
          } else {
            auto item_range =
              fields_item_range(this->fields(), result_left.position);
            auto result_right =
              item_lower_bound<NODE_TYPE, FIELD_TYPE>(item_range, key, s_match);
            if (result_right.is_end()) {
              ++result_left.position;
              result_left.match = MatchKindBS::NE;
              if (result_left.position == this->keys()) {
                assert(this->is_level_tail());
                return search_result_t::end(this);
              } else {
                return search_result_t(this, result_left, nullptr);
              }
            } else {
              assert(result_right.value);
              return search_result_t(this, result_left, result_right);
            }
          }
        }
      }
    }

    static Ref<ConcreteType> allocate(bool is_level_tail) {
      return ConcreteType::_allocate(0u, is_level_tail);
    }
  };
  class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {};
  class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
  class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
  class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

  Ref<Node> Node::load(laddr_t addr, bool is_level_tail, const parent_info_t& parent_info) {
    // TODO: throw error if cannot read from address
    auto extent = transaction_manager.read_extent(addr);
    const auto header = extent->get_ptr<node_header_t>(0u);
    auto _field_type = header->get_field_type();
    if (!_field_type.has_value()) {
      throw std::runtime_error("load failed: bad field type");
    }
    auto _node_type = header->get_node_type();
    Ref<Node> ret;
    if (_field_type == field_type_t::N0) {
      if (_node_type == node_type_t::LEAF) {
        ret = new LeafNode0();
      } else {
        ret = new InternalNode0();
      }
    } else if (_field_type == field_type_t::N1) {
      if (_node_type == node_type_t::LEAF) {
        ret = new LeafNode1();
      } else {
        ret = new InternalNode1();
      }
    } else if (_field_type == field_type_t::N2) {
      if (_node_type == node_type_t::LEAF) {
        ret = new LeafNode2();
      } else {
        ret = new InternalNode2();
      }
    } else if (_field_type == field_type_t::N3) {
      if (_node_type == node_type_t::LEAF) {
        ret = new LeafNode3();
      } else {
        ret = new InternalNode3();
      }
    } else {
      assert(false);
    }
    ret->init(extent, is_level_tail, parent_info);
    return ret;
  }

  /*
   * btree interfaces
   * requirements are based on:
   *   ceph::os::Transaction::create/touch/remove()
   *   ceph::ObjectStore::collection_list()
   *   ceph::BlueStore::get_onode()
   *   db->get_iterator(PREFIIX_OBJ) by ceph::BlueStore::fsck()
   */
  class Btree {
    // TODO: track cursors in LeafNode by position (intrusive)
    class Cursor {
     public:
      Cursor(Btree* tree, const search_result_t& result)
        : tree(*tree), position(result.position), value_ptr(result.value) {
        if (!result.is_end()) {
          leaf_node = result.leaf_node;
        }
      }
      Cursor(const Cursor& x) = default;
      ~Cursor() = default;

      bool is_end() const { return position.is_end(); }
      const onode_key_t& key() { return {}; }
      // might return Onode class to track the changing onode_t pointer
      const onode_t* value() { return nullptr; }
      bool operator==(const Cursor& x) const {
        return leaf_node == x.leaf_node &&
               position == x.position; }
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

      static Cursor make_end(Btree* tree) { return Cursor(tree); }

     private:
      Cursor(Btree* tree)
        : tree(*tree), position(search_position_t::end()), value_ptr(nullptr) {}

      Btree& tree;
      Ref<LeafNode> leaf_node;
      search_position_t position;
      std::optional<onode_key_t> key_copy;
      const onode_t* value_ptr;
    };

   public:
    // TODO: transaction
    // lookup
    Cursor begin() { return Cursor::make_end(this); }
    Cursor end() { return Cursor::make_end(this); }
    bool contains(const onode_key_t& key) {
      // TODO: can be faster if contains() == true
      return MatchKindBS::EQ == root_node->lower_bound(key).match;
    }
    Cursor find(const onode_key_t& key) {
      auto result = root_node->lower_bound(key);
      if (result.match == MatchKindBS::EQ) {
        return Cursor(this, result);
      } else {
        return Cursor::make_end(this);
      }
    }
    Cursor lower_bound(const onode_key_t& key) {
      return Cursor(this, root_node->lower_bound(key));
    }
    // modifiers
    std::pair<Cursor, bool>
    insert_or_assign(const onode_key_t& key, onode_t&& value) {
      return {Cursor::make_end(this), false};
    }
    std::pair<Cursor, bool>
    insert_or_assign(const Cursor& hint, const onode_key_t& key, onode_t&& value) {
      return {Cursor::make_end(this), false};
    }
    size_t erase(const onode_key_t& key) { return 0u; }
    Cursor erase(Cursor& pos) { return Cursor::make_end(this); }
    Cursor erase(Cursor& first, Cursor& last) { return Cursor::make_end(this); }

    static Btree& get() {
      static std::unique_ptr<Btree> singleton;
      if (!singleton) {
        singleton.reset(new Btree(LeafNode0::allocate(true)));
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
