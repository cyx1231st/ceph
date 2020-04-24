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
  constexpr node_offset_t NODE_BLOCK_SIZE = 1u << 12;

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
  constexpr unsigned FIELD_BITS = 7u;
  struct node_header_t {
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
      auto p_key = p_size - *size;
      key = static_cast<const char*>(p_key);
    }
    const char* p_start() const { return key; }

    const char* key;
    const string_size_t* size;
  };
  MatchKindCMP compare_to(const std::string& key, const string_key_view_t& target) {
    return toMatchKindCMP(key.compare(0u, key.length(), target.key, *target.size));
  }

  struct variable_key_t {
    variable_key_t(const char* p_end) : nspace(p_end), oid(nspace.p_start()) {}
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
      // TODO: overflow
      IndexType mid = (begin + end) >> 1;
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

  template <typename FieldType>
  search_result_bs_t<typename FieldType::num_keys_t>
  fields_lower_bound(const FieldType& node_fields, const onode_key_t& key) {
    using num_keys_t = typename FieldType::num_keys_t;
    return binary_search(key, num_keys_t(0u), node_fields.num_keys,
        [&node_fields] (num_keys_t index) { return node_fields.get_key(index); });
  }

  struct item_range_t {
    const char* p_start;
    const char* p_end;
  };
  template <typename FieldType>
  item_range_t fields_item_range(
      const FieldType& node_fields, typename FieldType::num_keys_t index) {
    node_offset_t item_start_offset = node_fields.get_offset(index);
    node_offset_t item_end_offset =
      (index == 0u ? NODE_BLOCK_SIZE : node_fields.get_offset(index - 1));
    assert(item_start_offset < item_end_offset);
    const char* p_start = reinterpret_cast<const char*>(&node_fields);
    return {p_start + item_start_offset, p_start + item_end_offset};
  }

  template <typename SlotType>
  struct _node_fields_013_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(SlotType), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    using key_t = typename SlotType::key_t;
    static constexpr field_type_t FIELD_TYPE = SlotType::FIELD_TYPE;

    const key_t& get_key(num_keys_t index) const {
      assert(index < num_keys);
      return slots[index].key;
    }
    node_offset_t get_offset(num_keys_t index) const {
      assert(index < num_keys);
      auto offset = slots[index].right_offset;
      assert(offset <= NODE_BLOCK_SIZE);
      return offset;
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

    key_t get_key(num_keys_t index) const {
      assert(index < num_keys);
      node_offset_t item_end_offset =
        (index == 0 ? NODE_BLOCK_SIZE : offsets[index - 1]);
      assert(item_end_offset <= NODE_BLOCK_SIZE);
      const char* p_start = reinterpret_cast<const char*>(this);
      return key_t(p_start + item_end_offset);
    }
    node_offset_t get_offset(num_keys_t index) const {
      assert(index < num_keys);
      auto offset = offsets[index];
      assert(offset <= NODE_BLOCK_SIZE);
      return offset;
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
    static constexpr field_type_t FIELD_TYPE = field_type_t::N3;

    const key_t& get_key(num_keys_t index) const {
      assert(index < num_keys);
      return keys[index];
    }

    node_header_t header;
    num_keys_t num_keys = 0u;
    key_t keys[MAX_NUM_KEYS];
    laddr_t child_addrs[MAX_NUM_KEYS + 1];
  } __attribute__((packed));
  static_assert(sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3>) <= NODE_BLOCK_SIZE &&
                sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3 + 1>) > NODE_BLOCK_SIZE);
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
    const onode_t* onode;

    leaf_sub_item_t(const char* p_start, const char* p_end) {
      assert(p_start < p_end);
      auto p_key = p_end - sizeof(fixed_key_3_t);
      assert(p_start < p_key);
      key = reinterpret_cast<const fixed_key_3_t*>(p_key);
      onode = reinterpret_cast<const onode_t*>(p_start);
      assert(p_start + onode->size == p_key);
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
      : collision_offset(reinterpret_cast<const node_offset_t*>(_p_end - sizeof(node_offset_t))),
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

  class LeafNode;
  struct search_result_t {
    Ref<LeafNode> leaf_node;
    size_t position;
    MatchKindBS match;
  };

  class Node
    : public boost::intrusive_ref_counter<Node, boost::thread_unsafe_counter> {
   public:
    virtual ~Node() = default;

    virtual node_type_t node_type() const = 0;
    virtual field_type_t field_type() const = 0;
    virtual size_t items() const = 0;
    virtual size_t keys() const = 0;
    virtual search_result_t lower_bound(const onode_key_t& key) { return {}; }

    laddr_t laddr() const {
      return extent->get_laddr();
    }
    level_t level() const {
      return extent->get_ptr<node_header_t>(0u)->level;
    }

   protected:
    Node() {}

    Ref<LogicalCachedExtent> extent;
  };

  template <typename FieldType, typename ConcreteType>
  class NodeT : virtual public Node {
   protected:
    using num_keys_t = typename FieldType::num_keys_t;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;

   public:
    virtual ~NodeT() = default;

    node_type_t node_type() const override final { return ConcreteType::NODE_TYPE; }
    field_type_t field_type() const override final { return FIELD_TYPE; }
    size_t keys() const override final { return fields().num_keys; }

   protected:
    const FieldType& fields() const {
      return *extent->get_ptr<FieldType>(0u);
    }

    static Ref<ConcreteType> _allocate(level_t level) {
      // might be asynchronous
      auto extent = transaction_manager.alloc_extent(NODE_BLOCK_SIZE);
      extent->copy_in(node_header_t{FIELD_TYPE, ConcreteType::NODE_TYPE, level}, 0u);
      extent->copy_in(num_keys_t(0u), sizeof(node_header_t));
      auto ret = Ref<ConcreteType>(new ConcreteType());
      ret->extent = extent;
      return ret;
    }
  };

  template <typename FieldType, typename ConcreteType>
  class InternalNodeT : public NodeT<FieldType, ConcreteType> {
   public:
    static constexpr node_type_t NODE_TYPE = node_type_t::INTERNAL;

    virtual ~InternalNodeT() = default;

    size_t items() const override final { return this->keys() + 1; }

    static Ref<ConcreteType> allocate(level_t level) {
      assert(level != 0u);
      return ConcreteType::_allocate(level);
    }
  };

  class InternalNode0 final : public NodeT<node_fields_0_t, InternalNode0> {
  };

  class InternalNode1 final : public NodeT<node_fields_1_t, InternalNode1> {
  };

  class InternalNode2 final : public NodeT<node_fields_2_t, InternalNode2> {
  };

  class InternalNode3 final : public NodeT<internal_fields_3_t, InternalNode3> {
  };

  class LeafNode : virtual public Node {
  };

  template <typename FieldType, typename ConcreteType>
  class LeafNodeT: public LeafNode, public NodeT<FieldType, ConcreteType> {
   public:
    static constexpr node_type_t NODE_TYPE = node_type_t::LEAF;

    virtual ~LeafNodeT() = default;

    size_t items() const override final { return keys(); }

    static Ref<ConcreteType> allocate() {
      return ConcreteType::_allocate(0u);
    }
  };

  class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {
  };

  class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {
  };

  class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {
  };

  class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {
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
    // TODO: track cursors in LeafNode (intrusive)
    class Cursor {
     public:
      Cursor(Btree* tree, search_result_t result) : tree(*tree) {
        if (result.position == result.leaf_node->items()) {
          // Cursor::make_end()
          assert(result.match == MatchKindBS::NE);
          position = std::numeric_limits<size_t>::max();
        } else {
          leaf_node = result.leaf_node;
          position = result.position;
        }
      }
      Cursor(const Cursor& x) = default;
      ~Cursor() = default;

      bool is_end() const { return leaf_node == nullptr; }
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

      static Cursor make_end(Btree* tree) { return Cursor(tree); }

     private:
      Cursor(Btree* tree) : tree(*tree), position(std::numeric_limits<size_t>::max()) {}

      Btree& tree;
      Ref<LeafNode> leaf_node;
      size_t position;
      std::optional<onode_key_t> key_copy;
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
