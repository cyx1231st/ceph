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
#include <sstream>
#include <type_traits>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/likely.h"

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
  } __attribute__((packed));
  std::ostream& operator<<(std::ostream& os, const onode_t& node) {
    return os << "onode(" << node.size << ")";
  }

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
    const void* copy_in(const void* from, loff_t to_block_offset, loff_t len) {
      assert(valid);
      assert(to_block_offset + len <= length);
      auto to = ptr_offset(to_block_offset);
      std::memcpy(to, from, len);
      return to;
    }
    template <typename T>
    const T* copy_in(const T& from, loff_t to_block_offset) {
      auto ret = copy_in(&from, to_block_offset, sizeof(from));
      return (const T*)ret;
    }
    void shift(loff_t from_block_offset, loff_t len, int shift_offset) {
      assert(valid);
      assert(from_block_offset + len <= length);
      assert((int)from_block_offset + shift_offset >= 0);
      auto to_block_offset = shift_offset + from_block_offset;
      assert(to_block_offset + len <= length);
      std::memmove(ptr_offset(to_block_offset), ptr_offset(from_block_offset), len);
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
  using snap_t = uint64_t;
  using gen_t = uint64_t;

  struct onode_key_t {
    shard_t shard;
    pool_t pool;
    crush_hash_t crush;
    std::string nspace;
    std::string oid;
    snap_t snap;
    gen_t gen;
  };
  template <typename T>
  MatchKindCMP _compare_shard_pool(const onode_key_t& key, const T& target) {
    if (key.shard < target.shard)
      return MatchKindCMP::NE;
    if (key.shard > target.shard)
      return MatchKindCMP::PO;
    if (key.pool < target.pool)
      return MatchKindCMP::NE;
    if (key.pool > target.pool)
      return MatchKindCMP::PO;
    return MatchKindCMP::EQ;
  }
  template <typename T>
  MatchKindCMP _compare_crush(const onode_key_t& key, const T& target) {
    if (key.crush < target.crush)
      return MatchKindCMP::NE;
    if (key.crush > target.crush)
      return MatchKindCMP::PO;
    return MatchKindCMP::EQ;
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
    auto ret = _compare_shard_pool(key, target);
    if (ret != MatchKindCMP::EQ)
      return ret;
    ret = _compare_crush(key, target);
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
  uint8_t to_unsigned(field_type_t type) {
    auto value = static_cast<uint8_t>(type);
    assert(value >= FIELD_TYPE_MAGIC);
    assert(value < static_cast<uint8_t>(field_type_t::_MAX));
    return value - FIELD_TYPE_MAGIC;
  }
  std::ostream& operator<<(std::ostream &os, field_type_t type) {
    const char* const names[] = {"0", "1", "2", "3"};
    auto index = to_unsigned(type);
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
  struct shard_pool_t {
    bool operator==(const shard_pool_t& x) const {
      return (shard == x.shard && pool == x.pool);
    }
    bool operator!=(const shard_pool_t& x) const { return !(*this == x); }
    static shard_pool_t from_key(const onode_key_t& key) {
      return {key.shard, key.pool};
    }

    shard_t shard;
    pool_t pool;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const shard_pool_t& target) {
    return _compare_shard_pool(key, target);
  }
  std::ostream& operator<<(std::ostream& os, const shard_pool_t& sp) {
    return os << (unsigned)sp.shard << "," << sp.pool;
  }

  struct crush_t {
    bool operator==(const crush_t& x) const { return crush == x.crush; }
    bool operator!=(const crush_t& x) const { return !(*this == x); }
    static crush_t from_key(const onode_key_t& key) { return {key.crush}; }

    crush_hash_t crush;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const crush_t& target) {
    return _compare_crush(key, target);
  }
  std::ostream& operator<<(std::ostream& os, const crush_t& c) {
    return os << c.crush;
  }

  struct shard_pool_crush_t {
    bool operator==(const shard_pool_crush_t& x) const {
      return (shard_pool == x.shard_pool && crush == x.crush);
    }
    bool operator!=(const shard_pool_crush_t& x) const { return !(*this == x); }
    static shard_pool_crush_t from_key(const onode_key_t& key) {
      return {shard_pool_t::from_key(key), crush_t::from_key(key)};
    }

    shard_pool_t shard_pool;
    crush_t crush;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const shard_pool_crush_t& target) {
    auto ret = _compare_shard_pool(key, target.shard_pool);
    if (ret != MatchKindCMP::EQ)
      return ret;
    return _compare_crush(key, target.crush);
  }
  std::ostream& operator<<(std::ostream& os, const shard_pool_crush_t& spc) {
    return os << spc.shard_pool << "," << spc.crush;
  }

  struct snap_gen_t {
    bool operator==(const snap_gen_t& x) const {
      return (snap == x.snap && gen == x.gen);
    }
    bool operator!=(const snap_gen_t& x) const { return !(*this == x); }
    static snap_gen_t from_key(const onode_key_t& key) {
      return {key.snap, key.gen};
    }

    snap_t snap;
    gen_t gen;
  } __attribute__((packed));
  MatchKindCMP compare_to(const onode_key_t& key, const snap_gen_t& target) {
    return _compare_snap_gen(key, target);
  }
  std::ostream& operator<<(std::ostream& os, const snap_gen_t& sg) {
    return os << sg.snap << "," << sg.gen;
  }

  struct string_key_view_t {
    enum class Type {MIN, STR, MAX};
    // presumably the maximum string length is 2KiB
    using string_size_t = uint16_t;
    string_key_view_t(const char* p_end) {
      auto p_length = p_end - sizeof(string_size_t);
      std::memcpy(&length, p_length, sizeof(string_size_t));
      if (length && length != std::numeric_limits<string_size_t>::max()) {
        auto _p_key = p_length - length;
        p_key = static_cast<const char*>(_p_key);
      } else {
        p_key = nullptr;
      }
    }
    string_key_view_t(const string_key_view_t& other) = default;
    string_key_view_t& operator=(const string_key_view_t& other) = default;

    Type type() const {
      if (length == 0u) {
        return Type::MIN;
      } else if (length == std::numeric_limits<string_size_t>::max()) {
        return Type::MAX;
      } else {
        return Type::STR;
      }
    }
    const char* p_start() const {
      if (p_key) {
        return p_key;
      } else {
        return p_length;
      }
    }
    const char* p_next_end() const {
      if (p_key) {
        return p_start();
      } else {
        return p_length + sizeof(string_size_t);
      }
    }
    size_t size() const { return length + sizeof(string_size_t); }
    bool operator==(const string_key_view_t& x) const {
      if (type() == x.type() && type() != Type::STR)
        return true;
      if (type() != x.type())
        return false;
      if (length != x.length)
        return false;
      return (memcmp(p_key, x.p_key, length) == 0);
    }
    bool operator!=(const string_key_view_t& x) const { return !(*this == x); }

    static void do_insert_str(const std::string& str,
                              node_offset_t& offset,
                              LogicalCachedExtent& extent) {
      offset -= sizeof(string_size_t);
      assert(str.length() < std::numeric_limits<string_size_t>::max());
      extent.copy_in((string_size_t)str.length(), offset);
      offset -= str.length();
      extent.copy_in(str.data(), offset, str.length());
    }

    static void do_insert_dedup(const string_key_view_t::Type& dedup_type,
                                node_offset_t& offset,
                                LogicalCachedExtent& extent) {
      offset -= sizeof(string_size_t);
      if (dedup_type == string_key_view_t::Type::MIN) {
        extent.copy_in((string_size_t)0u, offset);
      } else if (dedup_type == string_key_view_t::Type::MAX) {
        extent.copy_in(std::numeric_limits<string_size_t>::max(), offset);
      } else {
        assert(false);
      }
    }

    const char* p_key;
    const char* p_length;
    // TODO: remove if p_length is aligned
    string_size_t length;

    friend std::ostream& operator<<(std::ostream&, const string_key_view_t&);
  };
  MatchKindCMP compare_to(const std::string& key, const string_key_view_t& target) {
    assert(key.length());
    if (target.type() == string_key_view_t::Type::MIN) {
      return MatchKindCMP::PO;
    } else if (target.type() == string_key_view_t::Type::MAX) {
      return MatchKindCMP::NE;
    }
    assert(target.p_key);
    return toMatchKindCMP(key.compare(0u, key.length(), target.p_key, target.length));
  }
  std::ostream& operator<<(std::ostream& os, const string_key_view_t& view) {
    auto type = view.type();
    if (type == string_key_view_t::Type::MIN) {
      return os << "MIN";
    } else if (type == string_key_view_t::Type::MAX) {
      return os << "MAX";
    } else {
      return os << "\"" << std::string(view.p_key, 0, view.length) << "\"";
    }
  }

  struct ns_oid_view_t {
    using string_size_t = string_key_view_t::string_size_t;
    using Type = string_key_view_t::Type;

    ns_oid_view_t(const char* p_end) : nspace(p_end), oid(nspace.p_next_end()) {}
    ns_oid_view_t(const ns_oid_view_t& other) = default;
    ns_oid_view_t& operator=(const ns_oid_view_t& other) = default;
    Type type() const { return oid.type(); }
    const char* p_start() const { return oid.p_start(); }
    size_t size() const {
      if (type() == Type::STR) {
        return nspace.size() + oid.size();
      } else {
        return sizeof(string_size_t);
      }
    }
    bool operator==(const ns_oid_view_t& x) const {
      return (nspace == x.nspace && oid == x.oid);
    }
    bool operator!=(const ns_oid_view_t& x) const { return !(*this == x); }

    static node_offset_t estimate_size(const onode_key_t* key) {
      if (key == nullptr) {
        // size after deduplication
        return sizeof(string_size_t);
      } else {
        return 2 * sizeof(string_size_t) + key->nspace.size() + key->oid.size();
      }
    }

    static void do_insert(const onode_key_t& key,
                          const ns_oid_view_t::Type& dedup_type,
                          node_offset_t& offset,
                          LogicalCachedExtent& extent) {
      if (dedup_type == ns_oid_view_t::Type::STR) {
        string_key_view_t::do_insert_str(
            key.nspace, offset, extent);
        string_key_view_t::do_insert_str(
            key.oid, offset, extent);
      } else {
        string_key_view_t::do_insert_dedup(
            dedup_type, offset, extent);
      }
    }

    string_key_view_t nspace;
    string_key_view_t oid;
  };
  MatchKindCMP compare_to(const onode_key_t& key, const ns_oid_view_t& target) {
    auto ret = compare_to(key.nspace, target.nspace);
    if (ret != MatchKindCMP::EQ)
      return ret;
    return compare_to(key.oid, target.oid);
  }
  std::ostream& operator<<(std::ostream& os, const ns_oid_view_t& ns_oid) {
    return os << ns_oid.nspace << "," << ns_oid.oid;
  }

  struct index_view_t {
    bool match_parent(const index_view_t& index) const {
      assert(p_snap_gen != nullptr);
      assert(index.p_snap_gen != nullptr);
      if (*p_snap_gen != *index.p_snap_gen)
        return false;

      if (!p_ns_oid.has_value())
        return true;
      assert(p_ns_oid->type() != ns_oid_view_t::Type::MIN);
      assert(index.p_ns_oid.has_value());
      assert(index.p_ns_oid->type() != ns_oid_view_t::Type::MIN);
      if (p_ns_oid->type() != ns_oid_view_t::Type::MAX &&
          *p_ns_oid != *index.p_ns_oid) {
        return false;
      }

      if (p_crush == nullptr)
        return true;
      assert(index.p_crush != nullptr);
      if (*p_crush != *index.p_crush)
        return false;

      if (p_shard_pool == nullptr)
        return true;
      assert(index.p_shard_pool != nullptr);
      if (*p_shard_pool != *index.p_shard_pool)
        return false;

      return true;
    }

    void set(const crush_t& key) {
      assert(p_crush == nullptr);
      p_crush = &key;
    }
    void set(const shard_pool_crush_t& key) {
      set(key.crush);
      assert(p_shard_pool == nullptr);
      p_shard_pool = &key.shard_pool;
    }
    void set(const ns_oid_view_t& key) {
      assert(!p_ns_oid.has_value());
      p_ns_oid = key;
    }
    void set(const snap_gen_t& key) {
      assert(p_snap_gen == nullptr);
      p_snap_gen = &key;
    }

    const shard_pool_t* p_shard_pool = nullptr;
    const crush_t* p_crush = nullptr;
    std::optional<ns_oid_view_t> p_ns_oid;
    const snap_gen_t* p_snap_gen = nullptr;
  };

  enum class MatchKindBS : int8_t { NE = -1, EQ = 0 };
  struct search_result_bs_t {
    size_t position;
    MatchKindBS match;
  };
  template <typename FGetKey>
  search_result_bs_t binary_search(
      const onode_key_t& key, size_t begin, size_t end, FGetKey&& f_get_key) {
    assert(begin <= end);
    while (begin < end) {
      auto total = begin + end;
      auto mid = total >> 1;
      // do not copy if return value is reference
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

  template <typename PivotType, typename FGet>
  search_result_bs_t binary_search_r(
      size_t rend, size_t rbegin, FGet&& f_get, const PivotType& key) {
    assert(rend <= rbegin);
    while (rend < rbegin) {
      auto total = rend + rbegin + 1;
      auto mid = total >> 1;
      // do not copy if return value is reference
      decltype(f_get(mid)) target = f_get(mid);
      int match = target - key;
      if (match < 0) {
        rend = mid;
      } else if (match > 0) {
        rbegin = mid - 1;
      } else {
        return {mid, MatchKindBS::EQ};
      }
    }
    return {rbegin, MatchKindBS::NE};
  }

  template <typename FixedKeyType, field_type_t _FIELD_TYPE>
  struct _slot_t {
    using key_t = FixedKeyType;
    static constexpr field_type_t FIELD_TYPE = _FIELD_TYPE;

    key_t key;
    node_offset_t right_offset;
  } __attribute__((packed));
  using slot_0_t = _slot_t<shard_pool_crush_t, field_type_t::N0>;
  using slot_1_t = _slot_t<crush_t, field_type_t::N1>;
  using slot_3_t = _slot_t<snap_gen_t, field_type_t::N3>;

  using match_stage_t = uint8_t;
  constexpr match_stage_t STAGE_LEFT = 2u;   // shard/pool/crush
  constexpr match_stage_t STAGE_STRING = 1u; // nspace/oid
  constexpr match_stage_t STAGE_RIGHT = 0u;  // snap/gen
  constexpr auto STAGE_TOP = STAGE_LEFT;
  constexpr auto STAGE_BOTTOM = STAGE_RIGHT;

  struct MatchHistory {
    template <match_stage_t STAGE>
    const std::optional<MatchKindCMP>& get() const {
      static_assert(STAGE >= STAGE_BOTTOM && STAGE <= STAGE_TOP);
      if constexpr (STAGE == STAGE_RIGHT) {
        return right_match;
      } else if (STAGE == STAGE_STRING) {
        return string_match;
      } else {
        return left_match;
      }
    }

    const std::optional<MatchKindCMP>&
    get_by_stage(match_stage_t stage) const {
      assert(stage >= STAGE_BOTTOM && stage <= STAGE_TOP);
      if (stage == STAGE_RIGHT) {
        return right_match;
      } else if (stage == STAGE_STRING) {
        return string_match;
      } else {
        return left_match;
      }
    }

    template <match_stage_t STAGE = STAGE_TOP>
    const bool is_PO() const;

    template <match_stage_t STAGE>
    void set(MatchKindCMP match) {
      static_assert(STAGE >= STAGE_BOTTOM && STAGE <= STAGE_TOP);
      if constexpr (STAGE < STAGE_TOP) {
        assert(*get<STAGE + 1>() == MatchKindCMP::EQ);
      }
      assert(!get<STAGE>().has_value() || *get<STAGE>() != MatchKindCMP::EQ);
      const_cast<std::optional<MatchKindCMP>&>(get<STAGE>()) = match;
    }

    std::optional<MatchKindCMP> left_match;
    std::optional<MatchKindCMP> string_match;
    std::optional<MatchKindCMP> right_match;
  };

  template <match_stage_t STAGE>
  struct _check_PO_t {
    static bool eval(const MatchHistory* history) {
      return history->get<STAGE>() &&
             (*history->get<STAGE>() == MatchKindCMP::PO ||
              (*history->get<STAGE>() == MatchKindCMP::EQ &&
               _check_PO_t<STAGE - 1>::eval(history)));
    }
  };
  template <>
  struct _check_PO_t<STAGE_RIGHT> {
    static bool eval(const MatchHistory* history) {
      return history->get<STAGE_RIGHT>() &&
             *history->get<STAGE_RIGHT>() == MatchKindCMP::PO;
    }
  };
  template <match_stage_t STAGE>
  const bool MatchHistory::is_PO() const {
    static_assert(STAGE >= STAGE_BOTTOM && STAGE <= STAGE_TOP);
    if constexpr (STAGE < STAGE_TOP) {
      assert(get<STAGE + 1>() == MatchKindCMP::EQ);
    }
    return _check_PO_t<STAGE>::eval(this);
  }

  struct memory_range_t {
    const char* p_start;
    const char* p_end;
  };

  struct node_range_t {
    node_offset_t start;
    node_offset_t end;
  };

  template <typename FieldType>
  const char* fields_start(const FieldType& node) {
    return reinterpret_cast<const char*>(&node);
  }

  template <node_type_t NODE_TYPE, typename FieldType>
  node_range_t fields_free_range_before(
      const FieldType& node, bool is_level_tail, size_t position) {
    assert(position <= node.num_keys);
    node_offset_t offset_start = node.get_key_start_offset(position);
    node_offset_t offset_end =
      (position == 0 ? FieldType::SIZE
                     : node.get_item_start_offset(position - 1));
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (is_level_tail) {
        offset_end -= sizeof(laddr_t);
      }
    }
    assert(offset_start <= offset_end);
    assert(offset_end - offset_start < FieldType::SIZE);
    return {offset_start, offset_end};
  }

  // internal/leaf node N0, N1; leaf node N3
  template <typename SlotType>
  struct _node_fields_013_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(SlotType), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    using key_t = typename SlotType::key_t;
    using key_get_type = const key_t&;
    using my_type_t = _node_fields_013_t<SlotType>;
    static constexpr field_type_t FIELD_TYPE = SlotType::FIELD_TYPE;
    static constexpr node_offset_t SIZE = NODE_BLOCK_SIZE;
    static constexpr node_offset_t SIZE_USABLE = SIZE - offsetof(my_type_t, slots);

    key_get_type get_key(size_t index) const {
      assert(index < num_keys);
      return slots[index].key;
    }
    node_offset_t get_key_start_offset(size_t index) const {
      assert(index <= num_keys);
      auto offset = offsetof(my_type_t, slots) + sizeof(SlotType) * index;
      assert(offset < SIZE);
      return offset;
    }
    node_offset_t get_item_start_offset(size_t index) const {
      assert(index < num_keys);
      auto offset = slots[index].right_offset;
      assert(offset <= SIZE);
      return offset;
    }
    node_offset_t get_item_end_offset(size_t index) const {
      return index == 0 ? SIZE : get_item_start_offset(index - 1);
    }
    template <node_type_t NODE_TYPE>
    node_offset_t free_size_before(bool is_level_tail, size_t position) const {
      auto range = fields_free_range_before<NODE_TYPE>(*this, is_level_tail, position);
      return range.end - range.start;
    }
#ifndef NDEBUG
    template <node_type_t NODE_TYPE>
    void fill_unused(bool is_level_tail, LogicalCachedExtent& extent) const {
      auto range = fields_free_range_before<NODE_TYPE>(*this, is_level_tail, num_keys);
      for (auto i = range.start; i < range.end; ++i) {
        extent.copy_in(uint8_t(0xc5), i);
      }
    }

    template <node_type_t NODE_TYPE>
    void validate_unused(bool is_level_tail) const {
      auto range = fields_free_range_before<NODE_TYPE>(*this, is_level_tail, num_keys);
      for (auto i = fields_start(*this) + range.start;
           i < fields_start(*this) + range.end;
           ++i) {
        assert(*i == char(0xc5));
      }
    }
#endif

    static node_offset_t estimate_insert_one() { return sizeof(SlotType); }

    node_header_t header;
    num_keys_t num_keys = 0u;
    SlotType slots[];
  } __attribute__((packed));
  using node_fields_0_t = _node_fields_013_t<slot_0_t>;
  using node_fields_1_t = _node_fields_013_t<slot_1_t>;

  // internal/leaf node N2
  struct node_fields_2_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(node_off_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_keys_t = uint8_t;
    using key_t = ns_oid_view_t;
    using key_get_type = key_t;
    static constexpr field_type_t FIELD_TYPE = field_type_t::N2;
    static constexpr node_offset_t SIZE = NODE_BLOCK_SIZE;
    static constexpr node_offset_t SIZE_USABLE =
      SIZE - sizeof(node_header_t) - sizeof(num_keys_t);

    key_get_type get_key(size_t index) const {
      assert(index < num_keys);
      node_offset_t item_end_offset =
        (index == 0 ? SIZE : offsets[index - 1]);
      assert(item_end_offset <= SIZE);
      const char* p_start = fields_start(*this);
      return key_t(p_start + item_end_offset);
    }
    node_offset_t get_key_start_offset(size_t index) const {
      assert(index <= num_keys);
      auto offset = offsetof(node_fields_2_t, offsets) +
                    sizeof(node_offset_t) * num_keys;
      assert(offset <= SIZE);
      return offset;
    }
    node_offset_t get_item_start_offset(size_t index) const {
      assert(index < num_keys);
      auto offset = offsets[index];
      assert(offset <= SIZE);
      return offset;
    }
    node_offset_t get_item_end_offset(size_t index) const {
      return index == 0 ? SIZE : get_item_start_offset(index - 1);
    }
    template <node_type_t NODE_TYPE>
    node_offset_t free_size_before(bool is_level_tail, size_t position) const {
      auto range = fields_free_range_before<NODE_TYPE>(*this, is_level_tail, position);
      return range.end - range.start;
    }
#ifndef NDEBUG
    template <node_type_t NODE_TYPE>
    void fill_unused(bool is_level_tail, LogicalCachedExtent& extent) const {
      auto range = fields_free_range_before<NODE_TYPE>(*this, is_level_tail, num_keys);
      for (auto i = range.start; i < range.end; ++i) {
        extent.copy_in(uint8_t(0xc5), i);
      }
    }

    template <node_type_t NODE_TYPE>
    void validate_unused(bool is_level_tail) const {
      auto range = fields_free_range_before<NODE_TYPE>(*this, is_level_tail, num_keys);
      for (auto i = fields_start(*this) + range.start;
           i < fields_start(*this) + range.end;
           ++i) {
        assert(*i == char(0xc5));
      }
    }
#endif

    static node_offset_t estimate_insert_one() { return sizeof(node_offset_t); }

    node_header_t header;
    num_keys_t num_keys = 0u;
    node_offset_t offsets[];
  } __attribute__((packed));

  // TODO: decide by NODE_BLOCK_SIZE, sizeof(snap_gen_t), sizeof(laddr_t)
  static constexpr unsigned MAX_NUM_KEYS_I3 = 170u;
  template <unsigned MAX_NUM_KEYS>
  struct _internal_fields_3_t {
    using key_get_type = const snap_gen_t&;
    using my_type_t = _internal_fields_3_t<MAX_NUM_KEYS>;
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(snap_gen_t), sizeof(laddr_t)
    using num_keys_t = uint8_t;
    static constexpr field_type_t FIELD_TYPE = field_type_t::N3;
    static constexpr node_offset_t SIZE = sizeof(my_type_t);
    static constexpr node_offset_t SIZE_USABLE = SIZE - offsetof(my_type_t, keys);

    key_get_type get_key(size_t index) const {
      assert(index < num_keys);
      return keys[index];
    }
    template <node_type_t NODE_TYPE>
    std::enable_if_t<NODE_TYPE == node_type_t::INTERNAL, node_offset_t>
    free_size_before(bool is_level_tail, size_t position) const {
      assert(position <= num_keys);
      auto allowed_num_keys = is_level_tail ? MAX_NUM_KEYS - 1 : MAX_NUM_KEYS;
      assert(position <= allowed_num_keys);
      auto free = (allowed_num_keys - position) * (sizeof(snap_gen_t) + sizeof(laddr_t));
      assert(free < SIZE);
      return free;
    }
#ifndef NDEBUG
    template <node_type_t NODE_TYPE>
    void fill_unused(bool is_level_tail, LogicalCachedExtent& extent) const {
      node_offset_t begin = (const char*)&keys[num_keys] - fields_start(*this);
      node_offset_t end = (const char*)&child_addrs[0] - fields_start(*this);
      for (auto i = begin; i < end; ++i) {
        extent.copy_in(uint8_t(0xc5), i);
      }
      begin = (const char*)&child_addrs[num_keys] - fields_start(*this);
      end = NODE_BLOCK_SIZE;
      if (is_level_tail) {
        begin += sizeof(laddr_t);
      }
      for (auto i = begin; i < end; ++i) {
        extent.copy_in(uint8_t(0xc5), i);
      }
    }

    template <node_type_t NODE_TYPE>
    void validate_unused(bool is_level_tail) const {
      auto begin = (const char*)&keys[num_keys];
      auto end = (const char*)&child_addrs[0];
      for (auto i = begin; i < end; ++i) {
        assert(*i == uint8_t(0xc5));
      }
      begin = (const char*)&child_addrs[num_keys];
      end = fields_start(*this) + NODE_BLOCK_SIZE;
      if (is_level_tail) {
        begin += sizeof(laddr_t);
      }
      for (auto i = begin; i < end; ++i) {
        assert(*i == char(0xc5));
      }
    }
#endif

    static node_offset_t estimate_insert_one() {
      return sizeof(snap_gen_t) + sizeof(laddr_t);
    }

    node_header_t header;
    num_keys_t num_keys = 0u;
    snap_gen_t keys[MAX_NUM_KEYS];
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
   * see item_iterator_t<node_type_t::INTERNAL>
   *
   * for internal node type 2:
   * previous off (block boundary) ----------------------+
   * current off --+                                     |
   *               |                                     |
   *               V                                     V
   *        <==== |   sub |fix|sub |fix|oid char|ns char|
   *  (next-item) |...addr|key|addr|key|array & |array &|(prv-item)...
   *        <==== |   1   |1  |0   |0  |len     |len    |
   * see sub_items_t<node_type_t::INTERNAL>
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
   * see item_iterator_t<node_type_t::LEAF>
   *
   * for leaf node type 2:
   * previous off (block boundary) ---------------------------------+
   * current off --+                                                |
   *               |                                                |
   *               V                                                V
   *        <==== |   fix|o-  |fix|   off|off|num |oid char|ns char|
   *  (next-item) |...key|node|key|...set|set|sub |array & |array &|(prv-item)
   *        <==== |   1  |0   |0  |   1  |0  |keys|len     |len    |
   * see sub_items_t<node_type_t::LEAF>
   */

  enum class ContainerType { ITERATIVE, INDEXABLE };

  struct internal_sub_item_t {
    const snap_gen_t& get_key() const { return key; }
    #pragma GCC diagnostic ignored "-Waddress-of-packed-member"
    const laddr_t* get_p_value() const { return &value; }

    snap_gen_t key;
    laddr_t value;
  } __attribute__((packed));

  /*
   * internal node N0, N1, N2
   *
   * p_first_item +
   * (num_items)  |
   *              V
   * |   fix|sub |fix|sub |
   * |...key|addr|key|addr|
   * |   1  |1   |0  |0   |
   */
  class internal_sub_items_t {
   public:
    using num_keys_t = size_t;

    internal_sub_items_t(const memory_range_t& range) {
      assert(range.p_start < range.p_end);
      assert((range.p_end - range.p_start) % sizeof(internal_sub_item_t) == 0);
      num_items = (range.p_end - range.p_start) / sizeof(internal_sub_item_t);
      assert(num_items > 0);
    }

    // container type system
    using key_get_type = const snap_gen_t&;
    static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
    num_keys_t keys() const { return num_items; }
    key_get_type operator[](size_t index) const {
      assert(index < num_items);
      return (p_first_item - index)->get_key();
    }
    const laddr_t* get_p_value(size_t index) const {
      assert(index < num_items);
      return (p_first_item - index)->get_p_value();
    }

    static node_offset_t estimate_insert_one() {
      return sizeof(internal_sub_item_t);
    }

    static node_offset_t estimate_insert_new() {
      return estimate_insert_one();
    }

   private:
    size_t num_items;
    const internal_sub_item_t* p_first_item;
  };

  /*
   * leaf node N0, N1, N2
   *
   * p_num_keys -----------------+
   * p_offsets --------------+   |
   * p_items_end -----+      |   |
   *                  |      |   |
   *                  V      V   V
   * |   fix|o-  |fix|   off|off|num |
   * |...key|node|key|...set|set|sub |
   * |   1  |0   |0  |   1  |0  |keys|
   *         ^        |       |
   *         |        |       |
   *         +--------+ <=====+
   */
  class leaf_sub_items_t {
   public:
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(snap_gen_t),
    //       and the minimal size of onode_t
    using num_keys_t = uint8_t;

    leaf_sub_items_t(const memory_range_t& range) {
      assert(range.p_start < range.p_end);
      auto _p_num_keys = range.p_end - sizeof(num_keys_t);
      assert(range.p_start < _p_num_keys);
      p_num_keys = reinterpret_cast<const num_keys_t*>(_p_num_keys);
      assert(keys());
      auto _p_offsets = _p_num_keys - sizeof(node_offset_t);
      assert(range.p_start < _p_offsets);
      p_offsets = reinterpret_cast<const node_offset_t*>(_p_offsets);
      p_items_end = reinterpret_cast<const char*>(&get_offset(keys() - 1));
      assert(range.p_start < p_items_end);
      assert(range.p_start == get_item_start(keys() - 1));
    }

    const node_offset_t& get_offset(size_t index) const {
      assert(index < keys());
      return *(p_offsets - index);
    }

    const char* get_item_start(size_t index) const {
      assert(index < keys());
      return p_items_end - get_offset(index);
    }

    const char* get_item_end(size_t index) const {
      assert(index <= keys());
      return index == 0 ? p_items_end : get_item_start(index - 1);
    }

    size_t kv_size_before(size_t index) const {
      assert(index <= keys());
      if (index == 0) {
        return sizeof(num_keys_t);
      }
      --index;
      auto ret = sizeof(num_keys_t) +
                 (index + 1) * sizeof(node_offset_t) +
                 get_offset(index);
      return ret;
    }

    // container type system
    using key_get_type = const snap_gen_t&;
    static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
    const num_keys_t& keys() const { return *p_num_keys; }
    key_get_type operator[](size_t index) const {
      assert(index < keys());
      auto pointer = get_item_end(index);
      assert(get_item_start(index) < pointer);
      pointer -= sizeof(snap_gen_t);
      assert(get_item_start(index) < pointer);
      return *reinterpret_cast<const snap_gen_t*>(pointer);
    }
    const onode_t* get_p_value(size_t index) const {
      assert(index < keys());
      auto pointer = get_item_start(index);
      auto value = reinterpret_cast<const onode_t*>(pointer);
      assert(pointer + value->size + sizeof(snap_gen_t) == get_item_end(index));
      return value;
    }

    static node_offset_t estimate_insert_one(const onode_t& value) {
      return value.size + sizeof(snap_gen_t) + sizeof(node_offset_t);
    }

    static node_offset_t estimate_insert_new(const onode_t& value) {
      return estimate_insert_one(value) + sizeof(num_keys_t);
    }

    static const onode_t* do_insert(const onode_key_t& key,
                                    const onode_t& value,
                                    node_offset_t& offset,
                                    LogicalCachedExtent& extent) {
      offset -= sizeof(leaf_sub_items_t::num_keys_t);
      extent.copy_in(leaf_sub_items_t::num_keys_t(1), offset);
      offset -= sizeof(node_offset_t);
      extent.copy_in(node_offset_t(sizeof(snap_gen_t) + value.size), offset);
      offset -= sizeof(snap_gen_t);
      extent.copy_in(snap_gen_t::from_key(key), offset);
      offset -= value.size;
      auto ret = extent.copy_in(&value, offset, value.size);
      return (const onode_t*)ret;
    }

   private:
    // TODO: support unaligned access
    const num_keys_t* p_num_keys;
    const node_offset_t* p_offsets;
    const char* p_items_end;
  };

  template <node_type_t> struct _sub_items_t;
  template<> struct _sub_items_t<node_type_t::INTERNAL> { using type = internal_sub_items_t; };
  template<> struct _sub_items_t<node_type_t::LEAF> { using type = leaf_sub_items_t; };
  template <node_type_t NODE_TYPE>
  using sub_items_t = typename _sub_items_t<NODE_TYPE>::type;

  /*
   * internal/leaf node N0, N1
   *
   * (_position)
   * p_items_start
   *  |   item_range ------------+
   *  |   |     +----key---------+
   *  |   |     |                |
   *  V   V     V                V
   * |   |sub  |oid char|ns char|colli-|   |
   * |...|items|array & |array &|-sion |...|
   * |   |...  |len     |len    |offset|   |
   *      ^                      |
   *      |                      |
   *      +---- back_offset -----+
   */
  template <node_type_t NODE_TYPE>
  class item_iterator_t {
   public:
    item_iterator_t(const memory_range_t& range)
      : p_items_start(range.p_start) { next_item_range(range.p_end); }

    const memory_range_t& get_item_range() const { return item_range; }
    node_offset_t get_back_offset() const { return back_offset; }
    size_t size() const {
      return item_range.p_end - item_range.p_start + sizeof(node_offset_t);
    };
    size_t size_nxt() const {
      return get_key().size() + sizeof(node_offset_t);
    }

    // container type system
    using key_get_type = const ns_oid_view_t&;
    static constexpr auto CONTAINER_TYPE = ContainerType::ITERATIVE;
    size_t position() const { return _position; }
    key_get_type get_key() const {
      if (!key.has_value()) {
        key = ns_oid_view_t(item_range.p_end);
        assert(item_range.p_start < (*key).p_start());
      }
      return *key;
    }
    memory_range_t get_nxt_container() const {
      return {item_range.p_start, get_key().p_start()};
    }
    bool has_next() const {
      assert(p_items_start <= item_range.p_start);
      return p_items_start < item_range.p_start;
    }
    const item_iterator_t<NODE_TYPE>& operator++() const {
      assert(has_next());
      next_item_range(item_range.p_start);
      key.reset();
      ++_position;
      return *this;
    }

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::INTERNAL, node_offset_t>
    estimate_insert_one(const onode_key_t* p_key) {
      return (internal_sub_items_t::estimate_insert_new() +
              ns_oid_view_t::estimate_size(p_key) + sizeof(node_offset_t));
    }

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::INTERNAL, node_offset_t>
    estimate_insert_new(const onode_key_t* p_key) { return estimate_insert_one<NT>(p_key); }

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::LEAF, node_offset_t>
    estimate_insert_one(const onode_key_t* p_key, const onode_t& value) {
      return (leaf_sub_items_t::estimate_insert_new(value) +
              ns_oid_view_t::estimate_size(p_key) + sizeof(node_offset_t));
    }

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::LEAF, node_offset_t>
    estimate_insert_new(const onode_key_t* p_key, const onode_t& value) {
      return estimate_insert_one<NT>(p_key, value);
    }

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::LEAF, const onode_t*>
    do_insert(const node_range_t& range,
              const onode_key_t& key,
              const onode_t& value,
              const ns_oid_view_t::Type& dedup_type,
              LogicalCachedExtent& extent) {
      auto offset = range.end;
      offset -= sizeof(node_offset_t);
      node_offset_t offset_next = (offset - range.start);
      extent.copy_in(offset_next, offset);
      ns_oid_view_t::do_insert(key, dedup_type, offset, extent);
      auto p_value = leaf_sub_items_t::do_insert(key, value, offset, extent);
      assert(offset == range.start);
      return p_value;
    }

   private:
    void next_item_range(const char* p_end) const {
      auto p_item_end = p_end - sizeof(node_offset_t);
      assert(p_items_start < p_item_end);
      back_offset = *reinterpret_cast<const node_offset_t*>(p_item_end);
      assert(back_offset);
      const char* p_item_start = p_item_end - back_offset;
      assert(p_items_start <= p_item_start);
      item_range = {p_item_start, p_item_end};
    }

    const char* p_items_start;
    mutable memory_range_t item_range;
    mutable node_offset_t back_offset;
    mutable std::optional<ns_oid_view_t> key;
    mutable size_t _position = 0u;
  };

  template <node_type_t> struct value_type;
  template<> struct value_type<node_type_t::INTERNAL> { using type = laddr_t; };
  template<> struct value_type<node_type_t::LEAF> { using type = onode_t; };
  template <node_type_t NODE_TYPE>
  using value_type_t = typename value_type<NODE_TYPE>::type;

  template <match_stage_t STAGE>
  struct staged_position_t {
    static_assert(STAGE > STAGE_BOTTOM && STAGE <= STAGE_TOP);
    using my_type_t = staged_position_t<STAGE>;
    using nxt_type_t = staged_position_t<STAGE - 1>;
    bool is_end() const { return position == std::numeric_limits<size_t>::max(); }
    bool operator==(const my_type_t& x) const {
      return position == x.position && position_nxt == x.position_nxt;
    }
    bool operator!=(const my_type_t& x) const { return !(*this == x); }
    bool operator<(const my_type_t& x) const {
      return std::make_pair(position, position_nxt) <
             std::make_pair(x.position, x.position_nxt);
    }

    static my_type_t begin() { return {0u, nxt_type_t::begin()}; }
    static my_type_t end() {
      return {std::numeric_limits<size_t>::max(), nxt_type_t::end()};
    }

    size_t position;
    nxt_type_t position_nxt;
  };
  template <match_stage_t STAGE>
  std::ostream& operator<<(std::ostream& os, const staged_position_t<STAGE>& pos) {
    if (pos.position == std::numeric_limits<size_t>::max()) {
      os << "END";
    } else {
      os << pos.position;
    }
    return os << ", " << pos.position_nxt;
  }

  template <>
  struct staged_position_t<STAGE_BOTTOM> {
    using my_type_t = staged_position_t<STAGE_BOTTOM>;
    bool is_end() const { return position == std::numeric_limits<size_t>::max(); }
    bool operator==(const my_type_t& x) const { return position == x.position; }
    bool operator!=(const my_type_t& x) const { return !(*this == x); }
    bool operator<(const my_type_t& x) const { return position < x.position; }

    static my_type_t begin() { return {0u}; }
    static my_type_t end() { return {std::numeric_limits<size_t>::max()}; }

    size_t position;
  };
  template <>
  std::ostream& operator<<(std::ostream& os, const staged_position_t<STAGE_BOTTOM>& pos) {
    if (pos.position == std::numeric_limits<size_t>::max()) {
      return os << "END";
    } else {
      return os << pos.position;
    }
  }

  using search_position_t = staged_position_t<STAGE_TOP>;

  template <match_stage_t STAGE, typename = std::enable_if_t<STAGE == STAGE_TOP>>
  const search_position_t& cast_down(const search_position_t& pos) { return pos; }

  template <match_stage_t STAGE, typename = std::enable_if_t<STAGE != STAGE_TOP>>
  staged_position_t<STAGE> cast_down(const search_position_t& pos) {
    if constexpr (STAGE == STAGE_STRING) {
#ifndef NDEBUG
      if (pos.is_end()) {
        assert(pos.position_nxt.is_end());
      } else {
        assert(pos.position == 0u);
      }
#endif
      return pos.position_nxt;
    } else if (STAGE == STAGE_RIGHT) {
#ifndef NDEBUG
      if (pos.is_end()) {
        assert(pos.position_nxt.position_nxt.is_end());
      } else {
        assert(pos.position == 0u);
        assert(pos.position_nxt.position == 0u);
      }
#endif
      return pos.position_nxt.position_nxt;
    } else {
      assert(false);
    }
  }

  search_position_t&& normalize(search_position_t&& pos) { return std::move(pos); }

  template <match_stage_t STAGE, typename = std::enable_if_t<STAGE != STAGE_TOP>>
  search_position_t normalize(staged_position_t<STAGE>&& pos) {
    if (pos.is_end()) {
      return search_position_t::end();
    }
    if constexpr (STAGE == STAGE_STRING) {
      return {0u, std::move(pos)};
    } else if (STAGE == STAGE_RIGHT) {
      return {0u, {0u, std::move(pos)}};
    } else {
      assert(false);
    }
  }

  // TODO: should be generalized
  template <node_type_t NODE_TYPE>
  struct search_iterator_t {
    search_position_t position;
    match_stage_t stage;
    std::optional<item_iterator_t<NODE_TYPE>> container_1;
    std::optional<sub_items_t<NODE_TYPE>> container_0;
  };

  using match_stat_t = int8_t;
  constexpr match_stat_t MSTAT_PO = -2; // index is search_position_t::end()
  constexpr match_stat_t MSTAT_EQ = -1; // key == index
  constexpr match_stat_t MSTAT_NE0 = 0; // key == index [pool/shard crush ns/oid]; key < index [snap/gen]
  constexpr match_stat_t MSTAT_NE1 = 1; // key == index [pool/shard crush]; key < index [ns/oid]
  constexpr match_stat_t MSTAT_NE2 = 2; // key < index [pool/shard crush ns/oid] ||
                                        // key == index [pool/shard]; key < index [crush]
  constexpr match_stat_t MSTAT_NE3 = 3; // key < index [pool/shard]

  bool matchable(field_type_t type, match_stat_t mstat) {
    /*
     * compressed prefix by field type:
     * N0: NONE
     * N1: pool/shard
     * N2: pool/shard crush
     * N3: pool/shard crush ns/oid
     *
     * if key matches the node's compressed prefix, return true
     * else, return false
     */
    return mstat + to_unsigned(type) < 4;
  }

  void assert_mstat(const onode_key_t& key, const index_view_t& index, match_stat_t mstat) {
    // key < index ...
    switch (mstat) {
     case MSTAT_EQ:
      break;
     case MSTAT_NE0:
      assert(index.p_snap_gen);
      assert(compare_to(key, *index.p_snap_gen) == MatchKindCMP::NE);
      break;
     case MSTAT_NE1:
      assert(index.p_ns_oid);
      assert(compare_to(key, *index.p_ns_oid) == MatchKindCMP::NE);
      break;
     case MSTAT_NE2:
      assert(index.p_crush);
      if (index.p_shard_pool) {
        assert(compare_to(key, shard_pool_crush_t{*index.p_shard_pool,
                                                  *index.p_crush}) ==
               MatchKindCMP::NE);
      } else {
        assert(compare_to(key, *index.p_crush) == MatchKindCMP::NE);
      }
      break;
     default:
      assert(false);
    }
    // key == index ...
    switch (mstat) {
     case MSTAT_EQ:
      assert(index.p_snap_gen);
      assert(compare_to(key, *index.p_snap_gen) == MatchKindCMP::EQ);
     case MSTAT_NE0:
      if (!index.p_ns_oid)
        break;
      assert(index.p_ns_oid->type() == ns_oid_view_t::Type::MAX ||
             compare_to(key, *index.p_ns_oid) == MatchKindCMP::EQ);
     case MSTAT_NE1:
      if (!index.p_crush)
        break;
      assert(compare_to(key, *index.p_crush) == MatchKindCMP::EQ);
      if (!index.p_shard_pool)
        break;
      assert(compare_to(key, *index.p_shard_pool) == MatchKindCMP::EQ);
     default:
      break;
    }
  }

  template <node_type_t NODE_TYPE, match_stage_t STAGE>
  struct staged_result_t {
    using my_type_t = staged_result_t<NODE_TYPE, STAGE>;
    bool is_end() const { return position.is_end(); }

    static my_type_t end() {
      return {staged_position_t<STAGE>::end(), MatchKindBS::NE, nullptr, MSTAT_PO};
    }
    template <typename T = my_type_t>
    static std::enable_if_t<STAGE != STAGE_BOTTOM, T>
    from_nxt(size_t position, const staged_result_t<NODE_TYPE, STAGE - 1>& nxt_stage_result) {
      return {{position, nxt_stage_result.position},
              nxt_stage_result.match,
              nxt_stage_result.p_value,
              nxt_stage_result.mstat};
    }

    staged_position_t<STAGE> position;
    MatchKindBS match;
    const value_type_t<NODE_TYPE>* p_value;
    match_stat_t mstat;
  };

  template <node_type_t NODE_TYPE>
  staged_result_t<NODE_TYPE, STAGE_TOP>&& normalize(
      staged_result_t<NODE_TYPE, STAGE_TOP>&& result) { return std::move(result); }

  template <node_type_t NODE_TYPE, match_stage_t STAGE,
            typename = std::enable_if_t<STAGE != STAGE_TOP>>
  staged_result_t<NODE_TYPE, STAGE_TOP> normalize(
      staged_result_t<NODE_TYPE, STAGE>&& result) {
    return {normalize(std::move(result.position)), result.match, result.p_value};
  }

  class LeafNode;

  struct tree_cursor_t {
    // TODO: deref LeafNode if destroyed with leaf_node available
    // TODO: make sure to deref LeafNode if is_end()
    bool is_end() const { return position.is_end(); }
    bool operator==(const tree_cursor_t& x) const {
      return (leaf_node == x.leaf_node && position == x.position);
    }
    bool operator!=(const tree_cursor_t& x) const { return !(*this == x); }

    static tree_cursor_t from(
        Ref<LeafNode> leaf_node,
        const staged_result_t<node_type_t::LEAF, STAGE_TOP>& result) {
      return {leaf_node, result.position, result.p_value};
    }

    static tree_cursor_t make_end() {
      return {nullptr, search_position_t::end(), nullptr};
    }

    Ref<LeafNode> leaf_node;
    search_position_t position;
    const onode_t* p_value;
  };

  struct search_result_t {
    bool is_end() const { return cursor.is_end(); }

    static search_result_t from(
        Ref<LeafNode> leaf_node,
        const staged_result_t<node_type_t::LEAF, STAGE_TOP>& result) {
      return {tree_cursor_t::from(leaf_node, result), result.match, result.mstat};
    }

    tree_cursor_t cursor;
    MatchKindBS match;
    match_stat_t mstat;
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
    virtual size_t free_size() const = 0;
    virtual size_t total_size() const = 0;
    size_t filled_size() const { return total_size() - free_size(); }
    size_t extent_size() const { return extent->get_length(); }
    virtual search_result_t lower_bound(const onode_key_t&, MatchHistory&) = 0;
    virtual tree_cursor_t lookup_smallest() = 0;
    virtual tree_cursor_t lookup_largest() = 0;
    virtual index_view_t get_index_view(const search_position_t&) const = 0;
    std::pair<tree_cursor_t, bool> insert(const onode_key_t&, const onode_t&, MatchHistory&);

    laddr_t laddr() const { return extent->get_laddr(); }
    level_t level() const { return extent->get_ptr<node_header_t>(0u)->level; }

    virtual std::ostream& dump(std::ostream&) = 0;

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

    void set_level_tail(bool value) { _is_level_tail = value; }

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
       << ", filled=" << node.filled_size() << "B"
       << ", free=" << node.free_size() << "B"
       << ")";
    return os;
  }

  class LeafNode : virtual public Node {
   public:
    virtual ~LeafNode() = default;
    virtual tree_cursor_t insert_bottomup(
        const onode_key_t&,
        const onode_t&,
        const search_position_t&,
        const match_stat_t& mstat,
        const MatchHistory& histor) = 0;
  };

  template <typename FieldType, node_type_t _NODE_TYPE, typename ConcreteType>
  class NodeT : virtual public Node {
   public:
    using my_type_t = NodeT<FieldType, _NODE_TYPE, ConcreteType>;
    using num_keys_t = typename FieldType::num_keys_t;
    using value_t = value_type_t<_NODE_TYPE>;
    static constexpr node_type_t NODE_TYPE = _NODE_TYPE;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;
    static constexpr node_offset_t TOTAL_SIZE = FieldType::SIZE;
    static constexpr node_offset_t EXTENT_SIZE =
      (TOTAL_SIZE + BLOCK_SIZE - 1u) / BLOCK_SIZE * BLOCK_SIZE;

    virtual ~NodeT() = default;

    node_type_t node_type() const override final { return NODE_TYPE; }
    field_type_t field_type() const override final { return FIELD_TYPE; }
    size_t free_size() const override final {
      return fields().template free_size_before<NODE_TYPE>(is_level_tail(), keys());
    }
    size_t kv_size_before(size_t position) const {
      auto free_size = this->fields().template free_size_before<NODE_TYPE>(
          is_level_tail(), position);
      assert(FieldType::SIZE_USABLE >= free_size);
      return FieldType::SIZE_USABLE - free_size;
    }
    size_t total_size() const override final { return TOTAL_SIZE; }
    index_view_t get_index_view(const search_position_t&) const override final;

    const value_t* get_value_ptr(const search_position_t&);

    // container type system
    using key_get_type = typename FieldType::key_get_type;
    static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
    size_t keys() const { return fields().num_keys; }
    key_get_type operator[] (size_t position) const { return fields().get_key(position); }

    template <typename T = memory_range_t>
    std::enable_if_t<!std::is_same_v<FieldType, internal_fields_3_t>, T>
    get_nxt_container(size_t position) const {
      node_offset_t item_start_offset = fields().get_item_start_offset(position);
      node_offset_t item_end_offset = fields().get_item_end_offset(position);
      assert(item_start_offset < item_end_offset);
      auto item_p_start = fields_start(fields()) + item_start_offset;
      auto item_p_end = fields_start(fields()) + item_end_offset;
      if constexpr (FIELD_TYPE == field_type_t::N2) {
        // range for sub_items_t<NODE_TYPE>
        item_p_end = ns_oid_view_t(item_p_end).p_start();
        assert(item_p_start < item_p_end);
      } else {
        // range for item_iterator_t<NODE_TYPE>
      }
      return {item_p_start, item_p_end};
    }

    template <typename T = FieldType>
    std::enable_if_t<T::FIELD_TYPE == field_type_t::N3, const value_t*>
    get_p_value(size_t position) const {
      assert(position < fields().num_keys);
      if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
        return &fields().child_addrs[position];
      } else {
        auto range = get_nxt_container(position);
        auto ret = reinterpret_cast<const onode_t*>(range.p_start);
        assert(range.p_start + ret->size == range.p_end);
        return ret;
      }
    }

    std::ostream& dump(std::ostream&) override final;

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::INTERNAL,
                            std::tuple<node_offset_t, node_offset_t>>
    estimate_insert_one(const onode_key_t* p_key) {
      node_offset_t left_size = FieldType::estimate_insert_one();
      node_offset_t right_size;
      if constexpr (FIELD_TYPE == field_type_t::N0 ||
                    FIELD_TYPE == field_type_t::N1) {
        right_size = item_iterator_t<NODE_TYPE>::estimate_insert_new(p_key);
      } else if (FIELD_TYPE == field_type_t::N2) {
        right_size = internal_sub_items_t::estimate_insert_new() +
                     ns_oid_view_t::estimate_size(p_key);
      } else {
        right_size = 0u;
      }
      return {left_size, right_size};
    }

    template <node_type_t NT = NODE_TYPE>
    static std::enable_if_t<NT == node_type_t::LEAF,
                            std::tuple<node_offset_t, node_offset_t>>
    estimate_insert_one(const onode_key_t* p_key, const onode_t& value) {
      node_offset_t left_size = FieldType::estimate_insert_one();
      node_offset_t right_size;
      if constexpr (FIELD_TYPE == field_type_t::N0 ||
                    FIELD_TYPE == field_type_t::N1) {
        right_size = item_iterator_t<NODE_TYPE>::estimate_insert_new(p_key, value);
      } else if (FIELD_TYPE == field_type_t::N2) {
        right_size = leaf_sub_items_t::estimate_insert_new(value) +
                     ns_oid_view_t::estimate_size(p_key);
      } else {
        right_size = value.size;
      }
      return {left_size, right_size};
    }

#ifndef NDEBUG
    void validate_unused() const {
      fields().template validate_unused<NODE_TYPE>(is_level_tail());
    }

    Ref<Node> test_clone() const {
      auto ret = ConcreteType::_allocate(0u, is_level_tail());
      ret->extent->copy_in(extent->get_ptr<void>(0u), 0u, EXTENT_SIZE);
      return ret;
    }
#endif

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
#ifndef NDEBUG
      ret->fields().template fill_unused<NODE_TYPE>(is_level_tail, *extent);
#endif
      return ret;
    }
  };

  std::pair<tree_cursor_t, bool> Node::insert(
      const onode_key_t& key, const onode_t& value, MatchHistory& history) {
    auto result = lower_bound(key, history);
    if (result.match == MatchKindBS::EQ) {
      return {result.cursor, false};
    } else {
      auto cursor = result.cursor.leaf_node->insert_bottomup(
          key, value, result.cursor.position, result.mstat, history);
      return {cursor, true};
    }
  }

  template <typename FieldType, typename ConcreteType>
  class InternalNodeT : public NodeT<FieldType, node_type_t::INTERNAL, ConcreteType> {
   public:
    using my_type_t = InternalNodeT<FieldType, ConcreteType>;
    using value_t = laddr_t;
    static constexpr node_type_t NODE_TYPE = node_type_t::INTERNAL;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;

    virtual ~InternalNodeT() = default;

    search_result_t lower_bound(const onode_key_t&, MatchHistory&) override final;

    tree_cursor_t lookup_smallest() override final {
      auto position = search_position_t::begin();
      laddr_t child_addr = *this->get_value_ptr(position);
      auto child = get_or_load_child(child_addr, position);
      return child->lookup_smallest();
    }

    tree_cursor_t lookup_largest() override final {
      auto position = search_position_t::end();
      laddr_t child_addr = *this->get_value_ptr(position);
      auto child = get_or_load_child(child_addr, position);
      return child->lookup_largest();
    }

    static Ref<ConcreteType> allocate(level_t level, bool is_level_tail) {
      assert(level != 0u);
      return ConcreteType::_allocate(level, is_level_tail);
    }

   private:
    Ref<Node> get_or_load_child(
        laddr_t child_addr, const search_position_t& position) {
      Ref<Node> child;
      auto found = tracked_child_nodes.find(position);
      if (found != tracked_child_nodes.end()) {
        child = found->second;
        assert(child_addr == child->laddr());
        assert(position == child->parent_info().position);
        assert(this == child->parent_info().ptr);
#ifndef NDEBUG
        if (position.is_end()) {
          assert(child->is_level_tail());
        } else {
          assert(!child->is_level_tail());
        }
#endif
      } else {
        child = Node::load(child_addr,
                           position.is_end(),
                           {position, this});
        tracked_child_nodes.insert({position, child});
      }
      assert(this->level() - 1 == child->level());
      assert(this->field_type() <= child->field_type());
      assert(child->get_index_view(search_position_t::begin()).match_parent(
            this->get_index_view(position)));
      return child;
    }
    // TODO: intrusive
    std::map<search_position_t, Ref<Node>> tracked_child_nodes;
  };
  class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {};
  class InternalNode1 final : public InternalNodeT<node_fields_1_t, InternalNode1> {};
  class InternalNode2 final : public InternalNodeT<node_fields_2_t, InternalNode2> {};
  class InternalNode3 final : public InternalNodeT<internal_fields_3_t, InternalNode3> {};

  template <typename FieldType, typename ConcreteType>
  class LeafNodeT: public LeafNode, public NodeT<FieldType, node_type_t::LEAF, ConcreteType> {
   public:
    using my_type_t = LeafNodeT<FieldType, ConcreteType>;
    using value_t = onode_t;
    static constexpr node_type_t NODE_TYPE = node_type_t::LEAF;
    static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;

    virtual ~LeafNodeT() = default;

    search_result_t lower_bound(const onode_key_t&, MatchHistory&) override final;

    tree_cursor_t lookup_smallest() override final {
      if (unlikely(this->keys() == 0)) {
        // only happens when root is empty
        return tree_cursor_t::make_end();
      }
      auto position = search_position_t::begin();
      const onode_t* p_value = this->get_value_ptr(position);
      // mstat not assigned
      return {this, position, p_value};
    }

    tree_cursor_t lookup_largest() override final;

    tree_cursor_t insert_bottomup(const onode_key_t& key,
                                  const onode_t& value,
                                  const search_position_t& position,
                                  const match_stat_t& mstat,
                                  const MatchHistory& history) override final {
      const onode_t* p_value;
      search_position_t i_position;
      match_stage_t i_stage;
      ns_oid_view_t::Type i_dedup_type;
      node_offset_t i_estimated_size_left;
      node_offset_t i_estimated_size_right;
      try_insert_value(key, value, position, mstat, history,
          p_value, i_position, i_stage, i_dedup_type,
          i_estimated_size_left, i_estimated_size_right);
      if (p_value != nullptr) {
        return {this, i_position, p_value};
      }

      node_offset_t i_estimated_size = i_estimated_size_left + i_estimated_size_right;

      std::cout << "should split:"
                << "\n  insert at: " << i_position
                << ", i_stage=" << (int)i_stage << ", size=" << i_estimated_size
                << std::endl;

      search_iterator_t<NODE_TYPE> s_iterator;
      bool i_to_left = locate_split_position(
          i_position, i_stage, i_estimated_size, s_iterator);

      std::cout << "\n  split at: " << s_iterator.position << ", is_left=" << i_to_left
                << "\n  insert at: " << i_position
                << std::endl << std::endl;

      auto right_leaf_node = ConcreteType::allocate(this->is_level_tail());

      search_iterator_t<NODE_TYPE> d_iterator;
      d_iterator.position = search_position_t::begin();
      if (!i_to_left) {
        if (i_position != search_position_t::begin()) {
          // append split part 1
          // s_iter, i_pos, d_node, d_iter
        }
        // append insertion to right
        // i_pos, key, value, d_extent, d_iter
      }

      // append split part 2
      // s_iter, d_node, d_iter

      // trim left
      this->set_level_tail(false);

      if (i_to_left) {
        // insert to left
      }

      // propagate index to parent

      return {};
      // TODO (optimize)
      // try to acquire space from siblings ... see btrfs
      // try to insert value
    }

    void try_insert_value(const onode_key_t& key,
                          const onode_t& value,
                          const search_position_t& position,
                          const match_stat_t& mstat,
                          const MatchHistory& history,
                          const onode_t*& p_value,
                          search_position_t& i_position,
                          match_stage_t& i_stage,
                          ns_oid_view_t::Type& dedup_type,
                          node_offset_t& estimated_size_left,
                          node_offset_t& estimated_size_right) {
      // TODO: should be generalized
      if constexpr (FIELD_TYPE == field_type_t::N0) {
        // calculate the stage where insertion happens
        i_stage = STAGE_LEFT;
        bool is_PO = history.is_PO();
        if (position == search_position_t::begin() && is_PO) {
          assert(*history.get<STAGE_LEFT>() == MatchKindCMP::PO);
        } else {
          while (*history.get_by_stage(i_stage) == MatchKindCMP::EQ) {
            assert(i_stage != STAGE_RIGHT);
            --i_stage;
          }
        }
#ifndef NDEBUG
        if (position.is_end()) {
          assert(this->is_level_tail());
        }
#endif

        // calculate i_position
        i_position = position;
        if (is_PO) {
          if (i_position != search_position_t::begin() &&
              !i_position.is_end()) {
            switch (i_stage) {
             size_t* p_pos;
             case STAGE_RIGHT:
              p_pos = &i_position.position_nxt.position_nxt.position;
              assert(*p_pos != std::numeric_limits<size_t>::max());
              if (*p_pos > 0) {
                --*p_pos;
                break;
              }
              *p_pos = std::numeric_limits<size_t>::max();
              [[fallthrough]];
             case STAGE_STRING:
              p_pos = &i_position.position_nxt.position;
              assert(*p_pos != std::numeric_limits<size_t>::max());
              if (*p_pos > 0) {
                --*p_pos;
                break;
              }
              *p_pos = std::numeric_limits<size_t>::max();
              [[fallthrough]];
             case STAGE_LEFT:
              p_pos = &i_position.position;
              assert(*p_pos != std::numeric_limits<size_t>::max());
              assert(*p_pos > 0);
              --*p_pos;
            }
          }
        }

        // take ns/oid deduplication into consideration
        const onode_key_t* p_key = &key;
        dedup_type = ns_oid_view_t::Type::STR;
        auto& s_match = history.get<STAGE_STRING>();
        if (s_match.has_value() && *s_match == MatchKindCMP::EQ) {
          p_key = nullptr;
          if (is_PO) {
            dedup_type = ns_oid_view_t::Type::MIN;
          } else {
            dedup_type = ns_oid_view_t::Type::MAX;
          }
        }

        // estimated size for insertion
        estimated_size_left = 0;
        switch (i_stage) {
         case STAGE_LEFT:
          std::tie(estimated_size_left, estimated_size_right) = this->estimate_insert_one(p_key, value);
          break;
         case STAGE_STRING:
          estimated_size_right = item_iterator_t<NODE_TYPE>::estimate_insert_one(p_key, value);
          break;
         case STAGE_RIGHT:
          estimated_size_right = leaf_sub_items_t::estimate_insert_one(value);
          break;
        }

        auto free_size_before = this->free_size();
        if (free_size_before < (estimated_size_left + estimated_size_right)) {
          p_value = nullptr;
          return;
        }

        // modify block at STAGE_LEFT
        assert(i_position.position < this->keys() ||
               i_position.position == std::numeric_limits<size_t>::max());
        auto f_compensate_left_offsets = [this, estimated_size_right] (size_t from) {
          for (const auto* p_slot = &this->fields().slots[from];
               p_slot < &this->fields().slots[this->keys()]; ++p_slot) {
            this->extent->copy_in(
                node_offset_t(p_slot->right_offset - estimated_size_right),
                (const char*)&p_slot->right_offset - fields_start(this->fields()));
          }
        };
        size_t pos = i_position.position;
        if (i_stage == STAGE_LEFT) {
          node_offset_t insert_offset_left;
          node_offset_t insert_offset_right;
          if (pos == std::numeric_limits<size_t>::max()) {
            pos = i_position.position = this->keys();
            insert_offset_left = this->fields().get_key_start_offset(pos);
            insert_offset_right = this->fields().get_item_end_offset(pos);
          } else {
            insert_offset_left = this->fields().get_key_start_offset(pos);
            insert_offset_right = this->fields().get_item_end_offset(pos);

            // shift right part
            auto block_shift_start = this->fields().get_item_end_offset(this->keys());
            auto block_shift_end = insert_offset_right;
            this->extent->shift(block_shift_start,
                                block_shift_end - block_shift_start,
                                -(int)estimated_size_right);

            f_compensate_left_offsets(pos);

            // shift left part
            block_shift_start = insert_offset_left;
            block_shift_end = this->fields().get_key_start_offset(this->keys());
            this->extent->shift(block_shift_start,
                                block_shift_end - block_shift_start,
                                estimated_size_left);
          }
          i_position.position_nxt = search_position_t::nxt_type_t::begin();

          this->extent->copy_in(typename FieldType::num_keys_t(this->fields().num_keys + 1),
                                offsetof(FieldType, num_keys));
          this->extent->copy_in(shard_pool_crush_t::from_key(key),
                                insert_offset_left);
          node_offset_t offset_right_start = insert_offset_right - estimated_size_right;
          this->extent->copy_in(offset_right_start,
                                insert_offset_left + sizeof(shard_pool_crush_t));

          p_value = item_iterator_t<NODE_TYPE>::do_insert(
              node_range_t{node_offset_t(insert_offset_right - estimated_size_right), insert_offset_right},
              key, value, dedup_type, *this->extent);

          assert(this->free_size() == free_size_before - estimated_size_left - estimated_size_right);
#ifndef NDEBUG
          this->validate_unused();
#endif
          return;
        }
        if (pos == std::numeric_limits<size_t>::max()) {
          pos = i_position.position = this->keys() - 1;
        }
        auto left_index = pos;

        // modify block at STAGE_STRING
        auto range = this->get_nxt_container(pos);
        item_iterator_t<NODE_TYPE> iter(range);
        pos = i_position.position_nxt.position;
        if (pos == std::numeric_limits<size_t>::max()) {
          // staged::_iterator_t::last
          while (iter.has_next()) {
            ++iter;
          }
        } else {
          // staged::_iterator_t::at
          auto position = pos;
          while (position > 0) {
            assert(iter.has_next());
            ++iter;
            --position;
          }
        }
        if (i_stage == STAGE_STRING) {
          node_offset_t block_shift_start = this->fields().get_item_end_offset(this->keys());
          node_offset_t block_shift_end;
          auto& item_range = iter.get_item_range();
          if (pos == std::numeric_limits<size_t>::max()) {
            block_shift_end = item_range.p_start - fields_start(this->fields());
            pos = i_position.position_nxt.position = iter.position() + 1;
          } else {
            block_shift_end = item_range.p_end + sizeof(node_offset_t) - fields_start(this->fields());
          }
          i_position.position_nxt.position_nxt = search_position_t::nxt_type_t::nxt_type_t::begin();
          this->extent->shift(block_shift_start,
                              block_shift_end - block_shift_start,
                              -(int)estimated_size_right);
          p_value = item_iterator_t<NODE_TYPE>::do_insert(
              node_range_t{node_offset_t(block_shift_end - estimated_size_right), block_shift_end},
              key, value, dedup_type, *this->extent);
          f_compensate_left_offsets(left_index);
          assert(this->free_size() == free_size_before - estimated_size_left - estimated_size_right);
#ifndef NDEBUG
          this->validate_unused();
#endif
          return;
        }
        if (pos == std::numeric_limits<size_t>::max()) {
          pos = i_position.position_nxt.position = iter.position();
        }
        this->extent->copy_in(node_offset_t(iter.get_back_offset() + estimated_size_right),
                              iter.get_item_range().p_end - fields_start(this->fields()));

        // modify block at STAGE_RIGHT
        assert(i_stage == STAGE_RIGHT);
        leaf_sub_items_t sub_items = iter.get_nxt_container();
        if (i_position.position_nxt.position_nxt.position == std::numeric_limits<size_t>::max()) {
          i_position.position_nxt.position_nxt.position = sub_items.keys();
        }
        pos = i_position.position_nxt.position_nxt.position;
        // a. [... item(pos)] << estimated_size_right
        node_offset_t block_shift_start = this->fields().get_item_end_offset(this->keys());
        node_offset_t block_shift_end;
        block_shift_end = sub_items.get_item_end(pos) - fields_start(this->fields());
        this->extent->shift(block_shift_start,
                            block_shift_end - block_shift_start,
                            -(int)estimated_size_right);
        // b. insert item
        auto insert_offset = block_shift_end - estimated_size_right;
        p_value = (const onode_t*)this->extent->copy_in(&value, insert_offset, value.size);
        insert_offset += value.size;
        this->extent->copy_in(snap_gen_t::from_key(key), insert_offset);
        assert(insert_offset + sizeof(snap_gen_t) + sizeof(node_offset_t) == block_shift_end);
        // c. compensate affected offsets
        auto item_size = value.size + sizeof(snap_gen_t);
        for (auto i = pos; i < sub_items.keys(); ++i) {
          const node_offset_t& offset_i = sub_items.get_offset(i);
          this->extent->copy_in(node_offset_t(offset_i + item_size),
                                (const char*)&offset_i - fields_start(this->fields()));
        }
        // d. [item(pos-1) ... item(0) ... offset(pos)] <<< sizeof(node_offset_t)
        const char* p_offset = (pos == 0 ?
                                (const char*)&sub_items.get_offset(0) + sizeof(node_offset_t) :
                                (const char*)&sub_items.get_offset(pos - 1));
        block_shift_start = block_shift_end;
        block_shift_end = p_offset - fields_start(this->fields());
        this->extent->shift(block_shift_start,
                            block_shift_end - block_shift_start,
                            -(int)sizeof(node_offset_t));
        // e. insert offset
        node_offset_t offset_to_item_start = item_size +
          (pos == 0 ? 0u : sub_items.get_offset(pos - 1));
        this->extent->copy_in(offset_to_item_start, block_shift_end - sizeof(node_offset_t));
        // f. update num_sub_keys
        node_offset_t offset_num_keys = (const char*)&sub_items.keys() - fields_start(this->fields());
        this->extent->copy_in(leaf_sub_items_t::num_keys_t(sub_items.keys() + 1), offset_num_keys);
        // g. compensate left offsets
        f_compensate_left_offsets(left_index);
        assert(this->free_size() == free_size_before - estimated_size_left - estimated_size_right);
#ifndef NDEBUG
        this->validate_unused();
#endif
        return;
      } else {
        // not implemented
        assert(false);
        return;
      }
    }

    bool locate_split_position(
        search_position_t& i_position,
        match_stage_t i_stage,
        size_t i_estimated_size,
        search_iterator_t<NODE_TYPE>& s_iterator) {
      // TODO: should be generalized
      if constexpr (FIELD_TYPE == field_type_t::N0) {
        size_t target_size = (FieldType::SIZE_USABLE + i_estimated_size) / 2;
        // TODO adjust NODE_BLOCK_SIZE according to this requirement
        assert(i_estimated_size < FieldType::SIZE_USABLE / 2);

        size_t current_size;
        std::optional<bool> i_to_left;
        bool i_maybe_end_if_begin = false;

        // split position at STAGE_LEFT
        auto& i_pos_2 = i_position.position;
        auto& s_pos_2 = s_iterator.position.position;
        if (i_stage == STAGE_LEFT) {
          auto f_get_used_size_stage_2 = [this, i_pos_2, i_estimated_size]
              (size_t position) {
            size_t ret = 0;
            if (position > i_pos_2) {
              ret += i_estimated_size;
              --position;
            }
            ret += this->kv_size_before(position);
            return ret;
          };
          auto r_pos_2 = binary_search_r(
              0, this->keys(), f_get_used_size_stage_2, target_size).position;
          assert(r_pos_2 <= this->keys());
          current_size = f_get_used_size_stage_2(r_pos_2);
          assert(current_size <= target_size);
          if (r_pos_2 == this->keys() && r_pos_2 <= i_pos_2) {
            // ...[s_pos-1] |!| (i_pos)
            assert(i_pos_2 == std::numeric_limits<size_t>::max());
            assert(current_size == this->kv_size_before(this->keys()));
            r_pos_2 = std::numeric_limits<size_t>::max();
          }

          if (r_pos_2 <= i_pos_2) {
            i_to_left = false;
            s_pos_2 = r_pos_2;
            if (s_pos_2 == i_pos_2) {
              // ...[s_pos-1] |!| (i_pos) [s_pos]...
              // offset i_position to right
              i_pos_2 = 0;
              s_iterator.position.position_nxt = search_position_t::nxt_type_t::begin();
              s_iterator.stage = STAGE_LEFT;
              std::cout << "  [2] size_to_left=" << current_size
                        << ", target_split_size=" << target_size
                        << ", original_size=" << this->kv_size_before(this->keys())
                        << ", insert_size=" << i_estimated_size;
              return *i_to_left;
            } else {
              // ...[s_pos-1] |?[s_pos]| ...(i_pos)...
              // offset i_position to right
              if (i_pos_2 != std::numeric_limits<size_t>::max()) {
                i_pos_2 -= s_pos_2;
              }
            }
          } else {
            assert(r_pos_2 != std::numeric_limits<size_t>::max());
            i_to_left = true;
            s_pos_2 = r_pos_2 - 1;
            if (s_pos_2 == i_pos_2) {
              // ...[s_pos-1] (i_pos)) |?[s_pos]| ...
              i_maybe_end_if_begin = true;
            } else {
              // ...(i_pos)...[s_pos-1] |?[s_pos]| ...
            }
          }
        } else {
          auto f_get_used_size_stage_2 = [this, i_pos_2, i_estimated_size]
              (size_t position) {
            size_t ret = 0;
            if (position > i_pos_2) {
              ret += i_estimated_size;
            }
            ret += this->kv_size_before(position);
            return ret;
          };
          s_pos_2 = binary_search_r(
              0, this->keys() - 1, f_get_used_size_stage_2, target_size).position;
          assert(s_pos_2 < this->keys());
          current_size = f_get_used_size_stage_2(s_pos_2);
          assert(current_size <= target_size);

          if (s_pos_2 < i_pos_2) {
            // ...[s_pos-1] |?[s_pos]| ...[(i_pos)[s_pos_k]...
            i_to_left = false;

            // offset i_position to right
            if (i_pos_2 != std::numeric_limits<size_t>::max()) {
              i_pos_2 -= s_pos_2;
            }
          } else if (s_pos_2 > i_pos_2) {
            // ...[(i_pos)s_pos-1] |?[s_pos]| ...
            // ...[(i_pos)s_pos_k]...[s_pos-1] |?[s_pos]| ...
            i_to_left = true;
          } else {
            // ...[s_pos-1] |?[(i_pos)s_pos]| ...
            // i_to_left = std::nullopt;
          }
        }

        // split position at STAGE_STRING
        assert(current_size <= target_size);
        auto range_1 = this->get_nxt_container(s_pos_2);
        s_iterator.container_1 = item_iterator_t<NODE_TYPE>(range_1);
        auto& iter = *s_iterator.container_1;
        auto& i_pos_1 = i_position.position_nxt.position;
        auto& s_pos_1 = s_iterator.position.position_nxt.position;
        size_t extra_size = FieldType::estimate_insert_one();
        if (i_to_left.has_value()) {
          do {
            if (!iter.has_next()) {
              assert(current_size + iter.size() + extra_size ==
                       this->kv_size_before(s_pos_2 + 1));
              break;
            }
            size_t nxt_size = current_size + iter.size() + extra_size;
            if (nxt_size > target_size) {
              break;
            }
            current_size = nxt_size;
            extra_size = 0;
            ++iter;
          } while (true);
          s_pos_1 = iter.position();
          if (s_pos_1 != 0) {
            i_maybe_end_if_begin = false;
          }
        } else if (i_stage == STAGE_STRING) {
          size_t r_pos_1 = 0;
          do {
            size_t nxt_size = current_size + extra_size;
            if (i_pos_1 == r_pos_1) {
              nxt_size += i_estimated_size;
              if (nxt_size > target_size) {
                break;
              }
              current_size = nxt_size;
              extra_size = 0;
              ++r_pos_1;
            }
            nxt_size += iter.size();
            if (nxt_size > target_size) {
              break;
            }
            current_size = nxt_size;
            extra_size = 0;
            if (iter.has_next()) {
              ++iter;
              ++r_pos_1;
            } else {
              r_pos_1 = std::numeric_limits<size_t>::max();
              assert(i_pos_1 == std::numeric_limits<size_t>::max());
              assert(current_size == this->kv_size_before(s_pos_2 + 1));
              break;
            }
          } while (true);

          assert(i_pos_2 == s_pos_2);
          if (r_pos_1 <= i_pos_1) {
            i_to_left = false;
            s_pos_1 = r_pos_1;
            if (s_pos_1 == i_pos_1) {
              // ...[s_pos-1] |!| (i_pos) [s_pos]...
              // offset i_position to right
              i_position = search_position_t::begin();
              if (s_pos_1 == std::numeric_limits<size_t>::max()) {
                // ...[last] |!| (i_pos)
                // increment s_position
                s_pos_1 = 0;
                assert(s_pos_2 < this->keys());
                ++s_pos_2;
                if (s_pos_2 == this->keys()) {
                  s_pos_2 = std::numeric_limits<size_t>::max();
                }
              }
              s_iterator.position.position_nxt.position_nxt =
                search_position_t::nxt_type_t::nxt_type_t::begin();
              s_iterator.stage = STAGE_STRING;
              std::cout << "  [1] size_to_left=" << current_size
                        << ", target_split_size=" << target_size
                        << ", original_size=" << this->kv_size_before(this->keys())
                        << ", insert_size=" << i_estimated_size;
              return *i_to_left;
            } else {
              // ...[s_pos-1] |?[s_pos]| ...(i_pos)...
              // offset i_position to right
              i_pos_2 = 0;
              if (i_pos_1 != std::numeric_limits<size_t>::max()) {
                i_pos_1 -= s_pos_1;
              }
            }
          } else {
            assert(r_pos_1 != std::numeric_limits<size_t>::max());
            i_to_left = true;
            s_pos_1 = r_pos_1 - 1;
            if (s_pos_1 == i_pos_1) {
              // ...[s_pos-1] (i_pos)) |?[s_pos]| ...
              assert(!i_maybe_end_if_begin);
              i_maybe_end_if_begin = true;
            } else {
              // ...(i_pos)...[s_pos-1] |?[s_pos]| ...
            }
          }
        } else {
          assert(i_stage == STAGE_RIGHT);
          do {
            if (!iter.has_next()) {
              if (i_pos_1 == std::numeric_limits<size_t>::max()) {
                // i_pos_1 points to the end,
                // but it really wants to point to the last one.
                i_pos_1 = iter.position();
              }
              assert(current_size + iter.size() + extra_size ==
                     this->kv_size_before(s_pos_2 + 1));
              break;
            }
            size_t nxt_size = current_size + extra_size;
            if (i_pos_1 == iter.position()) {
              nxt_size += i_estimated_size;
            }
            nxt_size += iter.size();
            if (nxt_size > target_size) {
              break;
            }
            current_size = nxt_size;
            extra_size = 0;
            ++iter;
          } while (true);
          s_pos_1 = iter.position();

          if (s_pos_1 < i_pos_1) {
            // ...[s_pos-1] |?[s_pos]| ...[(i_pos)[s_pos_k]...
            i_to_left = false;

            // offset i_position to right
            if (i_pos_1 != std::numeric_limits<size_t>::max()) {
              i_pos_1 -= s_pos_1;
            }
            assert(i_pos_2 == s_pos_2);
            i_pos_2 = 0;
          } else if (s_pos_1 > i_pos_1) {
            // ...[(i_pos)s_pos-1] |?[s_pos]| ...
            // ...[(i_pos)s_pos_k]...[s_pos-1] |?[s_pos]| ...
            i_to_left = true;
          } else {
            // ...[s_pos-1] |?[(i_pos)s_pos]| ...
            // i_to_left = std::nullopt;
          }
        }

        // identify split at STAGE_RIGHT
        assert(current_size <= target_size);
        auto range_0 = iter.get_nxt_container();
        s_iterator.container_0 = leaf_sub_items_t(range_0);
        auto& sub_items = *s_iterator.container_0;
        auto& i_pos_0 = i_position.position_nxt.position_nxt.position;
        auto& s_pos_0 = s_iterator.position.position_nxt.position_nxt.position;
        size_t r_pos_0;
        if (!i_to_left.has_value()) {
          assert(i_stage == STAGE_RIGHT);
          auto current_size_1 = current_size + iter.size_nxt() + extra_size;
          auto f_get_used_size_stage_0 =
              [&sub_items, current_size, current_size_1,
               i_pos_0, i_estimated_size] (size_t position) {
            if (position == 0) {
              return current_size;
            }
            size_t ret = current_size_1;
            if (position > i_pos_0) {
              ret += i_estimated_size;
              --position;
            }
            ret += sub_items.kv_size_before(position);
            return ret;
          };
          r_pos_0 = binary_search_r(
              0, sub_items.keys(), f_get_used_size_stage_0, target_size).position;
          assert(r_pos_0 <= sub_items.keys());
#ifndef NDEBUG
          auto pre_current_size = current_size;
          current_size = f_get_used_size_stage_0(r_pos_0);
          assert(current_size <= target_size);
#endif
          if (r_pos_0 == sub_items.keys() && r_pos_0 < i_pos_0) {
            // ...[s_pos-1] |!| (i_pos)
            assert(i_pos_0 == std::numeric_limits<size_t>::max());
            assert(current_size == pre_current_size + iter.size());
            r_pos_0 = std::numeric_limits<size_t>::max();
          }

          assert(i_pos_2 == s_pos_2);
          assert(i_pos_1 == s_pos_1);
          if (r_pos_0 <= i_pos_0) {
            i_to_left = false;
            s_pos_0 = r_pos_0;
            if (s_pos_0 == i_pos_0) {
              // ...[s_pos-1] |!| (i_pos) [s_pos]...
              // offset i_position to right
              i_position = search_position_t::begin();
              if (s_pos_0 == std::numeric_limits<size_t>::max()) {
                // ...[last] |!| (i_pos)
                // increment s_position
                s_pos_0 = 0;
                if (iter.has_next()) {
                  ++s_pos_1;
                } else {
                  s_pos_1 = 0;
                  assert(s_pos_2 < this->keys());
                  ++s_pos_2;
                  if (s_pos_2 == this->keys()) {
                    s_pos_2 = std::numeric_limits<size_t>::max();
                  }
                }
              }
            } else {
              // ...[s_pos-1] |!| [s_pos]...(i_pos)...
              // offset i_position to right
              i_pos_2 = 0;
              i_pos_1 = 0;
              if (i_pos_0 != std::numeric_limits<size_t>::max()) {
                i_pos_0 -= s_pos_0;
              }
            }
          } else {
            assert(r_pos_0 != std::numeric_limits<size_t>::max());
            i_to_left = true;
            s_pos_0 = r_pos_0 - 1;
            if (s_pos_0 == i_pos_0) {
              // ...[s_pos-1] (i_pos)) |!| [s_pos]...
              i_position = search_position_t::end();
            } else {
              // ...(i_pos)...[s_pos-1] |!| [s_pos]...
            }
          }
        } else {
          auto current_size_1 = current_size + iter.size_nxt() + extra_size;
          auto f_get_used_size_stage_0 = [&sub_items, current_size, current_size_1] (size_t position) {
            if (position == 0) {
              return current_size;
            } else {
              return current_size_1 + sub_items.kv_size_before(position);
            }
          };
          r_pos_0 = binary_search_r(
              0, sub_items.keys() - 1, f_get_used_size_stage_0, target_size).position;
          assert(r_pos_0 < sub_items.keys());
#ifndef NDEBUG
          current_size = f_get_used_size_stage_0(r_pos_0);
          assert(current_size <= target_size);
#endif
          s_pos_0 = r_pos_0;
          if (s_pos_0 == 0) {
            if (i_maybe_end_if_begin) {
              assert(*i_to_left == true);
              i_position = search_position_t::end();
            }
          }
        }

        s_iterator.stage = STAGE_RIGHT;
        std::cout << "  [0] size_to_left=" << current_size
                  << ", target_split_size=" << target_size
                  << ", original_size=" << this->kv_size_before(this->keys())
                  << ", insert_size=" << i_estimated_size;
        return *i_to_left;
      } else {
        // not implemented
        assert(false);
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
   * staged infrastructure
   */

  template <node_type_t _NODE_TYPE>
  struct staged_params_subitems {
    using container_t = sub_items_t<_NODE_TYPE>;
    static constexpr auto NODE_TYPE = _NODE_TYPE;
    static constexpr auto STAGE = STAGE_RIGHT;

    // dummy type in order to make our type system work
    // any better solution to get rid of this?
    using next_param_t = staged_params_subitems<NODE_TYPE>;
  };

  template <node_type_t _NODE_TYPE>
  struct staged_params_item_iterator {
    using container_t = item_iterator_t<_NODE_TYPE>;
    static constexpr auto NODE_TYPE = _NODE_TYPE;
    static constexpr auto STAGE = STAGE_STRING;

    using next_param_t = staged_params_subitems<NODE_TYPE>;
  };

  template <typename NodeType>
  struct staged_params_node_01 {
    using container_t = NodeType;
    static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
    static constexpr auto STAGE = STAGE_LEFT;

    using next_param_t = staged_params_item_iterator<NODE_TYPE>;
  };

  template <typename NodeType>
  struct staged_params_node_2 {
    using container_t = NodeType;
    static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
    static constexpr auto STAGE = STAGE_STRING;

    using next_param_t = staged_params_subitems<NODE_TYPE>;
  };

  template <typename NodeType>
  struct staged_params_node_3 {
    using container_t = NodeType;
    static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
    static constexpr auto STAGE = STAGE_RIGHT;

    // dummy type in order to make our type system work
    // any better solution to get rid of this?
    using next_param_t = staged_params_node_3<NodeType>;
  };

  template <typename Params>
  struct staged {
    static_assert(Params::STAGE >= STAGE_BOTTOM);
    static_assert(Params::STAGE <= STAGE_TOP);
    using container_t = typename Params::container_t;
    using key_get_type = typename container_t::key_get_type;
    using position_t = staged_position_t<Params::STAGE>;
    using result_t = staged_result_t<Params::NODE_TYPE, Params::STAGE>;
    using value_t = value_type_t<Params::NODE_TYPE>;
    static constexpr auto CONTAINER_TYPE = container_t::CONTAINER_TYPE;
    static constexpr bool IS_BOTTOM = (Params::STAGE == STAGE_BOTTOM);
    static constexpr auto NODE_TYPE = Params::NODE_TYPE;
    static constexpr auto STAGE = Params::STAGE;

    /*
     * iterator_t encapsulates both indexable and iterative implementations
     * from a *non-empty* container.
     * access:
     *   position() -> size_t
     *   get_key() -> key_get_type (const reference or value type)
     *   is_last() -> bool
     *   is_end() -> bool
     * IF IS_BOTTOM
     *   get_p_value() -> const value_t*
     * ELSE
     *   get_nxt_container() -> nxt_stage::container_t
     * modifiers:
     *   operator++() -> iterator_t&
     *   seek(key, exclude_last) -> MatchKindBS
     * factory:
     *   static begin(container) -> iterator_t
     *   static last(container) -> iterator_t
     *   static at(container, position) -> iterator_t
     *
     */
    template <ContainerType CTYPE, typename Enable = void> class _iterator_t;
    template <ContainerType CTYPE>
    class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::INDEXABLE>> {
     /*
      * indexable container type system:
      *   CONTAINER_TYPE = ContainerType::INDEXABLE
      *   keys() const -> size_t
      *   operator[](size_t) const -> key_get_type
      * IF IS_BOTTOM:
      *   get_p_value(size_t) const -> const value_t*
      * ELSE
      *   get_nxt_container(size_t) const
      */
     public:
      using my_type_t = _iterator_t<CTYPE>;

      size_t position() const {
        assert(!is_end());
        return _position;
      }
      key_get_type get_key() const {
        assert(!is_end());
        return container[_position];
      }
      template <typename T = typename staged<typename Params::next_param_t>::container_t>
      std::enable_if_t<!IS_BOTTOM, T> get_nxt_container() const {
        assert(!is_end());
        return container.get_nxt_container(_position);
      }
      template <typename T = value_t>
      std::enable_if_t<IS_BOTTOM, const T*> get_p_value() const {
        assert(!is_end());
        return container.get_p_value(_position);
      }
      bool is_last() const {
        assert(container.keys());
        return _position + 1 == container.keys();
      }
      bool is_end() const { return _position == container.keys(); }
      my_type_t& operator++() {
        assert(!is_end());
        assert(!is_last());
        ++_position;
        return *this;
      }
      MatchKindBS seek(const onode_key_t& key, bool exclude_last) {
        assert(!is_end());
        size_t end_position = container.keys();
        if (exclude_last) {
          assert(end_position);
          --end_position;
          assert(compare_to(key, container[end_position]) == MatchKindCMP::NE);
        }
        auto ret = binary_search(key, _position, end_position,
            [this] (size_t index) { return container[index]; });
        _position = ret.position;
        return ret.match;
      }

      static my_type_t begin(const container_t& container) {
        assert(container.keys() != 0);
        return my_type_t(container, 0u);
      }
      static my_type_t last(const container_t& container) {
        assert(container.keys() != 0);
        return my_type_t(container, container.keys() - 1);
      }
      static my_type_t at(const container_t& container, size_t position) {
        assert(position < container.keys());
        return my_type_t(container, position);
      }

     private:
      _iterator_t(const container_t& container, size_t position)
        : container{container}, _position{position} {}

      const container_t& container;
      size_t _position;
    };

    template <ContainerType CTYPE>
    class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::ITERATIVE>> {
      /*
       * iterative container type system (!IS_BOTTOM):
       *   CONTAINER_TYPE = ContainerType::ITERATIVE
       *   position() const -> size_t
       *   get_key() const -> key_get_type
       *   get_nxt_container() const
       *   has_next() const -> bool
       *   operator++()
       */
      // currently the iterative iterator is only implemented with STAGE_STRING
      // for in-node space efficiency
      static_assert(STAGE == STAGE_STRING);
     public:
      using my_type_t = _iterator_t<CTYPE>;

      size_t position() const {
        assert(!is_end());
        return p_container->position();
      }
      key_get_type get_key() const {
        assert(!is_end());
        return p_container->get_key();
      }
      const typename staged<typename Params::next_param_t>::container_t
      get_nxt_container() const {
        assert(!is_end());
        return p_container->get_nxt_container();
      }
      bool is_last() const {
        assert(!is_end());
        return !p_container->has_next();
      }
      bool is_end() const { return p_container == nullptr; }
      my_type_t& operator++() {
        assert(!is_end());
        assert(!is_last());
        ++(*p_container);
        return *this;
      }
      MatchKindBS seek(const onode_key_t& key, bool exclude_last) {
        assert(!is_end());
        do {
          if (exclude_last && is_last()) {
            assert(compare_to(key, get_key()) == MatchKindCMP::NE);
            return MatchKindBS::NE;
          }
          auto match = compare_to(key, get_key());
          if (match == MatchKindCMP::NE) {
            return MatchKindBS::NE;
          } else if (match == MatchKindCMP::EQ) {
            return MatchKindBS::EQ;
          } else {
            if (p_container->has_next()) {
              ++(*p_container);
            } else {
              // end
              break;
            }
          }
        } while (true);
        assert(!exclude_last);
        p_container = nullptr;
        return MatchKindBS::NE;
      }

      static my_type_t begin(const container_t& container) {
        assert(container.position() == 0u);
        return my_type_t(container);
      }
      static my_type_t last(const container_t& container) {
        while (container.has_next()) {
          ++container;
        }
        return my_type_t(container);
      }
      static my_type_t at(const container_t& container, size_t position) {
        while (position > 0) {
          assert(container.has_next());
          ++container;
          --position;
        }
        return my_type_t(container);
      }

     private:
      _iterator_t(const container_t& container) : p_container{&container} {}

      const container_t* p_container;
    };

    using iterator_t = _iterator_t<CONTAINER_TYPE>;

    static result_t
    smallest_result(const iterator_t& iter) {
      static_assert(!IS_BOTTOM);
      assert(!iter.is_end());
      auto pos_smallest = staged<typename Params::next_param_t>::position_t::begin();
      auto nxt_container = iter.get_nxt_container();
      auto value_ptr = staged<typename Params::next_param_t>::get_p_value(
          nxt_container, pos_smallest);
      return {{iter.position(), pos_smallest}, MatchKindBS::NE, value_ptr, STAGE};
    }

    static result_t
    nxt_lower_bound(const onode_key_t& key, iterator_t& iter, MatchHistory& history) {
      static_assert(!IS_BOTTOM);
      assert(!iter.is_end());
      auto nxt_container = iter.get_nxt_container();
      auto nxt_result = staged<typename Params::next_param_t>::lower_bound(
          nxt_container, key, history);
      if (nxt_result.is_end()) {
        if (iter.is_last()) {
          return result_t::end();
        } else {
          return smallest_result(++iter);
        }
      } else {
        return result_t::from_nxt(iter.position(), nxt_result);
      }
    }

    static void
    lookup_largest(const container_t& container, position_t& position, const onode_t*& p_value) {
      auto iter = iterator_t::last(container);
      position.position = iter.position();
      if constexpr (IS_BOTTOM) {
        p_value = iter.get_p_value();
      } else {
        auto nxt_container = iter.get_nxt_container();
        staged<typename Params::next_param_t>::lookup_largest(
            nxt_container, position.position_nxt, p_value);
      }
    }

    static void
    lookup_largest_normalized(
        const container_t& container, search_position_t& position, const onode_t*& p_value) {
      if constexpr (STAGE == STAGE_LEFT) {
        lookup_largest(container, position, p_value);
        return;
      }
      position.position = 0;
      auto& nxt = position.position_nxt;
      if constexpr (STAGE == STAGE_STRING) {
        lookup_largest(container, nxt, p_value);
        return;
      }
      nxt.position = 0;
      auto& nxt_nxt = nxt.position_nxt;
      if constexpr (STAGE == STAGE_RIGHT) {
        lookup_largest(container, nxt_nxt, p_value);
        return;
      }
      assert(false);
    }

    static const value_t* get_p_value(const container_t& container, const position_t& position) {
      auto iter = iterator_t::at(container, position.position);
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        return staged<typename Params::next_param_t>::get_p_value(
            nxt_container, position.position_nxt);
      } else {
        return iter.get_p_value();
      }
    }

    static const value_t* get_p_value_normalized(
        const container_t& container, const search_position_t& position) {
      return get_p_value(container, cast_down<STAGE>(position));
    }

    static void get_index_view(
        const container_t& container, const position_t& position, index_view_t& output) {
      auto iter = iterator_t::at(container, position.position);
      output.set(iter.get_key());
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        return staged<typename Params::next_param_t>::get_index_view(
            nxt_container, position.position_nxt, output);
      }
    }

    static void get_index_view_normalized(
        const container_t& container, const search_position_t& position, index_view_t& output) {
      return get_index_view(container, cast_down<STAGE>(position), output);
    }

    static result_t
    lower_bound(const container_t& container, const onode_key_t& key, MatchHistory& history) {
      bool exclude_last = false;
      if (history.get<STAGE>().has_value()) {
        if (*history.get<STAGE>() == MatchKindCMP::EQ) {
          // lookup is short-circuited
          if constexpr (!IS_BOTTOM) {
            assert(history.get<STAGE - 1>().has_value());
            if (history.is_PO<STAGE - 1>()) {
              auto iter = iterator_t::begin(container);
              bool test_key_equal;
              if constexpr (STAGE == STAGE_STRING) {
                test_key_equal = (iter.get_key().type() == ns_oid_view_t::Type::MIN);
              } else {
                auto cmp = compare_to(key, iter.get_key());
                assert(cmp != MatchKindCMP::PO);
                test_key_equal = (cmp == MatchKindCMP::EQ);
              }
              if (test_key_equal) {
                return nxt_lower_bound(key, iter, history);
              } else {
                return smallest_result(iter);
              }
            }
          }
          // IS_BOTTOM || !history.is_PO<STAGE - 1>()
          auto iter = iterator_t::last(container);
          if constexpr (STAGE == STAGE_STRING) {
            assert(iter.get_key().type() == ns_oid_view_t::Type::MAX);
          } else {
            assert(compare_to(key, iter.get_key()) == MatchKindCMP::EQ);
          }
          if constexpr (IS_BOTTOM) {
            auto value_ptr = iter.get_p_value();
            return {{iter.position()}, MatchKindBS::EQ, value_ptr, MSTAT_EQ};
          } else {
            auto nxt_container = iter.get_nxt_container();
            auto nxt_result = staged<typename Params::next_param_t>::lower_bound(
                nxt_container, key, history);
            assert(!nxt_result.is_end());
            return result_t::from_nxt(iter.position(), nxt_result);
          }
        } else if (*history.get<STAGE>() == MatchKindCMP::NE) {
          exclude_last = true;
        }
      }
      auto iter = iterator_t::begin(container);
      auto bs_match = iter.seek(key, exclude_last);
      if (iter.is_end()) {
        assert(!exclude_last);
        assert(bs_match == MatchKindBS::NE);
        history.set<STAGE>(MatchKindCMP::PO);
        return result_t::end();
      }
      history.set<STAGE>(bs_match == MatchKindBS::EQ ?
                         MatchKindCMP::EQ : MatchKindCMP::NE);
      if constexpr (IS_BOTTOM) {
        auto value_ptr = iter.get_p_value();
        return {{iter.position()}, bs_match, value_ptr,
                (bs_match == MatchKindBS::EQ ? MSTAT_EQ : MSTAT_NE0)};
      } else {
        if (bs_match == MatchKindBS::EQ) {
          return nxt_lower_bound(key, iter, history);
        } else {
          return smallest_result(iter);
        }
      }
    }

    static staged_result_t<NODE_TYPE, STAGE_TOP> lower_bound_normalized(
        const container_t& container, const onode_key_t& key, MatchHistory& history) {
      auto&& result = lower_bound(container, key, history);
#ifndef NDEBUG
      if (result.is_end()) {
        assert(result.mstat == MSTAT_PO);
      } else {
        index_view_t index;
        get_index_view(container, result.position, index);
        assert_mstat(key, index, result.mstat);
      }
#endif
      if constexpr (container_t::FIELD_TYPE == field_type_t::N0) {
        // currently only internal node checks mstat
        if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
          if (result.mstat == MSTAT_NE2) {
            auto cmp = compare_to(key, container[result.position.position].shard_pool);
            assert(cmp != MatchKindCMP::PO);
            if (cmp != MatchKindCMP::EQ) {
              result.mstat = MSTAT_NE3;
            }
          }
        }
      }
      return normalize(std::move(result));
    }

    static std::ostream& dump(const container_t& container,
                              std::ostream& os,
                              const std::string& prefix) {
      auto iter = iterator_t::begin(container);
      assert(!iter.is_end());
      std::string prefix_blank(prefix.size(), ' ');
      const std::string* p_prefix = &prefix;
      do {
        std::ostringstream sos;
        sos << *p_prefix << iter.get_key() << ": ";
        std::string i_prefix = sos.str();
        if constexpr (!IS_BOTTOM) {
          auto nxt_container = iter.get_nxt_container();
          staged<typename Params::next_param_t>::dump(nxt_container, os, i_prefix);
        } else {
          auto value_ptr = iter.get_p_value();
          os << "\n" << i_prefix << *value_ptr;
        }
        if (iter.is_last()) {
          break;
        } else {
          ++iter;
          p_prefix = &prefix_blank;
        }
      } while (true);
      return os;
    }
  };

  template <typename NodeType, typename Enable = void> struct _node_to_stage_t;
  template <typename NodeType>
  struct _node_to_stage_t<NodeType,
      std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N0 ||
                       NodeType::FIELD_TYPE == field_type_t::N1>> {
    using type = staged<staged_params_node_01<NodeType>>;
  };
  template <typename NodeType>
  struct _node_to_stage_t<NodeType,
      std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N2>> {
    using type = staged<staged_params_node_2<NodeType>>;
  };
  template <typename NodeType>
  struct _node_to_stage_t<NodeType,
      std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N3>> {
    using type = staged<staged_params_node_3<NodeType>>;
  };
  template <typename NodeType>
  using node_to_stage_t = typename _node_to_stage_t<NodeType>::type;

  template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
  index_view_t NodeT<FieldType, NODE_TYPE, ConcreteType>::get_index_view(
      const search_position_t& position) const {
    index_view_t ret;
    node_to_stage_t<my_type_t>::get_index_view_normalized(*this, position, ret);
    return ret;
  }

  /*
   * Node implementations depending on stage
   */

  template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
  const value_type_t<NODE_TYPE>*
  NodeT<FieldType, NODE_TYPE, ConcreteType>::get_value_ptr(
      const search_position_t& position) {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (position.is_end()) {
        assert(is_level_tail());
        if constexpr (FIELD_TYPE == field_type_t::N3) {
          return &fields().child_addrs[keys()];
        } else {
          auto offset_start = fields().get_item_end_offset(keys());
          assert(offset_start <= FieldType::SIZE);
          offset_start -= sizeof(laddr_t);
          auto p_addr = fields_start(fields()) + offset_start;
          return reinterpret_cast<const laddr_t*>(p_addr);
        }
      }
    } else {
      assert(!position.is_end());
    }
    return node_to_stage_t<my_type_t>::get_p_value_normalized(*this, position);
  }

  template <typename FieldType, node_type_t NODE_TYPE, typename ConcreteType>
  std::ostream& NodeT<FieldType, NODE_TYPE, ConcreteType>::dump(std::ostream& os) {
    os << *this << ":";
    if (this->keys()) {
      return node_to_stage_t<my_type_t>::dump(*this, os, "");
    } else {
      return os << " empty!";
    }
  }

  template <typename FieldType, typename ConcreteType>
  search_result_t InternalNodeT<FieldType, ConcreteType>::lower_bound(
      const onode_key_t& key, MatchHistory& history) {
    auto ret = node_to_stage_t<my_type_t>::lower_bound_normalized(*this, key, history);

    auto& position = ret.position;
    laddr_t child_addr;
    if (position.is_end()) {
      assert(this->is_level_tail());
      child_addr = *this->get_value_ptr(position);
    } else {
      assert(ret.p_value);
      child_addr = *ret.p_value;
    }

    Ref<Node> child = get_or_load_child(child_addr, position);
    match_stat_t mstat = ret.mstat;
    if (matchable(child->field_type(), mstat)) {
      return child->lower_bound(key, history);
    } else {
      // out of lookup range due to prefix compression
      auto&& ret = child->lookup_smallest();
      return {std::move(ret), MatchKindBS::NE, mstat};
    }
  }

  template <typename FieldType, typename ConcreteType>
  search_result_t LeafNodeT<FieldType, ConcreteType>::lower_bound(
      const onode_key_t& key, MatchHistory& history) {
    if (unlikely(this->keys() == 0)) {
      // only happens when root is empty
      history.set<STAGE_LEFT>(MatchKindCMP::NE);
      return search_result_t::from(this, staged_result_t<node_type_t::LEAF, STAGE_TOP>::end());
    }

    auto result = node_to_stage_t<my_type_t>::lower_bound_normalized(*this, key, history);
    if (result.is_end()) {
      assert(this->is_level_tail());
      // return an end cursor with leaf node
    }
    return search_result_t::from(this, result);
  }

  template <typename FieldType, typename ConcreteType>
  tree_cursor_t LeafNodeT<FieldType, ConcreteType>::lookup_largest() {
    if (unlikely(this->keys() == 0)) {
      // only happens when root is empty
      return tree_cursor_t::make_end();
    }
    tree_cursor_t ret{this, {}, nullptr};
    node_to_stage_t<my_type_t>::lookup_largest_normalized(*this, ret.position, ret.p_value);
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
   public:
    // TODO: track cursors in LeafNode by position (intrusive)
    class Cursor {
     public:
      Cursor(Btree* tree, const tree_cursor_t& _cursor)
        : tree(*tree),
          cursor(_cursor) {
        // for cursors indicating end of tree, might need to
        // untrack the leaf node
        if (cursor.is_end()) {
          cursor.leaf_node.reset();
        }
      }
      Cursor(const Cursor& x) = default;
      ~Cursor() = default;

      bool is_end() const { return cursor.is_end(); }
      const onode_key_t& key() { return {}; }
      // might return Onode class to track the changing onode_t pointer
      // TODO: p_value might be invalid
      const onode_t* value() const { return cursor.p_value; }
      bool operator==(const Cursor& x) const { return cursor == x.cursor; }
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
        : tree(*tree), cursor(tree_cursor_t::make_end()) {}

      Btree& tree;
      tree_cursor_t cursor;
      std::optional<onode_key_t> key_copy;
    };

    // TODO: transaction
    // lookup
    Cursor begin() { return {this, root_node->lookup_smallest()}; }
    Cursor last() { return {this, root_node->lookup_largest()}; }
    Cursor end() { return Cursor::make_end(this); }
    bool contains(const onode_key_t& key) {
      // TODO: can be faster if contains() == true
      MatchHistory history;
      return MatchKindBS::EQ == root_node->lower_bound(key, history).match;
    }
    Cursor find(const onode_key_t& key) {
      MatchHistory history;
      auto result = root_node->lower_bound(key, history);
      if (result.match == MatchKindBS::EQ) {
        return Cursor(this, result.cursor);
      } else {
        return Cursor::make_end(this);
      }
    }
    Cursor lower_bound(const onode_key_t& key) {
      MatchHistory history;
      return Cursor(this, root_node->lower_bound(key, history).cursor);
    }
    // modifiers
    std::pair<Cursor, bool>
    insert(const onode_key_t& key, const onode_t& value) {
      MatchHistory history;
      auto [cursor, success] = root_node->insert(key, value, history);
      return {{this, cursor}, success};
    }
    size_t erase(const onode_key_t& key) {
      // TODO
      return 0u;
    }
    Cursor erase(Cursor& pos) {
      // TODO
      return Cursor::make_end(this);
    }
    Cursor erase(Cursor& first, Cursor& last) {
      // TODO
      return Cursor::make_end(this);
    }
    // stats
    size_t height() const { return root_node->level() + 1; }
    std::ostream& dump(std::ostream& os) {
      return root_node->dump(os);
    }

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
