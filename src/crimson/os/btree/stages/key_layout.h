// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <cstring>
#include <limits>
#include <optional>
#include <ostream>

#include "crimson/os/btree/fwd.h"
#include "crimson/os/btree/tree_types.h"

namespace crimson::os::seastore::onode {

// TODO: decide by NODE_BLOCK_SIZE
using node_offset_t = uint16_t;
constexpr node_offset_t BLOCK_SIZE = 1u << 12;
constexpr node_offset_t NODE_BLOCK_SIZE = BLOCK_SIZE * 1u;

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
inline MatchKindCMP compare_to(const onode_key_t& key, const shard_pool_t& target) {
  return _compare_shard_pool(key, target);
}
inline std::ostream& operator<<(std::ostream& os, const shard_pool_t& sp) {
  return os << (unsigned)sp.shard << "," << sp.pool;
}

struct crush_t {
  bool operator==(const crush_t& x) const { return crush == x.crush; }
  bool operator!=(const crush_t& x) const { return !(*this == x); }
  static crush_t from_key(const onode_key_t& key) { return {key.crush}; }

  crush_hash_t crush;
} __attribute__((packed));
inline MatchKindCMP compare_to(const onode_key_t& key, const crush_t& target) {
  return _compare_crush(key, target);
}
inline std::ostream& operator<<(std::ostream& os, const crush_t& c) {
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
inline MatchKindCMP compare_to(const onode_key_t& key, const shard_pool_crush_t& target) {
  auto ret = _compare_shard_pool(key, target.shard_pool);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return _compare_crush(key, target.crush);
}
inline std::ostream& operator<<(std::ostream& os, const shard_pool_crush_t& spc) {
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
inline MatchKindCMP compare_to(const onode_key_t& key, const snap_gen_t& target) {
  return _compare_snap_gen(key, target);
}
inline std::ostream& operator<<(std::ostream& os, const snap_gen_t& sg) {
  return os << sg.snap << "," << sg.gen;
}

class LogicalCachedExtent;

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

  static void append_str(
      LogicalCachedExtent&, const char* data, size_t len, char*& p_append);

  static void append_str(LogicalCachedExtent& dst,
                         const std::string& str,
                         char*& p_append) {
    append_str(dst, str.data(), str.length(), p_append);
  }

  static void append_dedup(
      LogicalCachedExtent& dst, const Type& dedup_type, char*& p_append);

  static void append(LogicalCachedExtent& dst,
                     const string_key_view_t& view,
                     char*& p_append) {
    assert(view.type() == Type::STR);
    append_str(dst, view.p_key, view.length, p_append);
  }

  const char* p_key;
  const char* p_length;
  // TODO: remove if p_length is aligned
  string_size_t length;

  friend std::ostream& operator<<(std::ostream&, const string_key_view_t&);
};
inline MatchKindCMP compare_to(const std::string& key, const string_key_view_t& target) {
  assert(key.length());
  if (target.type() == string_key_view_t::Type::MIN) {
    return MatchKindCMP::PO;
  } else if (target.type() == string_key_view_t::Type::MAX) {
    return MatchKindCMP::NE;
  }
  assert(target.p_key);
  return toMatchKindCMP(key.compare(0u, key.length(), target.p_key, target.length));
}
inline std::ostream& operator<<(std::ostream& os, const string_key_view_t& view) {
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

  static void append(LogicalCachedExtent& dst,
                     const onode_key_t& key,
                     const ns_oid_view_t::Type& dedup_type,
                     char*& p_append) {
    if (dedup_type == Type::STR) {
      string_key_view_t::append_str(dst, key.nspace, p_append);
      string_key_view_t::append_str(dst, key.oid, p_append);
    } else {
      string_key_view_t::append_dedup(dst, dedup_type, p_append);
    }
  }

  static void append(LogicalCachedExtent& dst,
                     const ns_oid_view_t& view,
                     char*& p_append) {
    if (view.type() == Type::STR) {
      string_key_view_t::append(dst, view.nspace, p_append);
      string_key_view_t::append(dst, view.oid, p_append);
    } else {
      string_key_view_t::append_dedup(dst, view.type(), p_append);
    }
  }

  string_key_view_t nspace;
  string_key_view_t oid;
};
inline MatchKindCMP compare_to(const onode_key_t& key, const ns_oid_view_t& target) {
  auto ret = compare_to(key.nspace, target.nspace);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return compare_to(key.oid, target.oid);
}
inline std::ostream& operator<<(std::ostream& os, const ns_oid_view_t& ns_oid) {
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

}
