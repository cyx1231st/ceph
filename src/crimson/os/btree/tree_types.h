// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

namespace crimson::os::seastore::onode {

// might be managed by an Onode class
struct onode_t {
  // onode should be smaller than a node
  uint16_t size; // address up to 64 KiB sized node
  // omap, extent_map, inline data
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const onode_t& node) {
  return os << "onode(" << node.size << ")";
}

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

enum class MatchKindCMP : int8_t { NE = -1, EQ = 0, PO };
inline MatchKindCMP toMatchKindCMP(int value) {
  if (value > 0) {
    return MatchKindCMP::PO;
  } else if (value < 0) {
    return MatchKindCMP::NE;
  } else {
    return MatchKindCMP::EQ;
  }
}

template <typename T>
inline MatchKindCMP _compare_shard_pool(const onode_key_t& key, const T& target) {
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
inline MatchKindCMP _compare_crush(const onode_key_t& key, const T& target) {
  if (key.crush < target.crush)
    return MatchKindCMP::NE;
  if (key.crush > target.crush)
    return MatchKindCMP::PO;
  return MatchKindCMP::EQ;
}
template <typename T>
inline MatchKindCMP _compare_snap_gen(const onode_key_t& key, const T& target) {
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
inline MatchKindCMP compare_to(const onode_key_t& key, const onode_key_t& target) {
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

}
