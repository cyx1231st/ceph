// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "fwd.h"

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
inline std::ostream& operator<<(std::ostream& os, const onode_key_t& key) {
  return os << "key("
            << (unsigned)key.shard << "," << key.pool << "," << key.crush << ";\" "
            << key.nspace << "\",\"" << key.oid << "\"; "
            << key.snap << "," << key.gen << ")";
}

template <typename L, typename R>
MatchKindCMP _compare_shard_pool(const L& l, const R& r) {
  auto ret = toMatchKindCMP(l.shard, r.shard);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(l.pool, r.pool);
}
template <typename L, typename R>
MatchKindCMP _compare_crush(const L& l, const R& r) {
  return toMatchKindCMP(l.crush, r.crush);
}
template <typename L, typename R>
MatchKindCMP _compare_snap_gen(const L& l, const R& r) {
  auto ret = toMatchKindCMP(l.snap, r.snap);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(l.gen, r.gen);
}
inline MatchKindCMP compare_to(const onode_key_t& key, const onode_key_t& target) {
  auto ret = _compare_shard_pool(key, target);
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = _compare_crush(key, target);
  if (ret != MatchKindCMP::EQ)
  ret = toMatchKindCMP(key.nspace, target.nspace);
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = toMatchKindCMP(key.oid, target.oid);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return _compare_snap_gen(key, target);
}

}
