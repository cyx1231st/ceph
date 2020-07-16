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

}
