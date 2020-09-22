// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"

namespace crimson::os::seastore::onode {

class SeastoreSuper final: public Super {
 public:
  SeastoreSuper(Transaction& t, RootNodeTracker& tracker,
                laddr_t root_addr, TransactionManager& tm)
    : Super(t, tracker), root_addr{root_addr}, tm{tm} {}
  ~SeastoreSuper() override = default;
 protected:
  laddr_t get_root_laddr() const override {
    return root_addr;
  }
  void write_root_laddr(context_t c, laddr_t addr) override;

  laddr_t root_addr;
  TransactionManager& tm;
};

class SeastoreNodeExtent final: public NodeExtent {
 public:
  SeastoreNodeExtent(ceph::bufferptr &&ptr)
    : NodeExtent(std::move(ptr)) {}
  SeastoreNodeExtent(const SeastoreNodeExtent& other)
    : NodeExtent(other) {}
  ~SeastoreNodeExtent() override = default;
 protected:
  NodeExtentRef mutate(context_t c) override;
  CachedExtentRef duplicate_for_write() override {
    return CachedExtentRef(new SeastoreNodeExtent(*this));
  }
  extent_types_t get_type() const override {
    return extent_types_t::ONODE_BLOCK_STAGED;
  }
  ceph::bufferlist get_delta() override {
    //TODO
    assert(false && "not implemented");
  }
  void apply_delta(const ceph::bufferlist&) override {
    //TODO
    assert(false && "not implemented");
  }
  //TODO: recorder
};

class SeastoreNodeExtentManager final: public NodeExtentManager {
 public:
  SeastoreNodeExtentManager(TransactionManager& tm, laddr_t min)
    : tm{tm}, addr_min{min} {};
  ~SeastoreNodeExtentManager() override = default;
  TransactionManager& get_tm() { return tm; }
 protected:
  bool is_read_isolated() const { return true; }

  tm_future<NodeExtentRef> read_extent(
      Transaction& t, laddr_t addr, extent_len_t len) {
    return tm.read_extents<SeastoreNodeExtent>(t, addr, len
    ).safe_then([](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      return NodeExtentRef(e);
    });
  }

  tm_future<NodeExtentRef> alloc_extent(
      Transaction& t, extent_len_t len) {
    return tm.alloc_extent<SeastoreNodeExtent>(t, addr_min, len
    ).safe_then([](auto extent) {
      return NodeExtentRef(extent);
    });
  }

  tm_future<Super::URef> get_super(Transaction& t, RootNodeTracker& tracker) {
    return tm.read_onode_root(t).safe_then([this, &t, &tracker](auto root_addr) {
      return Super::URef(new SeastoreSuper(t, tracker, root_addr, tm));
    });
  }

  TransactionManager& tm;
  const laddr_t addr_min;
};

inline void SeastoreSuper::write_root_laddr(context_t c, laddr_t addr) {
  root_addr = addr;
  auto nm = static_cast<SeastoreNodeExtentManager*>(&c.nm);
  nm->get_tm().write_onode_root(c.t, addr);
}

inline NodeExtentRef SeastoreNodeExtent::mutate(context_t c) {
  auto nm = static_cast<SeastoreNodeExtentManager*>(&c.nm);
  auto ret = nm->get_tm().get_mutable_extent(c.t, this);
  return ret->cast<SeastoreNodeExtent>();
}

}
