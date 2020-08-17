// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include "fwd.h"

namespace crimson::os::seastore::onode {

class Btree;
class Node;

// the dummy super block which is supposed to be backed by LogicalCachedExtent
// to provide transactional update to the onode root laddr.
class DummyRootBlock {
 public:
  ~DummyRootBlock();
  laddr_t get_root_laddr() const {
    return *p_root_laddr;
  }
  void write_root_laddr(context_t c, laddr_t addr) {
    *p_root_laddr = addr;
  }
  void do_track_root(Node& root) {
    assert(root_node == nullptr);
    root_node = &root;
  }
  void do_untrack_root(Node& root) {
    assert(root_node == &root);
    root_node = nullptr;
  }
  Node* get_p_root() const {
    assert(root_node != nullptr);
    return root_node;
  }

 private:
  DummyRootBlock(Transaction& t, Btree&, laddr_t*);
  Transaction& t;
  Btree& tree;
  laddr_t* p_root_laddr;
  Node* root_node = nullptr;
  friend class DummyTransactionManager;
};
using SuperNodeURef = std::unique_ptr<DummyRootBlock>;

}
