// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "super_node.h"
#include "tree.h"

namespace crimson::os::seastore::onode {

DummyRootBlock::DummyRootBlock(Transaction& t, Btree& tree, laddr_t* p_root_laddr)
    : t{t}, tree{tree}, p_root_laddr{p_root_laddr} {
  tree.do_track_super(t, *this);
}

DummyRootBlock::~DummyRootBlock() {
  assert(root_node == nullptr);
  tree.do_untrack_super(t, *this);
}

}
