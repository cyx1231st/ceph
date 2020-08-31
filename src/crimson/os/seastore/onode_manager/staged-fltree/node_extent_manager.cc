// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_extent_manager.h"
#include "node_extent_manager/dummy.h"

namespace crimson::os::seastore::onode {

NodeExtentManagerURef NodeExtentManager::create_dummy() {
  return NodeExtentManagerURef(new DummyNodeExtentManager());
}

NodeExtentManagerURef NodeExtentManager::create_seastore() {
  assert(false);
}

}
