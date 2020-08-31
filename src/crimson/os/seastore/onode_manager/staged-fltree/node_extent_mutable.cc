// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_extent_mutable.h"
#include "node_extent_manager.h"

namespace crimson::os::seastore::onode {

NodeExtentMutable::NodeExtentMutable(NodeExtent& extent)
    : p_extent{&extent} {
  assert(p_extent->is_pending());
}

const char* NodeExtentMutable::get_read() const {
  assert(p_extent->is_pending());
  return p_extent->get_bptr().c_str();
}

char* NodeExtentMutable::get_write() {
  assert(p_extent->is_pending());
  return p_extent->get_bptr().c_str();
}

void NodeExtentMutable::test_copy_from(const NodeExtent& from) {
  assert(p_extent->get_length() == from.get_length());
  std::memcpy(get_write(), from.get_read(), p_extent->get_length());
}

const char* NodeExtentMutable::buf_upper_bound() const {
  return get_read() + p_extent->get_length();
}

}
