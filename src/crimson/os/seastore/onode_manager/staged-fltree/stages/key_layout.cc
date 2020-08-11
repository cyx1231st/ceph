// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "key_layout.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/dummy_transaction_manager.h"

namespace crimson::os::seastore::onode {

void string_key_view_t::append_str(
    LogicalCachedExtent& dst, const char* data, size_t len, char*& p_append) {
  p_append -= sizeof(string_size_t);
  assert(len < std::numeric_limits<string_size_t>::max());
  dst.copy_in_mem((string_size_t)len, p_append);
  p_append -= len;
  dst.copy_in_mem(data, p_append, len);
}

void string_key_view_t::append_dedup(
    LogicalCachedExtent& dst, const Type& dedup_type, char*& p_append) {
  p_append -= sizeof(string_size_t);
  if (dedup_type == Type::MIN) {
    dst.copy_in_mem((string_size_t)0u, p_append);
  } else if (dedup_type == Type::MAX) {
    dst.copy_in_mem(std::numeric_limits<string_size_t>::max(), p_append);
  } else {
    assert(false);
  }
}

}
