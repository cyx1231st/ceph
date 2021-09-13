// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "key_layout.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

void string_key_view_t::append(
    NodeExtentMutable& mut, std::string_view str, char*& p_append)
{
  assert(is_valid_size(str.length()));
  p_append -= sizeof(string_size_t);
  string_size_t len = str.length();
  mut.copy_in_absolute(p_append, len);
  p_append -= len;
  mut.copy_in_absolute(p_append, str.data(), len);
}

}
