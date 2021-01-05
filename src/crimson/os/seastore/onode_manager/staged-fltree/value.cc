// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "value.h"

#include "tree.h"
#include "node.h"

namespace crimson::os::seastore::onode {

using ertr = Value::ertr;
template <class ValueT=void>
using future = Value::future<ValueT>;

future<> Value::extend(Transaction& t, value_size_t extend_size) {
  auto target_size = get_payload_size() + extend_size;
  return p_cursor->extend_value(p_tree->get_context(t), extend_size
  ).safe_then([this, target_size] {
    assert(target_size == get_payload_size());
  });
}

future<> Value::trim(Transaction& t, value_size_t trim_size) {
  assert(get_payload_size() > trim_size);
  auto target_size = get_payload_size() - trim_size;
  return p_cursor->trim_value(p_tree->get_context(t), trim_size
  ).safe_then([this, target_size] {
    assert(target_size == get_payload_size());
  });
}

const value_header_t* Value::read_value_header() const {
  return p_cursor->read_value_header();
}

std::pair<NodeExtentMutable*, ValueDeltaRecorder*>
Value::do_prepare_mutate_payload(Transaction& t) {
   return p_cursor->prepare_mutate_value_payload(p_tree->get_context(t));
}

}
