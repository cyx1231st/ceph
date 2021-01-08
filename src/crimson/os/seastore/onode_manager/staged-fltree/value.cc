// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "value.h"

#include "crimson/common/log.h"
#include "node.h"
#include "node_delta_recorder.h"

// value implementations
#include "test/crimson/seastore/onode_tree/test_value.h"

namespace crimson::os::seastore::onode {

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

using ertr = Value::ertr;
template <class ValueT=void>
using future = Value::future<ValueT>;

Value::Value(NodeExtentManager& nm, Ref<tree_cursor_t> p_cursor)
  : nm{nm}, p_cursor{p_cursor} {}

future<> Value::extend(Transaction& t, value_size_t extend_size) {
  auto target_size = get_payload_size() + extend_size;
  return p_cursor->extend_value(get_context(t), extend_size
  ).safe_then([this, target_size] {
    assert(target_size == get_payload_size());
  });
}

future<> Value::trim(Transaction& t, value_size_t trim_size) {
  assert(get_payload_size() > trim_size);
  auto target_size = get_payload_size() - trim_size;
  return p_cursor->trim_value(get_context(t), trim_size
  ).safe_then([this, target_size] {
    assert(target_size == get_payload_size());
  });
}

const value_header_t* Value::read_value_header() const {
  return p_cursor->read_value_header();
}

std::pair<NodeExtentMutable*, ValueDeltaRecorder*>
Value::do_prepare_mutate_payload(Transaction& t) {
   return p_cursor->prepare_mutate_value_payload(get_context(t));
}

Ref<Value> Value::create_value(
    NodeExtentManager& nm, Ref<tree_cursor_t> p_cursor) {
  assert(p_cursor && !p_cursor->is_end());
  auto type = p_cursor->read_value_header()->type;
  switch (type) {
  case value_types_t::TEST:
    return new TestValue(nm, p_cursor);
  case value_types_t::ONODE:
    ceph_abort("not implemented");
  default:
    logger().error("OTree::Value::create: got unexpected type={}", type);
    ceph_abort("impossible type");
  }
}

// node_delta_recorder.h
ValueDeltaRecorder* DeltaRecorder::get_value_recorder(value_types_t type) {
  if (!value_recorder) {
    switch (type) {
    case value_types_t::TEST:
      value_recorder.reset(new TestValue::Recorder(encoded));
      break;
    case value_types_t::ONODE:
      ceph_abort("not implemented");
      break;
    default:
      logger().error("OTree::Recorder::value: got unexpected type={}", type);
      ceph_abort("impossible type");
    }
  }
  assert(value_recorder);
  assert(value_recorder->get_type() == type);
  return value_recorder.get();
}

}
