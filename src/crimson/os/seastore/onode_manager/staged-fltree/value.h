// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <ostream>

#include "crimson/common/log.h"
#include "crimson/common/type_helpers.h"

#include "include/buffer.h"
#include "node_extent_mutable.h"
#include "node_types.h"

namespace crimson::os::seastore::onode {

// value size up to 64 KiB
using value_size_t = uint16_t;

// A Btree can only have one value implementation/type.
// TODO: use strategy pattern to configure Btree to one value/recorder implementation.
enum class value_types_t :uint8_t {
  TEST = 0x52,
  ONODE,
};
inline std::ostream& operator<<(std::ostream& os, const value_types_t& type) {
  switch (type) {
  case value_types_t::TEST:
    return os << "TEST";
  case value_types_t::ONODE:
    return os << "ONODE";
  default:
    return os << "UNKNOWN(" << type << ")";
  }
}

struct value_config_t {
  value_types_t type = value_types_t::ONODE;
  value_size_t payload_size = 256;

  value_size_t allocation_size() const;

  void encode(ceph::bufferlist& encoded) const {
    ceph::encode(type, encoded);
    ceph::encode(payload_size, encoded);
  }

  static value_config_t decode(ceph::bufferlist::const_iterator& delta) {
    value_types_t type;
    ceph::decode(type, delta);
    value_size_t payload_size;
    ceph::decode(payload_size, delta);
    return value_config_t{type, payload_size};
  }
};
inline std::ostream& operator<<(std::ostream& os, const value_config_t& conf) {
  return os << "ValueConf(" << conf.type
            << ", " << conf.payload_size << "B)";
}

/**
 * value_header_t
 *
 * The value layout in tree leaf node.
 *
 * # <- alloc size -> #
 * # header | payload #
 */
struct value_header_t {
  value_types_t type;
  value_size_t payload_size;

  value_size_t allocation_size() const {
    return payload_size + sizeof(value_header_t);
  }

  const char* get_payload() const {
    return reinterpret_cast<const char*>(this) + sizeof(value_header_t);
  }

  char* get_payload() {
    return reinterpret_cast<char*>(this) + sizeof(value_header_t);
  }

  void initiate(NodeExtentMutable& mut, const value_config_t& config) {
    value_header_t header{config.type, config.payload_size};
    mut.copy_in_absolute(this, header);
    mut.set_absolute(get_payload(), 0, config.payload_size);
  }

  static value_size_t estimate_allocation_size(value_size_t payload_size) {
    return payload_size + sizeof(value_header_t);
  }
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const value_header_t& header) {
  return os << "Value(" << header.type
            << ", " << header.payload_size << "B)";
}

inline value_size_t value_config_t::allocation_size() const {
  return value_header_t::estimate_allocation_size(payload_size);
}

class ValueDeltaRecorder {
 public:
  virtual ~ValueDeltaRecorder() = default;
  ValueDeltaRecorder(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder(ValueDeltaRecorder&&) = delete;
  ValueDeltaRecorder& operator=(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder& operator=(ValueDeltaRecorder&&) = delete;

  virtual value_types_t get_type() const = 0;

  virtual void apply_value_delta(ceph::bufferlist::const_iterator&,
                                 NodeExtentMutable&,
                                 laddr_t) = 0;

  template <class Derived>
  Derived* cast() {
    assert(get_type() == Derived::rtype);
    return static_cast<Derived*>(this);
  }

 protected:
  ValueDeltaRecorder(ceph::bufferlist& encoded) : encoded{encoded} {}

  ceph::bufferlist& get_encoded(NodeExtentMutable& payload_mut) {
    ceph::encode(node_delta_op_t::SUBOP_UPDATE_VALUE, encoded);
    node_offset_t offset = payload_mut.get_node_offset();
    assert(offset > sizeof(value_header_t));
    offset -= sizeof(value_header_t);
    ceph::encode(offset, encoded);
    return encoded;
  }

  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }

 private:
  ceph::bufferlist& encoded;
};

class NodeExtentManager;
class tree_cursor_t;
class Value : public boost::intrusive_ref_counter<
                     Value, boost::thread_unsafe_counter> {
 public:
  using ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  template <class ValueT=void>
  using future = ertr::future<ValueT>;

  virtual ~Value();
  Value(const Value&) = delete;
  Value(Value&&) = delete;
  Value& operator=(const Value&) = delete;
  Value& operator=(Value&&) = delete;

  value_size_t get_payload_size() const {
    return read_value_header()->payload_size;
  }

  value_types_t get_type() const {
    return read_value_header()->type;
  }

  template <class Derived>
  Ref<Derived> cast() {
    assert(get_type() == Derived::vtype);
    return Ref<Derived>(static_cast<Derived*>(this));
  }

  bool operator==(const Value& v) const { return p_cursor == v.p_cursor; }
  bool operator!=(const Value& v) const { return !(*this == v); }

  static Ref<Value> create_value(NodeExtentManager&, Ref<tree_cursor_t>);

 protected:
  Value(NodeExtentManager&, Ref<tree_cursor_t>&);

  future<> extend(Transaction&, value_size_t extend_size);

  future<> trim(Transaction&, value_size_t trim_size);

  // TODO: return NodeExtentMutable&
  template <typename PayloadT>
  std::pair<NodeExtentMutable*, ValueDeltaRecorder*>
  prepare_mutate_payload(Transaction& t) {
    assert(sizeof(PayloadT) <= get_payload_size());

    auto [p_payload_mut, p_recorder] = do_prepare_mutate_payload(t);
    assert(p_payload_mut->get_write() ==
           const_cast<const Value*>(this)->template read_payload<char>());
    assert(p_payload_mut->get_length() == get_payload_size());
    return std::make_pair(p_payload_mut, p_recorder);
  }

  template <typename PayloadT>
  const PayloadT* read_payload() const {
    assert(sizeof(PayloadT) <= get_payload_size());
    return reinterpret_cast<const PayloadT*>(read_value_header()->get_payload());
  }

 private:
  const value_header_t* read_value_header() const;
  context_t get_context(Transaction& t) { return {nm, t}; }

  std::pair<NodeExtentMutable*, ValueDeltaRecorder*>
  do_prepare_mutate_payload(Transaction&);

  NodeExtentManager& nm;
  Ref<tree_cursor_t> p_cursor;
};

}
