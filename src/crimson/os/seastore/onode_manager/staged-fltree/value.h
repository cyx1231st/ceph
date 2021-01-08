// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <ostream>

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
  TEST = 0,
  ONODE = 1,
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

struct value_header_t {
  static constexpr value_size_t ALIGNMENT = 8u;

  value_types_t type;
  uint8_t padding;
  value_size_t payload_size;
  uint32_t padding1;

  value_size_t value_size() const {
    return payload_size + sizeof(value_header_t);
  }

  value_size_t allocation_size() const {
    return value_size() + ALIGNMENT;
  }

  const char* get_payload() const {
    return reinterpret_cast<const char*>(this) + sizeof(value_header_t);
  }

  char* get_payload() {
    return const_cast<char*>(get_payload());
  }

  static value_size_t estimate_allocation_size(value_size_t payload_size) {
    assert(payload_size % ALIGNMENT == 0);
    return payload_size + sizeof(value_header_t) + ALIGNMENT;
  }
};
static_assert(alignof(value_header_t) <= value_header_t::ALIGNMENT);
static_assert(sizeof(value_header_t) % value_header_t::ALIGNMENT == 0);

struct value_config_t {
  value_types_t type = value_types_t::ONODE;
  value_size_t payload_size = 256;

  value_size_t allocation_size() const {
    return value_header_t::estimate_allocation_size(payload_size);
  }

  value_header_t to_header() const {
    value_header_t header{type, 0u, payload_size, 0u};
    assert(allocation_size() == header.allocation_size());
    return header;
  }

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
  return os << "ValueC(" << conf.type
            << ", " << conf.payload_size << "B)";
}

/**
 * raw_value_t
 *
 * The value layout in tree leaf node.
 *
 * # <-----> | 8 bytes padding  | <-----> #
 * #         | <- value size -> |         #
 * # padding | header | payload | padding #
 *           ^        ^         ^
 *           |        |         |
 *            aligned to 8 bytes
 *
 * TODO: make raw_value_t itself aligned to 8 bytes to simplify the implementation.
 */
struct raw_value_t {
  static constexpr auto ALIGNMENT = value_header_t::ALIGNMENT;

  const value_header_t* get_header() const {
    uint64_t p_start = reinterpret_cast<uint64_t>(this);
    p_start = (p_start + ALIGNMENT) / ALIGNMENT * ALIGNMENT;
    auto ret = reinterpret_cast<const value_header_t*>(p_start);
    assert((ret->payload_size > 0) && (ret->payload_size % ALIGNMENT == 0));
    return ret;
  }

  value_header_t* get_header() {
    return const_cast<value_header_t*>(get_header());
  }

  value_size_t allocation_size() const {
    return get_header()->allocation_size();
  }

  void initiate(NodeExtentMutable& mut, const value_config_t& config) {
    // the paddings for alignment are not zeroed
    auto p_header = get_header();
    auto header = config.to_header();
    mut.copy_in_absolute(p_header, header);
    mut.set_absolute(p_header->get_payload(), 0, p_header->payload_size);
  }
};
inline std::ostream& operator<<(std::ostream& os, const raw_value_t& value) {
  auto p_header = value.get_header();
  return os << "ValueR(" << p_header->type
            << ", " << p_header->payload_size
            << "+" << p_header->allocation_size() - p_header->payload_size << "B)";
}

class ValueDeltaRecorder {
 public:
  virtual ~ValueDeltaRecorder() = default;
  ValueDeltaRecorder(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder(ValueDeltaRecorder&&) = delete;
  ValueDeltaRecorder& operator=(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder& operator=(ValueDeltaRecorder&&) = delete;

  virtual value_types_t get_type() const = 0;

  void apply_delta(ceph::bufferlist::const_iterator& p,
                   NodeExtentMutable& node_mut,
                   laddr_t node_addr) {
    node_offset_t value_header_offset;
    ceph::decode(value_header_offset, p);
    assert(value_header_offset % value_header_t::ALIGNMENT == 0);
    auto p_header = node_mut.get_read() + value_header_offset;
    assert((uint64_t)p_header % value_header_t::ALIGNMENT == 0);
    auto p_header_ = reinterpret_cast<const value_header_t*>(p_header);
    auto payload_mut = node_mut.get_mutable_absolute(
        p_header_->get_payload(), p_header_->payload_size);
    auto value_addr = node_addr + payload_mut.get_node_offset();
    apply_value_delta(p, payload_mut, value_addr);
  }

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

  virtual void apply_value_delta(ceph::bufferlist::const_iterator&,
                                 NodeExtentMutable&,
                                 laddr_t) = 0;

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

  virtual ~Value() = default;
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

  static Ref<Value> create_value(NodeExtentManager&, Ref<tree_cursor_t>);

 protected:
  Value(NodeExtentManager&, Ref<tree_cursor_t>);

  future<> extend(Transaction&, value_size_t extend_size);

  future<> trim(Transaction&, value_size_t trim_size);

  // TODO: return NodeExtentMutable&
  template <typename PayloadT>
  std::pair<NodeExtentMutable*, ValueDeltaRecorder*>
  prepare_mutate_payload(Transaction& t) {
    static_assert(alignof(PayloadT) <= value_header_t::ALIGNMENT);
    assert(sizeof(PayloadT) <= get_payload_size());

    auto [p_payload_mut, p_recorder] = do_prepare_mutate_payload(t);
    assert(p_payload_mut->get_write() ==
           const_cast<const Value*>(this)->template read_payload<char>());
    assert(p_payload_mut->get_length() == get_payload_size());
    return std::make_pair(p_payload_mut, p_recorder);
  }

  template <typename PayloadT>
  const PayloadT* read_payload() const {
    static_assert(alignof(PayloadT) <= value_header_t::ALIGNMENT);
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
