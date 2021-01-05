// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"

#include "include/buffer.h"
#include "node_extent_mutable.h"
#include "node_types.h"

namespace crimson::os::seastore::onode {

enum class value_types_t :uint8_t {
  TEST = 0,
};

// value size up to 64 KiB
using value_size_t = uint16_t;
struct value_header_t {
  static constexpr value_size_t ALIGNMENT = 8u;

  value_types_t type;
  uint8_t padding;
  value_size_t payload_size;
  uint32_t padding1;

  value_size_t size() const { return payload_size + sizeof(value_header_t); }
};
static_assert(alignof(value_header_t) <= value_header_t::ALIGNMENT);
static_assert(sizeof(value_header_t) % value_header_t::ALIGNMENT == 0);

/**
 * node_value_t
 *
 * The value layout in tree leaf node.
 *
 * # <-----> | 8 bytes padding  | <-----> #
 * #         | <---- size ----> |         #
 * # padding | header | payload | padding #
 *           ^        ^         ^
 *           |        |         |
 *            aligned to 8 bytes
 *
 * TODO: make node_value_t itself aligned to 8 bytes to simplify the implementation.
 */
struct node_value_t {
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
    return get_header()->size() + ALIGNMENT;
  }

  static value_size_t estimate_allocation_size(value_size_t payload_size) {
    assert(payload_size % ALIGNMENT == 0);
    return payload_size + sizeof(value_header_t) + ALIGNMENT;
  }
};

class ValueDeltaRecorder {
 public:
  virtual ~ValueDeltaRecorder() = default;
  ValueDeltaRecorder(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder(ValueDeltaRecorder&&) = delete;
  ValueDeltaRecorder& operator=(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder& operator=(ValueDeltaRecorder&&) = delete;

  void apply_delta(ceph::bufferlist::const_iterator& p,
                   NodeExtentMutable& node_mut,
                   laddr_t node_addr) {
    node_offset_t value_header_offset;
    ceph::decode(value_header_offset, p);
    assert(value_header_offset % value_header_t::ALIGNMENT == 0);
    auto p_header = node_mut.get_read() + value_header_offset;
    assert((uint64_t)p_header % value_header_t::ALIGNMENT == 0);
    auto p_header_ = reinterpret_cast<const value_header_t*>(p_header);
    auto payload_offset = value_header_offset + sizeof(value_header_t);
    auto payload_mut = node_mut.get_mutable(payload_offset, p_header_->payload_size);
    auto value_addr = node_addr + payload_offset;
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

  virtual value_types_t get_type() const = 0;

  virtual void apply_value_delta(ceph::bufferlist::const_iterator&,
                                 NodeExtentMutable&,
                                 laddr_t) = 0;

 private:
  ceph::bufferlist& encoded;
};

class Btree;
class tree_cursor_t;
class Value {
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

 protected:
  Value() = default;

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

    auto p_value_header = reinterpret_cast<const char*>(read_value_header());
    return reinterpret_cast<const PayloadT*>(p_value_header + sizeof(value_header_t));
  }

 private:
  const value_header_t* read_value_header() const;

  std::pair<NodeExtentMutable*, ValueDeltaRecorder*>
  do_prepare_mutate_payload(Transaction&);

  Btree* p_tree;
  Ref<tree_cursor_t> p_cursor;
};

}
