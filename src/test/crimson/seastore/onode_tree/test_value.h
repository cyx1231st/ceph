// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/value.h"

namespace crimson::os::seastore::onode {

class TestValue final : public Value {
 public:
  using id_t = uint16_t;
  using magic_t = uint32_t;
  static constexpr auto vtype = value_types_t::TEST;

 private:
  struct payload_t {
    id_t id;
  };

  struct Replayable {
    static void set_id(NodeExtentMutable& payload_mut, id_t id) {
      auto p_payload = get_write(payload_mut);
      p_payload->id = id;
    }

    static void set_tail_magic(NodeExtentMutable& payload_mut, magic_t magic) {
      auto length = payload_mut.get_length();
      auto offset_magic = length - sizeof(magic_t);
      payload_mut.copy_in_relative(offset_magic, magic);
    }

   private:
    static payload_t* get_write(NodeExtentMutable& payload_mut) {
      return reinterpret_cast<payload_t*>(payload_mut.get_write());
    }
  };

 public:
  class Recorder final : public ValueDeltaRecorder {
    enum class delta_op_t : uint8_t {
      UPDATE_ID,
      UPDATE_TAIL_MAGIC,
    };

   public:
    static constexpr auto rtype = vtype;

    Recorder(ceph::bufferlist& encoded) : ValueDeltaRecorder(encoded) {}
    ~Recorder() override = default;

    void encode_set_id(NodeExtentMutable& payload_mut, id_t id) {
      auto encoded = get_encoded(payload_mut);
      ceph::encode(delta_op_t::UPDATE_ID, encoded);
      ceph::encode(id, encoded);
    }

    void encode_set_tail_magic(NodeExtentMutable& payload_mut, magic_t magic) {
      auto encoded = get_encoded(payload_mut);
      ceph::encode(delta_op_t::UPDATE_TAIL_MAGIC, encoded);
      ceph::encode(magic, encoded);
    }

   protected:
    void apply_value_delta(ceph::bufferlist::const_iterator& delta,
                           NodeExtentMutable& payload_mut,
                           laddr_t value_addr) override {
      delta_op_t op;
      try {
        ceph::decode(op, delta);
        switch (op) {
        case delta_op_t::UPDATE_ID: {
          logger().debug("OTree::Value::Replay: decoding UPDATE_ID ...");
          id_t id;
          ceph::decode(id, delta);
          logger().debug("OTree::Value::Replay: apply id={} ...", id);
          Replayable::set_id(payload_mut, id);
          break;
        }
        case delta_op_t::UPDATE_TAIL_MAGIC: {
          logger().debug("OTree::Value::Replay: decoding UPDATE_TAIL_MAGIC ...");
          magic_t magic;
          ceph::decode(magic, delta);
          logger().debug("OTree::Value::Replay: apply magic={} ...", magic);
          Replayable::set_tail_magic(payload_mut, magic);
          break;
        }
        default:
          logger().error("OTree::Value::Replay: got unknown op {} when replay {:#x}+{:#x}",
                         op, value_addr, payload_mut.get_length());
          ceph_abort();
        }
      } catch (buffer::error& e) {
        logger().error("OTree::Value::Replay: got decode error {} when replay {:#x}+{:#x}",
                       e, value_addr, payload_mut.get_length());
        ceph_abort();
      }
    }

    value_types_t get_type() const override { return rtype; }

   private:
    seastar::logger& logger() {
      return crimson::get_logger(ceph_subsys_test);
    }
  };

  TestValue() : Value() {}
  ~TestValue() override = default;

  id_t get_id() const {
    return read_payload<payload_t>()->id;
  }
  void set_id_replayable(Transaction& t, id_t id) {
    auto [p_payload_mut, p_recorder_base] = prepare_mutate_payload<payload_t>(t);
    if (p_recorder_base) {
      auto p_recorder = p_recorder_base->cast<Recorder>();
      p_recorder->encode_set_id(*p_payload_mut, id);
    }
    Replayable::set_id(*p_payload_mut, id);
  }

  magic_t get_tail_magic(magic_t magic) const {
    auto p_payload = read_payload<payload_t>();
    auto offset_magic = get_payload_size() - sizeof(magic_t);
    auto p_magic = reinterpret_cast<const char*>(p_payload) + offset_magic;
    return *reinterpret_cast<const magic_t*>(p_magic);
  }
  void set_tail_magic_replayable(Transaction& t, magic_t magic) {
    auto [p_payload_mut, p_recorder_base] = prepare_mutate_payload<payload_t>(t);
    if (p_recorder_base) {
      auto p_recorder = p_recorder_base->cast<Recorder>();
      p_recorder->encode_set_tail_magic(*p_payload_mut, magic);
    }
    Replayable::set_tail_magic(*p_payload_mut, magic);
  }
};

}
