// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

namespace crimson::os::seastore::onode {
  /*
   * stubs
   */
  using laddr_t = uint64_t;

  /*
   * onode indexes
   */
  using shard_t = int8_t;
  using pool_t = int64_t;
  using crush_hash_t = uint32_t;
  // nspace, oid (variable)
  using snap_t = uint64_t;
  using gen_t = uint64_t;

  /*
   * fixed keys
   */
  // TODO: consider alignments
  struct fixed_key_0_t {
    shard_t shard;
    pool_t pool;
    crush_hash_t crush_hash;
  } __attribute__((packed));

  struct fixed_key_1_t {
    crush_hash_t crush_hash;
  } __attribute__((packed));

  struct fixed_key_3_t {
    snap_t snap;
    gen_t gen;
  } __attribute__((packed));

  /*
   * btree types
   */
  constexpr uint32_t NODE_BLOCK_SIZE = 1u << 12;
  // TODO: decide by NODE_BLOCK_SIZE
  using node_offset_t = uint16_t;

  // allowed fixed_key_type: fixed_key_0_t, fixed_key_1_t
  template <typename fixed_key_type>
  struct slot_t {
    fixed_key_type key;
    node_offset_t right_offset;
  } __attribute__((packed));

  template <typename fixed_key_type>
  struct node_fields_01_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(slot_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_slots_t = uint8_t;
    num_slots_t num_slots = 0u;
    slot_t<fixed_key_type> slots[];
  } __attribute__((packed));

  struct node_fields_2_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(node_off_t), sizeof(laddr_t)
    // and the minimal size of variable_key.
    using num_offsets_t = uint8_t;
    num_offsets_t num_offsets = 0u;
    node_offset_t offsets[];
  } __attribute__((packed));

  // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
  static constexpr unsigned MAX_NUM_KEYS_I3 = 170;
  template <unsigned MAX_NUM_KEYS>
  struct _internal_fields_3_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
    using num_keys_t = uint8_t;
    num_keys_t num_keys = 0u;
    fixed_key_3_t keys[MAX_NUM_KEYS];
    laddr_t child_addrs[MAX_NUM_KEYS + 1];
  } __attribute__((packed));
  static_assert(sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3>) <= NODE_BLOCK_SIZE &&
                sizeof(_internal_fields_3_t<MAX_NUM_KEYS_I3 + 1>) > NODE_BLOCK_SIZE);
  using internal_fields_3_t = _internal_fields_3_t<MAX_NUM_KEYS_I3>;

  struct leaf_fields_3_t {
    // TODO: decide by NODE_BLOCK_SIZE, sizeof(fixed_key_3_t), sizeof(laddr_t)
    using num_values_t = uint8_t;
    num_values_t num_values = 0u;
    fixed_key_3_t keys[];
  } __attribute__((packed));
};
