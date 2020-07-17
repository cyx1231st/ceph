// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node_layout.h"

#include "crimson/os/btree/dummy_transaction_manager.h"

namespace crimson::os::seastore::onode {

#define F013_T _node_fields_013_t<SlotType>
#define F013_INST(ST) _node_fields_013_t<ST>
#define F013_TEMPLATE(ST) template struct F013_INST(ST)
F013_TEMPLATE(slot_0_t);
F013_TEMPLATE(slot_1_t);
F013_TEMPLATE(slot_3_t);

template <typename SlotType>
void F013_T::update_size_at(
    LogicalCachedExtent& dst, const me_t& node, size_t index, int change) {
  assert(index <= node.num_keys);
  for (const auto* p_slot = &node.slots[index];
       p_slot < &node.slots[node.num_keys];
       ++p_slot) {
    node_offset_t offset = p_slot->right_offset;
    dst.copy_in_mem(node_offset_t(offset - change), (void*)&(p_slot->right_offset));
  }
}

template <typename SlotType>
void F013_T::append_key(
    LogicalCachedExtent& dst, const key_t& key, char*& p_append) {
  dst.copy_in_mem(key, p_append);
  p_append += sizeof(key_t);
}

template <typename SlotType>
void F013_T::append_offset(
    LogicalCachedExtent& dst, node_offset_t offset_to_right, char*& p_append) {
  dst.copy_in_mem(offset_to_right, p_append);
  p_append += sizeof(node_offset_t);
}

template <typename SlotType>
template <KeyT KT>
void F013_T::insert_at(
    LogicalCachedExtent& dst, const full_key_t<KT>& key,
    const me_t& node, size_t index, node_offset_t size_right) {
  assert(index <= node.num_keys);
  update_size_at(dst, node, index, size_right);
  auto p_insert = const_cast<char*>(fields_start(node)) +
                  node.get_key_start_offset(index);
  auto p_shift_end = fields_start(node) + node.get_key_start_offset(node.num_keys);
  dst.shift_mem(p_insert, p_shift_end - p_insert, estimate_insert_one());
  dst.copy_in_mem(num_keys_t(node.num_keys + 1), (void*)&node.num_keys);
  append_key(dst, key_t::template from_key<KT>(key), p_insert);
  append_offset(dst, node.get_item_end_offset(index) - size_right, p_insert);
}
#define IA_TEMPLATE(ST, KT) template void F013_INST(ST)::      \
    insert_at<KT>(LogicalCachedExtent&, const full_key_t<KT>&, \
                  const F013_INST(ST)&, size_t, node_offset_t)
IA_TEMPLATE(slot_0_t, KeyT::VIEW);
IA_TEMPLATE(slot_1_t, KeyT::VIEW);
IA_TEMPLATE(slot_3_t, KeyT::VIEW);
IA_TEMPLATE(slot_0_t, KeyT::HOBJ);
IA_TEMPLATE(slot_1_t, KeyT::HOBJ);
IA_TEMPLATE(slot_3_t, KeyT::HOBJ);

void node_fields_2_t::append_offset(
    LogicalCachedExtent& dst, node_offset_t offset_to_right, char*& p_append) {
  dst.copy_in_mem(offset_to_right, p_append);
  p_append += sizeof(node_offset_t);
}

}
