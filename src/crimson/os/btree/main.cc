// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <vector>

#include "global/signal_handler.h"

#include "btree.h"

using namespace crimson::os::seastore::onode;

int main(int argc, char* argv[])
{
  // TODO: move to unit tests
  install_standard_sighandlers();

  // sizes of struct
  std::cout << "sizes of struct: " << std::endl;
  std::cout << "node_header_t: " << sizeof(node_header_t) << std::endl;
  std::cout << "shard_pool_t: " << sizeof(shard_pool_t) << std::endl;
  std::cout << "shard_pool_crush_t: " << sizeof(shard_pool_crush_t) << std::endl;
  std::cout << "crush_t: " << sizeof(crush_t) << std::endl;
  std::cout << "snap_gen_t: " << sizeof(snap_gen_t) << std::endl;
  std::cout << "slot_0_t: " << sizeof(slot_0_t) << std::endl;
  std::cout << "slot_1_t: " << sizeof(slot_1_t) << std::endl;
  std::cout << "slot_3_t: " << sizeof(slot_3_t) << std::endl;
  std::cout << "node_fields_0_t: " << sizeof(node_fields_0_t) << std::endl;
  std::cout << "node_fields_1_t: " << sizeof(node_fields_1_t) << std::endl;
  std::cout << "node_fields_2_t: " << sizeof(node_fields_2_t) << std::endl;
  std::cout << "internal_fields_3_t: " << sizeof(internal_fields_3_t) << std::endl;
  std::cout << "leaf_fields_3_t: " << sizeof(leaf_fields_3_t) << std::endl;
  std::cout << "internal_sub_item_t: " << sizeof(internal_sub_item_t) << std::endl;
  std::cout << std::endl;

  // sizes of an insertion
  auto f_sum = [] (std::tuple<node_offset_t, node_offset_t> input) {
    return std::get<0>(input) + std::get<1>(input);
  };
  std::cout << "sizes of a full-string insertion('ns', 'oid', onode_t{1}): " << std::endl;
  onode_key_t key = {0, 0, 0, "ns", "oid", 0, 0};
  onode_t value = {1};
  std::cout << "internal_sub_items_t: " << internal_sub_items_t::estimate_insertion() << std::endl;
  std::cout << "item_iterator_t<INTERNAL>: "
            << item_iterator_t<node_type_t::INTERNAL>::estimate_insertion(&key) << std::endl;
  std::cout << "InternalNode0:" << f_sum(InternalNode0::estimate_insertion(&key)) << std::endl;
  std::cout << "InternalNode1:" << f_sum(InternalNode1::estimate_insertion(&key)) << std::endl;
  std::cout << "InternalNode2:" << f_sum(InternalNode2::estimate_insertion(&key)) << std::endl;
  std::cout << "InternalNode3:" << f_sum(InternalNode3::estimate_insertion(&key)) << std::endl;
  std::cout << "leaf_sub_items_t: " << leaf_sub_items_t::estimate_insertion(value) << std::endl;
  std::cout << "item_iterator_t<LEAF>: "
            << item_iterator_t<node_type_t::LEAF>::estimate_insertion(&key, value) << std::endl;
  std::cout << "LeafNode0:" << f_sum(LeafNode0::estimate_insertion(&key, value)) << std::endl;
  std::cout << "LeafNode1:" << f_sum(LeafNode1::estimate_insertion(&key, value)) << std::endl;
  std::cout << "LeafNode2:" << f_sum(LeafNode2::estimate_insertion(&key, value)) << std::endl;
  std::cout << "LeafNode3:" << f_sum(LeafNode3::estimate_insertion(&key, value)) << std::endl;
  std::cout << std::endl;

  // node tests
  auto internal_node_0 = InternalNode0::allocate(1u, false);
  auto internal_node_1 = InternalNode1::allocate(1u, false);
  auto internal_node_2 = InternalNode2::allocate(1u, false);
  auto internal_node_3 = InternalNode3::allocate(1u, false);
  auto internal_node_0t = InternalNode0::allocate(1u, true);
  auto internal_node_1t = InternalNode1::allocate(1u, true);
  auto internal_node_2t = InternalNode2::allocate(1u, true);
  auto internal_node_3t = InternalNode3::allocate(1u, true);
  std::vector<Ref<Node>> internal_nodes = {
    internal_node_0, internal_node_1, internal_node_2, internal_node_3,
    internal_node_0t, internal_node_1t, internal_node_2t, internal_node_3t};

  auto leaf_node_0 = LeafNode0::allocate(false);
  auto leaf_node_1 = LeafNode1::allocate(false);
  auto leaf_node_2 = LeafNode2::allocate(false);
  auto leaf_node_3 = LeafNode3::allocate(false);
  auto leaf_node_0t = LeafNode0::allocate(true);
  auto leaf_node_1t = LeafNode1::allocate(true);
  auto leaf_node_2t = LeafNode2::allocate(true);
  auto leaf_node_3t = LeafNode3::allocate(true);
  std::vector<Ref<LeafNode>> leaf_nodes = {
    leaf_node_0, leaf_node_1, leaf_node_2, leaf_node_3,
    leaf_node_0t, leaf_node_1t, leaf_node_2t, leaf_node_3t};

  std::vector<Ref<Node>> nodes;
  nodes.insert(nodes.end(), internal_nodes.begin(), internal_nodes.end());
  nodes.insert(nodes.end(), leaf_nodes.begin(), leaf_nodes.end());

  std::cout << "allocated nodes:" << std::endl;
  for (auto& node : nodes) {
    std::cout << *node << std::endl;
  }
  std::cout << std::endl;

  // tree tests
  auto& btree = Btree::get();

  transaction_manager.free_all();
}
