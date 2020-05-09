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

  // struct sizes
  std::cout << "size node_header_t: " << sizeof(node_header_t) << std::endl;
  std::cout << "size fixed_key_0_t: " << sizeof(fixed_key_0_t) << std::endl;
  std::cout << "size fixed_key_1_t: " << sizeof(fixed_key_1_t) << std::endl;
  std::cout << "size fixed_key_3_t: " << sizeof(fixed_key_3_t) << std::endl;
  std::cout << "size slot_0_t: " << sizeof(slot_0_t) << std::endl;
  std::cout << "size slot_1_t: " << sizeof(slot_1_t) << std::endl;
  std::cout << "size slot_3_t: " << sizeof(slot_3_t) << std::endl;
  std::cout << "size node_fields_0_t: " << sizeof(node_fields_0_t) << std::endl;
  std::cout << "size node_fields_1_t: " << sizeof(node_fields_1_t) << std::endl;
  std::cout << "size node_fields_2_t: " << sizeof(node_fields_2_t) << std::endl;
  std::cout << "size internal_fields_3_t: " << sizeof(internal_fields_3_t) << std::endl;
  std::cout << "size leaf_fields_3_t: " << sizeof(leaf_fields_3_t) << std::endl;
  std::cout << "size internal_sub_item_t: " << sizeof(internal_sub_item_t) << std::endl;

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
    std::cout << "  " << *node << std::endl;
  }

  // tree tests
  auto& btree = Btree::get();

  transaction_manager.free_all();
}
