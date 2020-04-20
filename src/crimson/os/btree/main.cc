// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "btree.h"

using namespace crimson::os::seastore::onode;

int main(int argc, char* argv[])
{
  std::cout << "size fixed_key_0_t: " << sizeof(fixed_key_0_t) << std::endl;
  std::cout << "size fixed_key_1_t: " << sizeof(fixed_key_1_t) << std::endl;
  std::cout << "size fixed_key_3_t: " << sizeof(fixed_key_3_t) << std::endl;
  std::cout << "size node_header_t: " << sizeof(node_header_t) << std::endl;
  std::cout << "size slot_0_t: " << sizeof(slot_0_t) << std::endl;
  std::cout << "size slot_1_t: " << sizeof(slot_1_t) << std::endl;
  std::cout << "size slot_3_t: " << sizeof(slot_3_t) << std::endl;
  std::cout << "size node_fields_0_t: " << sizeof(node_fields_0_t) << std::endl;
  std::cout << "size node_fields_1_t: " << sizeof(node_fields_1_t) << std::endl;
  std::cout << "size node_fields_2_t: " << sizeof(node_fields_2_t) << std::endl;
  std::cout << "size internal_fields_3_t: " << sizeof(internal_fields_3_t) << std::endl;
  std::cout << "size leaf_fields_3_t: " << sizeof(leaf_fields_3_t) << std::endl;

  auto& btree = Btree::get();

  transaction_manager.free_all();
}
