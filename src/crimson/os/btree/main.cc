// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include "global/signal_handler.h"

#include "btree.h"

using namespace crimson::os::seastore::onode;

class Onodes {
 public:
  Onodes(size_t n) {
    for (size_t i = 1; i <= n; ++i) {
      auto p_onode = &create(i * 8);
      onodes.push_back(p_onode);
    }
  }

  ~Onodes() {
    std::for_each(tracked_onodes.begin(), tracked_onodes.end(),
                  [] (onode_t* onode) {
      std::free(onode);
    });
  }

  const onode_t& create(size_t size) {
    assert(size >= sizeof(onode_t) + sizeof(uint32_t));
    uint32_t target = size * 137;
    auto p_mem = (char*)std::malloc(size);
    auto p_onode = (onode_t*)p_mem;
    tracked_onodes.push_back(p_onode);
    p_onode->size = size;
    p_mem += (size - sizeof(uint32_t));
    std::memcpy(p_mem, &target, sizeof(uint32_t));
    validate(*p_onode);
    return *p_onode;
  }

  const onode_t& pick() const {
#if 1
    // always pick the largest onode
    return *onodes[onodes.size() - 1];
#else
    // pick randomly
    auto index = rd() % onodes.size();
    return *onodes[index];
#endif
  }

  static void validate(const onode_t& node) {
    auto p_target = (const char*)&node + node.size - sizeof(uint32_t);
    uint32_t target;
    std::memcpy(&target, p_target, sizeof(uint32_t));
    assert(target == node.size * 137);
  }

 private:
  mutable std::random_device rd;
  std::vector<const onode_t*> onodes;
  std::vector<onode_t*> tracked_onodes;
};

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

  // sizes of a key-value insertion
  {
    auto f_sum = [] (std::tuple<node_offset_t, node_offset_t> input) {
      return std::get<0>(input) + std::get<1>(input);
    };
    std::cout << "sizes of a key-value insertion (full-string):" << std::endl;
    std::cout << "s-p-c, 'n'-'o', s-g => onode_t{2}: typically internal 41B, leaf 35B" << std::endl;
    onode_key_t key = {0, 0, 0, "n", "o", 0, 0};
    onode_t value = {2};
    std::cout << "InternalNode0: "
              << f_sum(InternalNode0::estimate_insert_one(&key)) << " "
              << item_iterator_t<node_type_t::INTERNAL>::estimate_insert_one(&key) << " "
              << internal_sub_items_t::estimate_insert_one() << std::endl;
    std::cout << "InternalNode1: "
              << f_sum(InternalNode1::estimate_insert_one(&key)) << " "
              << item_iterator_t<node_type_t::INTERNAL>::estimate_insert_one(&key) << " "
              << internal_sub_items_t::estimate_insert_one() << std::endl;
    std::cout << "InternalNode2: "
              << f_sum(InternalNode2::estimate_insert_one(&key)) << " "
              << internal_sub_items_t::estimate_insert_one() << std::endl;
    std::cout << "InternalNode3: "
              << f_sum(InternalNode3::estimate_insert_one(&key)) << std::endl;
    std::cout << "LeafNode0: "
              << f_sum(LeafNode0::estimate_insert_one(&key, value)) << " "
              << item_iterator_t<node_type_t::LEAF>::estimate_insert_one(&key, value) << " "
              << leaf_sub_items_t::estimate_insert_one(value) << std::endl;
    std::cout << "LeafNode1: "
              << f_sum(LeafNode1::estimate_insert_one(&key, value)) << " "
              << item_iterator_t<node_type_t::LEAF>::estimate_insert_one(&key, value) << " "
              << leaf_sub_items_t::estimate_insert_one(value) << std::endl;
    std::cout << "LeafNode2: "
              << f_sum(LeafNode2::estimate_insert_one(&key, value)) << " "
              << leaf_sub_items_t::estimate_insert_one(value) << std::endl;
    std::cout << "LeafNode3: "
              << f_sum(LeafNode3::estimate_insert_one(&key, value)) << std::endl;
    std::cout << std::endl;
  }

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

  /*************** tree tests ***************/
  auto& btree = Btree::get();
  auto onodes = Onodes(15);
  auto f_validate_cursor = [] (const Btree::Cursor& cursor, const onode_t& onode) {
    assert(!cursor.is_end());
    assert(cursor.value());
    assert(cursor.value()->size == onode.size);
    Onodes::validate(*cursor.value());
  };

  // in-node insertion
  {
    auto key_s = onode_key_t{0, 0, 0, "ns", "oid", 0, 0};
    auto key_e = onode_key_t{std::numeric_limits<shard_t>::max(), 0, 0, "ns", "oid", 0, 0};
    assert(btree.find(key_s).is_end());
    assert(btree.begin().is_end());
    assert(btree.last().is_end());

    auto f_validate_insert_new =
        [&btree, &f_validate_cursor] (const onode_key_t& key, const onode_t& value) {
      auto [cursor, ret] = btree.insert(key, value);
      assert(ret == true);
      f_validate_cursor(cursor, value);
      auto cursor_ = btree.lower_bound(key);
      assert(cursor_.value() == cursor.value());
      return cursor.value();
    };

    // insert key1, onode1 at STAGE_LEFT
    auto key1 = onode_key_t{3, 3, 3, "ns3", "oid3", 3, 3};
    auto& onode1 = onodes.pick();
    auto p_value1 = f_validate_insert_new(key1, onode1);

    // validate lookup
    auto cursor1_s = btree.lower_bound(key_s);
    assert(cursor1_s.value() == p_value1);
    auto cursor1_e = btree.lower_bound(key_e);
    assert(cursor1_e.is_end());

    // insert the same key1 with a different onode
    auto& onode1_dup = onodes.pick();
    auto [cursor1_dup, ret1_dup] = btree.insert(key1, onode1_dup);
    assert(ret1_dup == false);
    f_validate_cursor(cursor1_dup, onode1);

    // insert key2, onode2 to key1's left at STAGE_LEFT
    // insert node front at STAGE_LEFT
    auto key2 = onode_key_t{2, 2, 2, "ns3", "oid3", 3, 3};
    auto& onode2 = onodes.pick();
    f_validate_insert_new(key2, onode2);

    // insert key3, onode3 to key1's right at STAGE_LEFT
    // insert node last at STAGE_LEFT
    auto key3 = onode_key_t{4, 4, 4, "ns3", "oid3", 3, 3};
    auto& onode3 = onodes.pick();
    f_validate_insert_new(key3, onode3);

    // insert key4, onode4 to key1's left at STAGE_STRING (collision)
    auto key4 = onode_key_t{3, 3, 3, "ns2", "oid2", 3, 3};
    auto& onode4 = onodes.pick();
    f_validate_insert_new(key4, onode4);

    // insert key5, onode5 to key1's right at STAGE_STRING (collision)
    auto key5 = onode_key_t{3, 3, 3, "ns4", "oid4", 3, 3};
    auto& onode5 = onodes.pick();
    f_validate_insert_new(key5, onode5);

    // insert key6, onode6 to key1's left at STAGE_RIGHT
    auto key6 = onode_key_t{3, 3, 3, "ns3", "oid3", 2, 2};
    auto& onode6 = onodes.pick();
    f_validate_insert_new(key6, onode6);

    // insert key7, onode7 to key1's right at STAGE_RIGHT
    auto key7 = onode_key_t{3, 3, 3, "ns3", "oid3", 4, 4};
    auto& onode7 = onodes.pick();
    f_validate_insert_new(key7, onode7);

    // insert node front at STAGE_RIGHT
    auto key8 = onode_key_t{2, 2, 2, "ns3", "oid3", 2, 2};
    auto& onode8 = onodes.pick();
    f_validate_insert_new(key8, onode8);

    // insert node front at STAGE_STRING (collision)
    auto key9 = onode_key_t{2, 2, 2, "ns2", "oid2", 3, 3};
    auto& onode9 = onodes.pick();
    f_validate_insert_new(key9, onode9);

    // insert node last at STAGE_RIGHT
    auto key10 = onode_key_t{4, 4, 4, "ns3", "oid3", 4, 4};
    auto& onode10 = onodes.pick();
    f_validate_insert_new(key10, onode10);

    // insert node last at STAGE_STRING (collision)
    auto key11 = onode_key_t{4, 4, 4, "ns4", "oid4", 3, 3};
    auto& onode11 = onodes.pick();
    f_validate_insert_new(key11, onode11);

    // insert key, value randomly until a perfect 3-ary tree is formed
    std::vector<std::pair<onode_key_t, const onode_t*>> kvs{
      {onode_key_t{2, 2, 2, "ns2", "oid2", 2, 2}, &onodes.pick()},
      {onode_key_t{2, 2, 2, "ns2", "oid2", 4, 4}, &onodes.pick()},
      {onode_key_t{2, 2, 2, "ns3", "oid3", 4, 4}, &onodes.pick()},
      {onode_key_t{2, 2, 2, "ns4", "oid4", 2, 2}, &onodes.pick()},
      {onode_key_t{2, 2, 2, "ns4", "oid4", 3, 3}, &onodes.pick()},
      {onode_key_t{2, 2, 2, "ns4", "oid4", 4, 4}, &onodes.pick()},
      {onode_key_t{3, 3, 3, "ns2", "oid2", 2, 2}, &onodes.pick()},
      {onode_key_t{3, 3, 3, "ns2", "oid2", 4, 4}, &onodes.pick()},
      {onode_key_t{3, 3, 3, "ns4", "oid4", 2, 2}, &onodes.pick()},
      {onode_key_t{3, 3, 3, "ns4", "oid4", 4, 4}, &onodes.pick()},
      {onode_key_t{4, 4, 4, "ns2", "oid2", 2, 2}, &onodes.pick()},
      {onode_key_t{4, 4, 4, "ns2", "oid2", 3, 3}, &onodes.pick()},
      {onode_key_t{4, 4, 4, "ns2", "oid2", 4, 4}, &onodes.pick()},
      {onode_key_t{4, 4, 4, "ns3", "oid3", 2, 2}, &onodes.pick()},
      {onode_key_t{4, 4, 4, "ns4", "oid4", 2, 2}, &onodes.pick()},
      {onode_key_t{4, 4, 4, "ns4", "oid4", 4, 4}, &onodes.pick()}};
    auto& smallest_value = *kvs[0].second;
    auto& largest_value = *kvs[kvs.size() - 1].second;
    std::random_shuffle(kvs.begin(), kvs.end());
    std::for_each(kvs.begin(), kvs.end(), [&f_validate_insert_new] (auto& kv) {
      f_validate_insert_new(kv.first, *kv.second);
    });
    assert(btree.height() == 1);

    // validate values keep intact
    auto f_validate_kv =
        [&btree, &f_validate_cursor] (const onode_key_t& key, const onode_t& value) {
      auto cursor = btree.lower_bound(key);
      f_validate_cursor(cursor, value);
    };
    f_validate_kv(key1, onode1);
    f_validate_kv(key2, onode2);
    f_validate_kv(key3, onode3);
    f_validate_kv(key4, onode4);
    f_validate_kv(key5, onode5);
    f_validate_kv(key6, onode6);
    f_validate_kv(key7, onode7);
    f_validate_kv(key8, onode8);
    f_validate_kv(key9, onode9);
    f_validate_kv(key10, onode10);
    f_validate_kv(key11, onode11);
    std::for_each(kvs.begin(), kvs.end(), [&f_validate_kv] (auto& kv) {
      f_validate_kv(kv.first, *kv.second);
    });
    f_validate_kv(key_s, smallest_value);
    f_validate_cursor(btree.begin(), smallest_value);
    f_validate_cursor(btree.last(), largest_value);

    btree.dump(std::cout) << std::endl;

    // TODO: better coverage to validate left part and right part won't
    // crisscross.

    btree.insert(onode_key_t{1, 1, 1, "ns3", "oid3", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{2, 2, 2, "ns1", "oid1", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{2, 2, 2, "ns2", "oid2", 1, 1}, onodes.create(1280));
    std::cout << std::endl;

    btree.insert(onode_key_t{2, 2, 2, "ns4", "oid4", 5, 5}, onodes.create(1280));
    btree.insert(onode_key_t{2, 2, 2, "ns5", "oid5", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{2, 3, 3, "ns3", "oid3", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{3, 3, 3, "ns2", "oid2", 1, 1}, onodes.create(1280));
    btree.insert(onode_key_t{3, 3, 3, "ns1", "oid1", 3, 3}, onodes.create(1280));
    std::cout << std::endl;

    btree.insert(onode_key_t{3, 3, 3, "ns4", "oid4", 5, 5}, onodes.create(1280));
    btree.insert(onode_key_t{3, 3, 3, "ns5", "oid5", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{3, 4, 4, "ns3", "oid3", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{4, 4, 4, "ns2", "oid2", 1, 1}, onodes.create(1280));
    btree.insert(onode_key_t{4, 4, 4, "ns1", "oid1", 3, 3}, onodes.create(1280));
    std::cout << std::endl;

    btree.insert(onode_key_t{4, 4, 4, "ns4", "oid4", 5, 5}, onodes.create(1280));
    btree.insert(onode_key_t{4, 4, 4, "ns5", "oid5", 3, 3}, onodes.create(1280));
    btree.insert(onode_key_t{5, 5, 5, "ns3", "oid3", 3, 3}, onodes.create(1280));
    std::cout << std::endl;
  }

  transaction_manager.free_all();
}
