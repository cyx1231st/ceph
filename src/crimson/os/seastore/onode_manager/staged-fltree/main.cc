// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <vector>

#include "dummy_transaction_manager.h"
#include "node_impl.h"
#include "stages/node_stage.h"
#include "stages/stage.h"
#include "tree.h"

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
    p_onode->id = id++;
    p_mem += (size - sizeof(uint32_t));
    std::memcpy(p_mem, &target, sizeof(uint32_t));
    validate(*p_onode);
    return *p_onode;
  }

  const onode_t& pick() const {
    auto index = rd() % onodes.size();
    return *onodes[index];
  }

  const onode_t& pick_largest() const {
    return *onodes[onodes.size() - 1];
  }

  static void validate(const onode_t& node) {
    auto p_target = (const char*)&node + node.size - sizeof(uint32_t);
    uint32_t target;
    std::memcpy(&target, p_target, sizeof(uint32_t));
    assert(target == node.size * 137);
  }

 private:
  uint16_t id = 0;
  mutable std::random_device rd;
  std::vector<const onode_t*> onodes;
  std::vector<onode_t*> tracked_onodes;
};

static std::set<onode_key_t> build_key_set(
    std::pair<unsigned, unsigned> range_2,
    std::pair<unsigned, unsigned> range_1,
    std::pair<unsigned, unsigned> range_0,
    std::string padding = "",
    bool is_internal = false) {
  std::set<onode_key_t> ret;
  onode_key_t key;
  for (unsigned i = range_2.first; i < range_2.second; ++i) {
    for (unsigned j = range_1.first; j < range_1.second; ++j) {
      for (unsigned k = range_0.first; k < range_0.second; ++k) {
        key.shard = i;
        key.pool = i;
        key.crush = i;
        std::ostringstream os_ns;
        os_ns << "ns" << j;
        key.nspace = os_ns.str();
        std::ostringstream os_oid;
        os_oid << "oid" << j << padding;
        key.oid = os_oid.str();
        key.snap = k;
        key.gen = k;
        ret.insert(key);
      }
    }
  }
  if (is_internal) {
    ret.insert(onode_key_t{9, 9, 9, "ns~last", "oid~last", 9, 9});
  }
  return ret;
}

static std::pair<key_view_t, void*> build_key_view(const onode_key_t& hobj) {
  key_hobj_t key_hobj(hobj);
  size_t key_size = sizeof(shard_pool_crush_t) + sizeof(snap_gen_t) +
                    ns_oid_view_t::estimate_size<KeyT::HOBJ>(key_hobj);
  void* p_mem = std::malloc(key_size);

  key_view_t key_view;
  char* p_fill = (char*)p_mem + key_size;

  auto spc = shard_pool_crush_t::from_key<KeyT::HOBJ>(key_hobj);
  p_fill -= sizeof(shard_pool_crush_t);
  std::memcpy(p_fill, &spc, sizeof(shard_pool_crush_t));
  key_view.set(*reinterpret_cast<const shard_pool_crush_t*>(p_fill));

  auto p_ns_oid = p_fill;
  ns_oid_view_t::append<KeyT::HOBJ>(key_hobj, p_fill);
  ns_oid_view_t ns_oid_view(p_ns_oid);
  key_view.set(ns_oid_view);

  auto sg = snap_gen_t::from_key<KeyT::HOBJ>(key_hobj);
  p_fill -= sizeof(snap_gen_t);
  assert(p_fill == (char*)p_mem);
  std::memcpy(p_fill, &sg, sizeof(snap_gen_t));
  key_view.set(*reinterpret_cast<const snap_gen_t*>(p_fill));

  return {key_view, p_mem};
}

int main(int argc, char* argv[])
{
  // TODO: move to unit tests

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
    std::cout << "sizes of a key-value insertion (full-string):" << std::endl;
    std::cout << "s-p-c, 'n'-'o', s-g => onode_t{2}: typically internal 41B, leaf 35B" << std::endl;
    onode_key_t hobj = {0, 0, 0, "n", "o", 0, 0};
    onode_t value = {2};

    key_hobj_t key(hobj);
    auto [key_view, p_mem] = build_key_view(hobj);

#define STAGE_T(NodeType) node_to_stage_t<typename NodeType::node_stage_t>
#define NXT_T(StageType)  staged<typename StageType::next_param_t>

    std::cout << "InternalNode0: "
              << STAGE_T(InternalNode0)::template
                 insert_size<KeyT::VIEW>(key_view, 0) << " "
              << NXT_T(STAGE_T(InternalNode0))::template
                 insert_size<KeyT::VIEW>(key_view, 0) << " "
              << NXT_T(NXT_T(STAGE_T(InternalNode0)))::template
                 insert_size<KeyT::VIEW>(key_view, 0) << std::endl;
    std::cout << "InternalNode1: "
              << STAGE_T(InternalNode1)::template
                 insert_size<KeyT::VIEW>(key_view, 0) << " "
              << NXT_T(STAGE_T(InternalNode1))::template
                 insert_size<KeyT::VIEW>(key_view, 0) << " "
              << NXT_T(NXT_T(STAGE_T(InternalNode1)))::template
                 insert_size<KeyT::VIEW>(key_view, 0) << std::endl;
    std::cout << "InternalNode2: "
              << STAGE_T(InternalNode2)::template
                 insert_size<KeyT::VIEW>(key_view, 0) << " "
              << NXT_T(STAGE_T(InternalNode2))::template
                 insert_size<KeyT::VIEW>(key_view, 0) << std::endl;
    std::cout << "InternalNode3: "
              << STAGE_T(InternalNode3)::template
                 insert_size<KeyT::VIEW>(key_view, 0) << std::endl;

    std::cout << "LeafNode0: "
              << STAGE_T(LeafNode0)::template
                 insert_size<KeyT::HOBJ>(key, value) << " "
              << NXT_T(STAGE_T(LeafNode0))::template
                 insert_size<KeyT::HOBJ>(key, value) << " "
              << NXT_T(NXT_T(STAGE_T(LeafNode0)))::template
                 insert_size<KeyT::HOBJ>(key, value) << std::endl;
    std::cout << "LeafNode1: "
              << STAGE_T(LeafNode1)::template
                 insert_size<KeyT::HOBJ>(key, value) << " "
              << NXT_T(STAGE_T(LeafNode1))::template
                 insert_size<KeyT::HOBJ>(key, value) << " "
              << NXT_T(NXT_T(STAGE_T(LeafNode1)))::template
                 insert_size<KeyT::HOBJ>(key, value) << std::endl;
    std::cout << "LeafNode2: "
              << STAGE_T(LeafNode2)::template
                 insert_size<KeyT::HOBJ>(key, value) << " "
              << NXT_T(STAGE_T(LeafNode2))::template
                 insert_size<KeyT::HOBJ>(key, value) << std::endl;
    std::cout << "LeafNode3: "
              << STAGE_T(LeafNode3)::template
                 insert_size<KeyT::HOBJ>(key, value) << std::endl;
    std::cout << std::endl;

    std::free(p_mem);
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
  std::vector<Ref<InternalNode>> internal_nodes = {
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
    node->test_make_destructable();
  }
  std::cout << std::endl;

  /*************** tree tests ***************/
  auto f_validate_cursor = [] (const Btree::Cursor& cursor, const onode_t& onode) {
    assert(!cursor.is_end());
    assert(cursor.value());
    assert(cursor.value() != &onode);
    assert(*cursor.value() == onode);
    Onodes::validate(*cursor.value());
  };
  auto onodes = Onodes(15);

  // leaf node insert
  {
    Ref<Btree> p_btree = new Btree();
    Btree& btree = *p_btree.get();
    btree.mkfs();

    std::cout << "---------------------------------------------\n"
              << "randomized leaf node insert:"
              << std::endl << std::endl;
    auto key_s = onode_key_t{0, 0, 0, "ns", "oid", 0, 0};
    auto key_e = onode_key_t{std::numeric_limits<shard_t>::max(), 0, 0, "ns", "oid", 0, 0};
    assert(btree.find(key_s).is_end());
    assert(btree.begin().is_end());
    assert(btree.last().is_end());

    std::vector<std::tuple<onode_key_t,
                           const onode_t*,
                           Btree::Cursor>> insert_history;
    auto f_validate_insert_new = [&btree, &f_validate_cursor, &insert_history] (
        const onode_key_t& key, const onode_t& value) {
      auto [cursor, success] = btree.insert(key, value);
      assert(success == true);
      insert_history.emplace_back(key, &value, cursor);
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
    {
      auto cursor1_s = btree.lower_bound(key_s);
      assert(cursor1_s.value() == p_value1);
      auto cursor1_e = btree.lower_bound(key_e);
      assert(cursor1_e.is_end());
    }

    // insert the same key1 with a different onode
    {
      auto& onode1_dup = onodes.pick();
      auto [cursor1_dup, ret1_dup] = btree.insert(key1, onode1_dup);
      assert(ret1_dup == false);
      f_validate_cursor(cursor1_dup, onode1);
    }

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
    assert(!btree.test_is_clean());

    for (auto& [k, v, c] : insert_history) {
      // validate values in tree keep intact
      auto cursor = btree.lower_bound(k);
      f_validate_cursor(cursor, *v);
      // validate values in cursors keep intact
      f_validate_cursor(c, *v);
    }
    f_validate_cursor(btree.lower_bound(key_s), smallest_value);
    f_validate_cursor(btree.begin(), smallest_value);
    f_validate_cursor(btree.last(), largest_value);

    btree.dump(std::cout) << std::endl << std::endl;

    insert_history.clear();
    assert(btree.test_is_clean());

    // FIXME: better coverage to validate left part and right part won't
    // crisscross.
  }

  // leaf node split
  {
    Ref<Btree> p_btree = new Btree();
    Btree& btree = *p_btree.get();
    btree.mkfs();

    std::cout << "---------------------------------------------\n"
              << "before leaf node split:"
              << std::endl << std::endl;
    auto keys = build_key_set({2, 5}, {2, 5}, {2, 5});
    std::vector<std::tuple<onode_key_t,
                           const onode_t*,
                           Btree::Cursor>> insert_history;
    for (auto& key : keys) {
      auto& value = onodes.create(120);
      auto [cursor, success] = btree.insert(key, value);
      assert(success == true);
      f_validate_cursor(cursor, value);
      insert_history.emplace_back(key, &value, cursor);
    }
    assert(btree.height() == 1);
    assert(!btree.test_is_clean());
    btree.dump(std::cout) << std::endl << std::endl;

    auto f_split = [&btree, &insert_history, &f_validate_cursor] (
        const onode_key_t& key, const onode_t& value) {
      Ref<Btree> p_btree_clone = new Btree();
      Btree& btree_clone = *p_btree_clone.get();
      btree_clone.test_clone_from(btree);
      std::cout << "insert " << key << ":" << std::endl;
      auto [cursor, success] = btree_clone.insert(key, value);
      assert(success == true);
      f_validate_cursor(cursor, value);
      btree_clone.dump(std::cout) << std::endl << std::endl;
      assert(btree_clone.height() == 2);

      for (auto& [k, v, c] : insert_history) {
        auto result = btree_clone.lower_bound(k);
        f_validate_cursor(result, *v);
      }
      auto result = btree_clone.lower_bound(key);
      f_validate_cursor(result, value);
    };
    auto& onode = onodes.create(1280);

    std::cout << "---------------------------------------------\n"
              << "split at stage 2; insert to left front at stage 2, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{1, 1, 1, "ns3", "oid3", 3, 3}, onode);
    f_split(onode_key_t{2, 2, 2, "ns1", "oid1", 3, 3}, onode);
    f_split(onode_key_t{2, 2, 2, "ns2", "oid2", 1, 1}, onode);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 2; insert to left back at stage 0, 1, 2, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{2, 2, 2, "ns4", "oid4", 5, 5}, onode);
    f_split(onode_key_t{2, 2, 2, "ns5", "oid5", 3, 3}, onode);
    f_split(onode_key_t{2, 3, 3, "ns3", "oid3", 3, 3}, onode);
    f_split(onode_key_t{3, 3, 3, "ns1", "oid1", 3, 3}, onode);
    f_split(onode_key_t{3, 3, 3, "ns2", "oid2", 1, 1}, onode);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 2; insert to right front at stage 0, 1, 2, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns4", "oid4", 5, 5}, onode);
    f_split(onode_key_t{3, 3, 3, "ns5", "oid5", 3, 3}, onode);
    f_split(onode_key_t{3, 4, 4, "ns3", "oid3", 3, 3}, onode);
    f_split(onode_key_t{4, 4, 4, "ns1", "oid1", 3, 3}, onode);
    f_split(onode_key_t{4, 4, 4, "ns2", "oid2", 1, 1}, onode);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 2; insert to right back at stage 0, 1, 2"
              << std::endl << std::endl;
    f_split(onode_key_t{4, 4, 4, "ns4", "oid4", 5, 5}, onode);
    f_split(onode_key_t{4, 4, 4, "ns5", "oid5", 3, 3}, onode);
    f_split(onode_key_t{5, 5, 5, "ns3", "oid3", 3, 3}, onode);
    std::cout << std::endl;

    auto& onode1 = onodes.create(512);
    std::cout << "---------------------------------------------\n"
              << "split at stage 1; insert to left middle at stage 0, 1, 2, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{2, 2, 2, "ns4", "oid4", 5, 5}, onode1);
    f_split(onode_key_t{2, 2, 2, "ns5", "oid5", 3, 3}, onode1);
    f_split(onode_key_t{2, 2, 3, "ns3", "oid3", 3, 3}, onode1);
    f_split(onode_key_t{3, 3, 3, "ns1", "oid1", 3, 3}, onode1);
    f_split(onode_key_t{3, 3, 3, "ns2", "oid2", 1, 1}, onode1);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 1; insert to left back at stage 0, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns2", "oid2", 5, 5}, onode1);
    f_split(onode_key_t{3, 3, 3, "ns2", "oid3", 3, 3}, onode1);
    f_split(onode_key_t{3, 3, 3, "ns3", "oid3", 1, 1}, onode1);
    std::cout << std::endl;

    auto& onode2 = onodes.create(256);
    std::cout << "---------------------------------------------\n"
              << "split at stage 1; insert to right front at stage 0, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns3", "oid3", 5, 5}, onode2);
    f_split(onode_key_t{3, 3, 3, "ns3", "oid4", 3, 3}, onode2);
    f_split(onode_key_t{3, 3, 3, "ns4", "oid4", 1, 1}, onode2);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 1; insert to right middle at stage 0, 1, 2, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns4", "oid4", 5, 5}, onode2);
    f_split(onode_key_t{3, 3, 3, "ns5", "oid5", 3, 3}, onode2);
    f_split(onode_key_t{3, 3, 4, "ns3", "oid3", 3, 3}, onode2);
    f_split(onode_key_t{4, 4, 4, "ns1", "oid1", 3, 3}, onode2);
    f_split(onode_key_t{4, 4, 4, "ns2", "oid2", 1, 1}, onode2);
    std::cout << std::endl;

    auto& onode3 = onodes.create(768);
    std::cout << "---------------------------------------------\n"
              << "split at stage 0; insert to right middle at stage 0, 1, 2, 1, 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns4", "oid4", 5, 5}, onode3);
    f_split(onode_key_t{3, 3, 3, "ns5", "oid5", 3, 3}, onode3);
    f_split(onode_key_t{3, 3, 4, "ns3", "oid3", 3, 3}, onode3);
    f_split(onode_key_t{4, 4, 4, "ns1", "oid1", 3, 3}, onode3);
    f_split(onode_key_t{4, 4, 4, "ns2", "oid2", 1, 1}, onode3);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 0; insert to right front at stage 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns4", "oid4", 2, 3}, onode3);
    std::cout << std::endl;

    std::cout << "---------------------------------------------\n"
              << "split at stage 0; insert to left back at stage 0"
              << std::endl << std::endl;
    f_split(onode_key_t{3, 3, 3, "ns2", "oid2", 3, 4}, onode3);
    std::cout << std::endl;

    // TODO: test split at {0, 0, 0}
    // TODO: test split at {END, END, END}
  }

  class ChildPool {
    class DummyChild final : public Node {
     public:
      virtual ~DummyChild() {
        std::free(p_mem_key_view);
      }

      void populate_split(std::set<Ref<DummyChild>>& splitable_nodes) {
        assert(can_split());
        assert(splitable_nodes.find(this) != splitable_nodes.end());

        size_t index;
        if (keys.size() == 2) {
          index = 1;
        } else {
          index = rd() % (keys.size() - 2) + 1;
        }
        auto iter = keys.begin();
        std::advance(iter, index);

        std::set<onode_key_t> left_keys(keys.begin(), iter);
        std::set<onode_key_t> right_keys(iter, keys.end());
        bool right_is_tail = _is_level_tail;
        reset(left_keys, false);
        auto right_child = DummyChild::create(right_keys, right_is_tail, pool);
        this->insert_parent(right_child);

        if (!can_split()) {
          splitable_nodes.erase(this);
        }
        if (right_child->can_split()) {
          splitable_nodes.insert(right_child);
        }
      }

      void insert_and_split(const onode_key_t& insert_key) {
        assert(keys.size() == 1);
        auto& key = *keys.begin();
        assert(insert_key < key);

        std::set<onode_key_t> new_keys;
        new_keys.insert(insert_key);
        new_keys.insert(key);
        reset(new_keys, _is_level_tail);

        std::set<Ref<DummyChild>> splitable_nodes;
        splitable_nodes.insert(this);
        populate_split(splitable_nodes);
        assert(!splitable_nodes.size());
      }

      bool match_pos(const search_position_t& pos) const {
        assert(!is_root());
        return pos == parent_info().position;
      }

      static Ref<DummyChild> create(
          const std::set<onode_key_t>& keys, bool is_level_tail, ChildPool& pool) {
        static laddr_t seed = 0;
        return new DummyChild(keys, is_level_tail, seed++, pool);
      }

      static Ref<DummyChild> create_initial(
          const std::set<onode_key_t>& keys, ChildPool& pool, Ref<Btree> btree) {
        auto initial = create(keys, true, pool);
        initial->make_root(btree);
        initial->upgrade_root();
        return initial;
      }

      static Ref<DummyChild> create_clone(
          const std::set<onode_key_t>& keys, bool is_level_tail,
          laddr_t addr, ChildPool& pool) {
        return new DummyChild(keys, is_level_tail, addr, pool);
      }

     protected:
      bool is_level_tail() const override { return _is_level_tail; }
      field_type_t field_type() const override { return field_type_t::N0; }
      laddr_t laddr() const override { return _laddr; }
      level_t level() const override { return 0u; }
      key_view_t get_key_view(const search_position_t&) const override { assert(false); }
      key_view_t get_largest_key_view() const override { return key_view; }
      std::ostream& dump(std::ostream&) const override { assert(false); }
      std::ostream& dump_brief(std::ostream&) const override { assert(false); }
      void init(Ref<LogicalCachedExtent>, bool) override { assert(false); }
      Node::search_result_t do_lower_bound(
          const key_hobj_t&, MatchHistory&) override { assert(false); }
      Ref<tree_cursor_t> lookup_smallest() override { assert(false); }
      Ref<tree_cursor_t> lookup_largest() override { assert(false); }

      void test_make_destructable() override { assert(false); }
      void test_clone_root(Ref<Btree>) const override { assert(false); }
      void test_clone_non_root(Ref<InternalNode> new_parent) const override {
        assert(!is_root());
        auto p_pool_clone = pool.pool_clone_in_progress;
        assert(p_pool_clone);
        auto clone = create_clone(keys, _is_level_tail, _laddr, *p_pool_clone);
        clone->as_child(parent_info().position, new_parent);
        clone->_laddr = _laddr;
      }

     private:
      DummyChild(const std::set<onode_key_t>& keys,
                 bool is_level_tail, laddr_t laddr, ChildPool& pool)
          : keys{keys}, _is_level_tail{is_level_tail}, _laddr{laddr}, pool{pool} {
        std::tie(key_view, p_mem_key_view) = build_key_view(*keys.crbegin());
        pool.track_node(this);
      }

      bool can_split() const { return keys.size() > 1; }

      void reset(const std::set<onode_key_t>& _keys, bool level_tail) {
        keys = _keys;
        _is_level_tail = level_tail;
        std::free(p_mem_key_view);
        std::tie(key_view, p_mem_key_view) = build_key_view(*keys.crbegin());
      }

      mutable std::random_device rd;
      std::set<onode_key_t> keys;
      bool _is_level_tail;
      laddr_t _laddr;
      ChildPool& pool;

      key_view_t key_view;
      void* p_mem_key_view;
    };

   public:
    ChildPool() = default;
    ~ChildPool() { reset(); }

    void build_tree(const std::set<onode_key_t>& keys) {
      reset();

      // create tree
      p_btree = new Btree();
      auto initial_child = DummyChild::create_initial(keys, *this, p_btree);

      // split
      std::set<Ref<DummyChild>> splitable_nodes;
      splitable_nodes.insert(initial_child);
      std::random_device rd;
      while (splitable_nodes.size()) {
        auto index = rd() % splitable_nodes.size();
        auto iter = splitable_nodes.begin();
        std::advance(iter, index);
        Ref<DummyChild> child = *iter;
        child->populate_split(splitable_nodes);
      }
      p_btree->dump(std::cout) << std::endl << std::endl;
      assert(p_btree->height() == 2);
    }

    void test_split(const onode_key_t& key, search_position_t pos) {
      std::cout << "insert " << key << " at " << pos << ":" << std::endl;

      ChildPool pool_clone;
      pool_clone_in_progress = &pool_clone;
      pool_clone.p_btree = new Btree();
      pool_clone.p_btree->test_clone_from(*p_btree);
      pool_clone_in_progress = nullptr;

      auto node_to_split = pool_clone.get_node_by_pos(pos);
      node_to_split->insert_and_split(key);
      pool_clone.p_btree->dump(std::cout) << std::endl << std::endl;
      assert(pool_clone.p_btree->height() == 3);
    }

   private:
    void reset() {
      assert(!pool_clone_in_progress);
      if (tracked_children.size()) {
        assert(!p_btree->test_is_clean());
        tracked_children.clear();
        assert(p_btree->test_is_clean());
        p_btree.reset();
      } else {
        assert(!p_btree);
      }
    }

    void track_node(Ref<DummyChild> node) {
      assert(tracked_children.find(node) == tracked_children.end());
      tracked_children.insert(node);
    }

    Ref<DummyChild> get_node_by_pos(const search_position_t& pos) const {
      auto iter = std::find_if(
          tracked_children.begin(), tracked_children.end(), [&pos](auto& child) {
        return child->match_pos(pos);
      });
      assert(iter != tracked_children.end());
      return *iter;
    }

    std::set<Ref<DummyChild>> tracked_children;
    Ref<Btree> p_btree;

    ChildPool* pool_clone_in_progress = nullptr;
  };

  // internal node insert & split
  {
    ChildPool pool;
    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert:"
                << std::endl << std::endl;
      auto padding = std::string(250, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 2; insert to right front at stage 0, 1, 2, 1, 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns4", "oid4" + padding, 5, 5}, {2, {0, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns5", "oid5", 3, 3}, {2, {0, {0}}});
      pool.test_split(onode_key_t{3, 4, 4, "ns3", "oid3", 3, 3}, {2, {0, {0}}});
      pool.test_split(onode_key_t{4, 4, 4, "ns1", "oid1", 3, 3}, {2, {0, {0}}});
      pool.test_split(onode_key_t{4, 4, 4, "ns2", "oid2" + padding, 1, 1}, {2, {0, {0}}});
      std::cout << std::endl;

      std::cout << "---------------------------------------------\n"
                << "split at stage 2; insert to right middle at stage 0, 1, 2, 1, 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 5, 5}, {3, {0, {0}}});
      pool.test_split(onode_key_t{4, 4, 4, "ns5", "oid5", 3, 3}, {3, {0, {0}}});
      pool.test_split(onode_key_t{4, 4, 5, "ns3", "oid3", 3, 3}, {3, {0, {0}}});
      pool.test_split(onode_key_t{5, 5, 5, "ns1", "oid1", 3, 3}, {3, {0, {0}}});
      pool.test_split(onode_key_t{5, 5, 5, "ns2", "oid2" + padding, 1, 1}, {3, {0, {0}}});
      std::cout << std::endl;

      std::cout << "---------------------------------------------\n"
                << "split at stage 2; insert to right back at stage 0, 1, 2"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{5, 5, 5, "ns4", "oid4" + padding, 5, 5}, search_position_t::end());
      pool.test_split(onode_key_t{5, 5, 5, "ns5", "oid5", 3, 3}, search_position_t::end());
      pool.test_split(onode_key_t{6, 6, 6, "ns3", "oid3", 3, 3}, search_position_t::end());
      std::cout << std::endl;

      std::cout << "---------------------------------------------\n"
                << "split at stage 0; insert to left front at stage 2, 1, 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{1, 1, 1, "ns3", "oid3", 3, 3}, {0, {0, {0}}});
      pool.test_split(onode_key_t{2, 2, 2, "ns1", "oid1", 3, 3}, {0, {0, {0}}});
      pool.test_split(onode_key_t{2, 2, 2, "ns2", "oid2" + padding, 1, 1}, {0, {0, {0}}});
      std::cout << std::endl;

      std::cout << "---------------------------------------------\n"
                << "split at stage 0/1; insert to left middle at stage 0, 1, 2, 1, 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{2, 2, 2, "ns4", "oid4" + padding, 5, 5}, {1, {0, {0}}});
      pool.test_split(onode_key_t{2, 2, 2, "ns5", "oid5", 3, 3}, {1, {0, {0}}});
      pool.test_split(onode_key_t{2, 2, 3, "ns3", "oid3" + std::string(80, '_'), 3, 3}, {1, {0, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns1", "oid1", 3, 3}, {1, {0, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns2", "oid2" + padding, 1, 1}, {1, {0, {0}}});
      std::cout << std::endl;

      std::cout << "---------------------------------------------\n"
                << "split at stage 0; insert to left back at stage 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns4", "oid4" + padding, 3, 4}, {1, {2, {2}}});
    }

    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert (1):"
                << std::endl << std::endl;
      auto padding = std::string(245, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      keys.insert(onode_key_t{5, 5, 5, "ns4", "oid4" + padding, 5, 5});
      keys.insert(onode_key_t{5, 5, 5, "ns4", "oid4" + padding, 6, 6});
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 2; insert to left back at stage 0, 1, 2, 1"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns4", "oid4" + padding, 5, 5}, {2, {0, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns5", "oid5", 3, 3}, {2, {0, {0}}});
      pool.test_split(onode_key_t{3, 4, 4, "n", "o", 3, 3}, {2, {0, {0}}});
      pool.test_split(onode_key_t{4, 4, 4, "n", "o", 3, 3}, {2, {0, {0}}});
      std::cout << std::endl;

      std::cout << "---------------------------------------------\n"
                << "split at stage 2; insert to left middle at stage 2"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{2, 3, 3, "n", "o", 3, 3}, {1, {0, {0}}});
      std::cout << std::endl;
    }

    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert (2):"
                << std::endl << std::endl;
      auto padding = std::string(245, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      keys.insert(onode_key_t{4, 4, 4, "n", "o", 3, 3});
      keys.insert(onode_key_t{5, 5, 5, "ns4", "oid4" + padding, 5, 5});
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 2; insert to left back at stage (0, 1, 2, 1,) 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{4, 4, 4, "n", "o", 2, 2}, {2, {0, {0}}});
      std::cout << std::endl;
    }

    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert (3):"
                << std::endl << std::endl;
      auto padding = std::string(417, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding, true);
      keys.insert(onode_key_t{4, 4, 4, "ns3", "oid3" + padding, 5, 5});
      keys.erase(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 2, 2});
      keys.erase(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 3, 3});
      keys.erase(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 4, 4});
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 1; insert to right front at stage 0, 1, 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns2", "oid2" + padding, 5, 5}, {1, {1, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns2", "oid3", 3, 3}, {1, {1, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns3", "oid3" + padding, 1, 1}, {1, {1, {0}}});
      std::cout << std::endl;
    }

    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert (4):"
                << std::endl << std::endl;
      auto padding = std::string(360, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding, true);
      keys.insert(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 5, 5});
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 1; insert to left back at stage 0, 1"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns2", "oid2" + padding, 5, 5}, {1, {1, {0}}});
      pool.test_split(onode_key_t{3, 3, 3, "ns2", "oid3", 3, 3}, {1, {1, {0}}});
      std::cout << std::endl;
    }

    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert (5):"
                << std::endl << std::endl;
      auto padding = std::string(412, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding);
      keys.insert(onode_key_t{3, 3, 3, "ns2", "oid3", 3, 3});
      keys.insert(onode_key_t{4, 4, 4, "ns3", "oid3" + padding, 5, 5});
      keys.insert(onode_key_t{9, 9, 9, "ns~last", "oid~last", 9, 9});
      keys.erase(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 2, 2});
      keys.erase(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 3, 3});
      keys.erase(onode_key_t{4, 4, 4, "ns4", "oid4" + padding, 4, 4});
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 1; insert to left back at stage (0, 1,) 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns2", "oid3", 2, 2}, {1, {1, {0}}});
      std::cout << std::endl;
    }

    {
      std::cout << "---------------------------------------------\n"
                << "before internal node insert (6):"
                << std::endl << std::endl;
      auto padding = std::string(328, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding);
      keys.insert(onode_key_t{5, 5, 5, "ns3", "oid3" + std::string(271, '_'), 3, 3});
      keys.insert(onode_key_t{9, 9, 9, "ns~last", "oid~last", 9, 9});
      pool.build_tree(keys);

      std::cout << "---------------------------------------------\n"
                << "split at stage 0; insert to right front at stage 0"
                << std::endl << std::endl;
      pool.test_split(onode_key_t{3, 3, 3, "ns3", "oid3" + padding, 2, 3}, {1, {1, {1}}});
      std::cout << std::endl;
    }

    // TODO: test split at {0, 0, 0}
    // TODO: test split at {END, END, END}
  }

  get_transaction_manager().free_all();
}
