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

#include "global/signal_handler.h"

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
  mutable std::random_device rd;
  std::vector<const onode_t*> onodes;
  std::vector<onode_t*> tracked_onodes;
};

// key view will be valid until the next build
static key_view_t build_key_view(const onode_key_t& hobj) {
  key_hobj_t key_hobj(hobj);
  key_view_t key_view;
  static auto extent = get_transaction_manager().alloc_extent(NODE_BLOCK_SIZE);
  char* p_fill = reinterpret_cast<char*>(extent->get_laddr() + NODE_BLOCK_SIZE);

  auto spc = shard_pool_crush_t::from_key<KeyT::HOBJ>(key_hobj);
  p_fill -= sizeof(shard_pool_crush_t);
  extent->copy_in_mem(spc, p_fill);
  key_view.set(*reinterpret_cast<const shard_pool_crush_t*>(p_fill));

  auto p_ns_oid = p_fill;
  ns_oid_view_t::append<KeyT::HOBJ>(*extent, key_hobj, p_fill);
  ns_oid_view_t ns_oid_view(p_ns_oid);
  key_view.set(ns_oid_view);

  auto sg = snap_gen_t::from_key<KeyT::HOBJ>(key_hobj);
  p_fill -= sizeof(snap_gen_t);
  extent->copy_in_mem(sg, p_fill);
  key_view.set(*reinterpret_cast<const snap_gen_t*>(p_fill));

  return key_view;
}

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
    std::cout << "sizes of a key-value insertion (full-string):" << std::endl;
    std::cout << "s-p-c, 'n'-'o', s-g => onode_t{2}: typically internal 41B, leaf 35B" << std::endl;
    onode_key_t hobj = {0, 0, 0, "n", "o", 0, 0};
    onode_t value = {2};

    key_hobj_t key(hobj);
    auto key_view = build_key_view(hobj);

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

  // leaf insertion
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

    btree.dump(std::cout) << std::endl << std::endl;

    // FIXME: better coverage to validate left part and right part won't
    // crisscross.
  }

  {
    // internal node insertion
    class DummyChild final : public Node {
     public:
      virtual ~DummyChild() = default;

      void set_root_ref(Ref<Node>& root_ref) {
        root_ref = this;
        p_root_ref = &root_ref;
      }

      bool can_split() const { return keys.size() > 1; }

      Ref<DummyChild> populate_split() {
        assert(can_split());
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

        keys = left_keys;
        bool right_is_tail = _is_level_tail;
        _is_level_tail = false;
        auto right_child = DummyChild::create(right_keys, right_is_tail);

        parent_info().ptr->apply_child_split(get_largest_key_view(), this, right_child);
        parent_info().ptr->dump(std::cout) << std::endl;

        return right_child;
      }

      static Ref<DummyChild> create(
          const std::set<onode_key_t>& keys,
          bool is_level_tail) {
        static laddr_t seed = 0;
        return new DummyChild(keys, is_level_tail, seed++);
      }

     protected:
      void as_child(const parent_info_t& info) override {
        assert(!p_root_ref);
        _parent_info = info;
      }
      void as_root(Ref<Node>& ref) override { assert(false); }
      void handover_root(Ref<InternalNode> root) override {
        assert(p_root_ref);
        root->as_root(*p_root_ref);
        p_root_ref = nullptr;
      }
      bool is_root() const override { return p_root_ref != nullptr; }
      const parent_info_t& parent_info() const override { return *_parent_info; }
      bool is_level_tail() const override { return _is_level_tail; }
      field_type_t field_type() const override { return field_type_t::N0; }
      laddr_t laddr() const override { return _laddr; }
      level_t level() const override { return 0u; }
      key_view_t get_key_view(const search_position_t&) const override { assert(false); }
      key_view_t get_largest_key_view() const override {
        return build_key_view(*keys.crbegin());
      }
      std::ostream& dump(std::ostream&) const override { assert(false); }
      std::ostream& dump_brief(std::ostream&) const override { assert(false); }
      Ref<Node> test_clone(Ref<Node>&) const override { assert(false); }
      void init(Ref<LogicalCachedExtent>, bool) override { assert(false); }
      Node::search_result_t do_lower_bound(
          const key_hobj_t&, MatchHistory&) override { assert(false); }
      Ref<tree_cursor_t> lookup_smallest() override { assert(false); }
      Ref<tree_cursor_t> lookup_largest() override { assert(false); }

     private:
      DummyChild(const std::set<onode_key_t>& keys,
                 bool is_level_tail, laddr_t laddr)
        : keys{keys}, _is_level_tail{is_level_tail}, _laddr{laddr} {}

      mutable std::random_device rd;
      std::optional<parent_info_t> _parent_info;
      Ref<Node>* p_root_ref = nullptr;
      std::set<onode_key_t> keys;
      bool _is_level_tail;
      laddr_t _laddr;

     friend class ChildPool;
    };

    class ChildPool {
     public:
      ChildPool(Ref<DummyChild> initial) {
        assert(initial->can_split());
        splitable_children.insert(initial);
      }

      bool can_split() const { return splitable_children.size(); }
      void populate_split() {
        auto index = rd() % splitable_children.size();
        auto iter = splitable_children.begin();
        std::advance(iter, index);
        Ref<DummyChild> child = *iter;
        auto new_child = child->populate_split();
        if (!child->can_split()) {
          splitable_children.erase(child);
        }
        if (new_child->can_split()) {
          splitable_children.insert(new_child);
        }
      }

     private:
      mutable std::random_device rd;
      std::set<Ref<DummyChild>> splitable_children;
    };

    // TODO: build the combination by parameters
    std::set<onode_key_t> keys{
      onode_key_t{2, 2, 2, "ns2", "oid2", 2, 2},
      onode_key_t{2, 2, 2, "ns2", "oid2", 3, 3},
      onode_key_t{2, 2, 2, "ns2", "oid2", 4, 4},
      onode_key_t{2, 2, 2, "ns3", "oid3", 2, 2},
      onode_key_t{2, 2, 2, "ns3", "oid3", 3, 3},
      onode_key_t{2, 2, 2, "ns3", "oid3", 4, 4},
      onode_key_t{2, 2, 2, "ns4", "oid4", 2, 2},
      onode_key_t{2, 2, 2, "ns4", "oid4", 3, 3},
      onode_key_t{2, 2, 2, "ns4", "oid4", 4, 4},
      onode_key_t{3, 3, 3, "ns2", "oid2", 2, 2},
      onode_key_t{3, 3, 3, "ns2", "oid2", 3, 3},
      onode_key_t{3, 3, 3, "ns2", "oid2", 4, 4},
      onode_key_t{3, 3, 3, "ns3", "oid3", 2, 2},
      onode_key_t{3, 3, 3, "ns3", "oid3", 3, 3},
      onode_key_t{3, 3, 3, "ns3", "oid3", 4, 4},
      onode_key_t{3, 3, 3, "ns4", "oid4", 2, 2},
      onode_key_t{3, 3, 3, "ns4", "oid4", 3, 3},
      onode_key_t{3, 3, 3, "ns4", "oid4", 4, 4},
      onode_key_t{4, 4, 4, "ns2", "oid2", 2, 2},
      onode_key_t{4, 4, 4, "ns2", "oid2", 3, 3},
      onode_key_t{4, 4, 4, "ns2", "oid2", 4, 4},
      onode_key_t{4, 4, 4, "ns3", "oid3", 2, 2},
      onode_key_t{4, 4, 4, "ns3", "oid3", 3, 3},
      onode_key_t{4, 4, 4, "ns3", "oid3", 4, 4},
      onode_key_t{4, 4, 4, "ns4", "oid4", 2, 2},
      onode_key_t{4, 4, 4, "ns4", "oid4", 3, 3},
      onode_key_t{4, 4, 4, "ns4", "oid4", 4, 4},
      onode_key_t{9, 9, 9, "ns~last", "oid~last", 9, 9}
    };

    Ref<Node> root_ref;
    auto initial_child = DummyChild::create(keys, true);
    initial_child->set_root_ref(root_ref);
    InternalNode0::upgrade_root(root_ref);
    ChildPool pool(initial_child);
    while (pool.can_split()) {
      pool.populate_split();
    }

    return 0;
  }

  // in-node split
  {
    Ref<Node> root;
    LeafNode0::allocate_root(root);

    // insert key, value randomly until a perfect 3-ary tree is formed
    onode_key_t key;
    for (unsigned i = 2; i <= 4; ++i) {
      for (unsigned j = 2; j <= 4; ++j) {
        for (unsigned k = 2; k <= 4; ++k) {
          key.shard = i;
          key.pool = i;
          key.crush = i;
          std::ostringstream os_ns;
          os_ns << "ns" << j;
          key.nspace = os_ns.str();
          std::ostringstream os_oid;
          os_oid << "oid" << j;
          key.oid = os_oid.str();
          key.snap = k;
          key.gen = k;

          auto [p_cursor, success] = root->insert(key, onodes.pick_largest());
          assert(success == true);
          assert(p_cursor->get_leaf_node() == root);
          assert(p_cursor->get_p_value());
          Onodes::validate(*p_cursor->get_p_value());
        }
      }
    }
    root->dump(std::cout) << std::endl << std::endl;

    auto f_split = [&root] (const onode_key_t& key, const onode_t& value) {
      Ref<Node> dummy_root;
      auto node = root->test_clone(dummy_root);
      std::cout << "insert " << key << ":" << std::endl;
      auto [p_cursor, success] = node->insert(key, value);
      assert(success);
      assert(p_cursor->get_p_value());
      assert(p_cursor->get_p_value() != &value);
      assert(p_cursor->get_p_value()->size == value.size);
      Onodes::validate(*p_cursor->get_p_value());
      dummy_root->dump(std::cout) << std::endl;
      std::cout << std::endl;
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
  }

  get_transaction_manager().free_all();
}
