// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node.h"

#include <cassert>
#include <exception>

#include "dummy_transaction_manager.h"
#include "node_impl.h"

namespace crimson::os::seastore::onode {

tree_cursor_t::tree_cursor_t(
    Ref<LeafNode> node, const search_position_t& pos, const onode_t* p_value)
      : leaf_node{node}, position{pos}, p_value{p_value} {
  assert((!pos.is_end() && p_value) || (pos.is_end() && !p_value));
  if (!pos.is_end()) {
    assert(p_value == leaf_node->get_p_value(position));
    leaf_node->do_track_cursor(*this);
  }
}

tree_cursor_t::~tree_cursor_t() {
  if (!position.is_end()) {
    leaf_node->do_untrack_cursor(*this);
  }
}

void tree_cursor_t::update_track(
    Ref<LeafNode> node, const search_position_t& pos) {
  // already untracked
  assert(!pos.is_end());
  assert(!is_end());
  leaf_node = node;
  position = pos;
  // p_value must be already invalidated
  assert(!p_value);
  leaf_node->do_track_cursor(*this);
}

const onode_t* tree_cursor_t::get_p_value() const {
  assert(!is_end());
  if (!p_value) {
    // TODO: we decide to pin the extent of leaf node in order to read its
    // value synchronously.
    p_value = leaf_node->get_p_value(position);
  }
  assert(p_value);
  return p_value;
}

Node::search_result_t Node::lower_bound(const onode_key_t& _key) {
  MatchHistory history;
  full_key_t<KeyT::HOBJ> key(_key);
  return do_lower_bound(key, history);
}

std::pair<Ref<tree_cursor_t>, bool>
Node::insert(const onode_key_t& _key, const onode_t& value) {
  MatchHistory history;
  full_key_t<KeyT::HOBJ> key(_key);
  auto result = do_lower_bound(key, history);
  if (result.match == MatchKindBS::EQ) {
    return {result.p_cursor, false};
  } else {
    auto leaf_node = result.p_cursor->get_leaf_node();
    auto p_cursor = leaf_node->insert_value(
        key, value, result.p_cursor->get_position(), history);
    return {p_cursor, true};
  }
}

void Node::mkfs(/* transaction, */Ref<Btree> btree) {
  auto root = LeafNode0::allocate(true);
  root->make_root(/* transaction, */btree);
}

Ref<Node> Node::load_root(/* transaction, */Ref<Btree> btree) {
  auto super = btree->get_super_block(/* transaction */);
  auto root_addr = super->get_onode_root_laddr();
  assert(root_addr != L_ADDR_NULL);
  auto root = Node::load(root_addr, true);
  root->as_root(btree, super);
  return root;
}

Ref<Node> Node::load(laddr_t addr, bool is_level_tail) {
  // TODO: throw error if cannot read from address
  auto extent = get_transaction_manager().read_extent(addr);
  const auto header = extent->get_ptr<node_header_t>(0u);
  auto _field_type = header->get_field_type();
  if (!_field_type.has_value()) {
    throw std::runtime_error("load failed: bad field type");
  }
  auto _node_type = header->get_node_type();
  Ref<Node> ret;
  if (_field_type == field_type_t::N0) {
    if (_node_type == node_type_t::LEAF) {
      ret = new LeafNode0();
    } else {
      ret = new InternalNode0();
    }
  } else if (_field_type == field_type_t::N1) {
    if (_node_type == node_type_t::LEAF) {
      ret = new LeafNode1();
    } else {
      ret = new InternalNode1();
    }
  } else if (_field_type == field_type_t::N2) {
    if (_node_type == node_type_t::LEAF) {
      ret = new LeafNode2();
    } else {
      ret = new InternalNode2();
    }
  } else if (_field_type == field_type_t::N3) {
    if (_node_type == node_type_t::LEAF) {
      ret = new LeafNode3();
    } else {
      ret = new InternalNode3();
    }
  } else {
    assert(false);
  }
  ret->init(extent, is_level_tail);
  return ret;
}

}
