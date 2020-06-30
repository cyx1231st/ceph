// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "node.h"

#include <exception>

#include "dummy_transaction_manager.h"
#include "node_impl.h"

namespace crimson::os::seastore::onode {

std::pair<Ref<tree_cursor_t>, bool>
Node::insert(const onode_key_t& key, const onode_t& value, MatchHistory& history) {
  auto result = lower_bound(key, history);
  if (result.match == MatchKindBS::EQ) {
    return {result.p_cursor, false};
  } else {
    auto leaf_node = result.p_cursor->get_leaf_node();
    auto p_cursor = leaf_node->insert_bottomup(
        key, value, result.p_cursor->get_position(), history);
    return {p_cursor, true};
  }
}

Ref<Node> Node::allocate_root() {
  return LeafNode0::allocate(true);
}

Ref<Node> Node::load(laddr_t addr, bool is_level_tail, const parent_info_t& parent_info) {
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
  ret->init(extent, is_level_tail, &parent_info);
  return ret;
}

}
