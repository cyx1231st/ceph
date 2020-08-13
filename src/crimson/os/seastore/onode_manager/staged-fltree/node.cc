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

Node::~Node() {
  // XXX: tolerate failure between allocate() and as_child()
  if (is_root()) {
    tree->do_untrack_root(*this);
  } else {
    _parent_info->ptr->do_untrack_child(*this);
  }
}

void Node::upgrade_root() {
  assert(is_root());
  assert(is_level_tail());
  assert(field_type() == field_type_t::N0);
  tree->do_untrack_root(*this);
  auto new_root = InternalNode0::allocate_root(level(), laddr(), tree, super);
  super.reset();
  tree.reset();
  as_child(search_position_t::end(), new_root);
}

template <bool VALIDATE>
void Node::as_child(const search_position_t& pos, Ref<InternalNode> parent_node) {
  assert(!super);
  assert(!tree);
  _parent_info = parent_info_t{pos, parent_node};
  parent_info().ptr->do_track_child<VALIDATE>(*this);
}
template void Node::as_child<true>(const search_position_t&, Ref<InternalNode>);
template void Node::as_child<false>(const search_position_t&, Ref<InternalNode>);


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
  LeafNode0::mkfs(/* transaction, */btree);
}

Ref<Node> Node::load_root(/* transaction, */Ref<Btree> btree) {
  auto super = btree->get_super_block(/* transaction */);
  auto root_addr = super->get_onode_root_laddr();
  assert(root_addr != L_ADDR_NULL);
  auto root = Node::load(root_addr, true);
  assert(root->field_type() == field_type_t::N0);
  root->as_root(btree, super);
  return root;
}

Ref<Node> Node::load(laddr_t addr, bool expect_is_level_tail) {
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
  ret->init(extent);
  assert(ret->is_level_tail() == expect_is_level_tail);
  return ret;
}

void Node::insert_parent(Ref<Node> right_node) {
  assert(!is_root());
  // TODO(cross-node string dedup)
  auto my_key = get_largest_key_view();
  parent_info().ptr->apply_child_split(
      parent_info().position, my_key, this, right_node);
}

void InternalNode::validate_child(const Node& child) const {
#ifndef NDEBUG
  assert(this->level() - 1 == child.level());
  assert(this == child.parent_info().ptr);
  auto& child_pos = child.parent_info().position;
  assert(*get_p_value(child_pos) == child.laddr());
  if (child_pos.is_end()) {
    assert(this->is_level_tail());
    assert(child.is_level_tail());
  } else {
    assert(!child.is_level_tail());
    assert(get_key_view(child_pos) == child.get_largest_key_view());
  }
  // XXX(multi-type)
  assert(this->field_type() <= child.field_type());
#endif
}

}
