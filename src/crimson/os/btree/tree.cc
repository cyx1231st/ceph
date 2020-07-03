// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tree.h"

#include <memory>
#include <optional>

#include "node.h"

namespace crimson::os::seastore::onode {

Btree::Cursor::Cursor(Btree* tree, Ref<tree_cursor_t> _p_cursor)
  : tree(*tree) {
  // for cursors indicating end of tree
  // untrack the leaf node
  if (!_p_cursor->is_end()) {
    p_cursor = _p_cursor;
  }
}

bool Btree::Cursor::is_end() const {
  if (p_cursor) {
    assert(!p_cursor->is_end());
    return false;
  } else {
    return true;
  }
}

const onode_key_t& Btree::Cursor::key() {
  // TODO
  return {};
}

// might return Onode class to track the changing onode_t pointer
// TODO: p_value might be invalid
const onode_t* Btree::Cursor::value() const {
  return p_cursor->get_p_value();
}

bool Btree::Cursor::operator==(const Cursor& x) const {
  return p_cursor == x.p_cursor;
}

Btree::Cursor& Btree::Cursor::operator++() {
  // TODO
  return *this;
}

Btree::Cursor Btree::Cursor::make_end(Btree* tree) {
  return {tree};
}

Btree::Cursor Btree::begin() {
  return {this, root_node->lookup_smallest()};
}

Btree::Cursor Btree::last() {
  return {this, root_node->lookup_largest()};
}

Btree::Cursor Btree::end() {
  return Cursor::make_end(this);
}

bool Btree::contains(const onode_key_t& key) {
  // TODO: improve lower_bound()
  return MatchKindBS::EQ == root_node->lower_bound(key).match;
}

Btree::Cursor Btree::find(const onode_key_t& key) {
  // TODO: improve lower_bound()
  auto result = root_node->lower_bound(key);
  if (result.match == MatchKindBS::EQ) {
    return Cursor(this, result.p_cursor);
  } else {
    return Cursor::make_end(this);
  }
}

Btree::Cursor Btree::lower_bound(const onode_key_t& key) {
  return Cursor(this, root_node->lower_bound(key).p_cursor);
}

std::pair<Btree::Cursor, bool>
Btree::insert(const onode_key_t& key, const onode_t& value) {
  auto [cursor, success] = root_node->insert(key, value);
  return {{this, cursor}, success};
}

size_t Btree::erase(const onode_key_t& key) {
  // TODO
  return 0u;
}

Btree::Cursor Btree::erase(Btree::Cursor& pos) {
  // TODO
  return Cursor::make_end(this);
}

Btree::Cursor Btree::erase(Btree::Cursor& first, Btree::Cursor& last) {
  // TODO
  return Cursor::make_end(this);
}

size_t Btree::height() const {
  return root_node->level() + 1;
}

std::ostream& Btree::dump(std::ostream& os) {
  return root_node->dump(os);
}

Btree& Btree::get() {
  static std::unique_ptr<Btree> singleton;
  if (!singleton) {
    singleton.reset(new Btree(Node::allocate_root()));
  }
  return *singleton;
}

}
