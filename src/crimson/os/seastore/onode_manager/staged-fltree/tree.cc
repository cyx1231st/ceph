// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tree.h"

#include "dummy_transaction_manager.h"
#include "node.h"
#include "super_node.h"

namespace crimson::os::seastore::onode {

Btree::Cursor::Cursor(Btree* tree, Ref<tree_cursor_t> _p_cursor)
  : tree(*tree) {
  // for cursors indicating end of tree
  // untrack the leaf node
  if (!_p_cursor->is_end()) {
    p_cursor = _p_cursor;
  }
}

Btree::Cursor::~Cursor() = default;

Btree::Cursor::Cursor(Btree* p_tree) : tree{*p_tree} {}

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

Btree::Cursor Btree::Cursor::operator++(int) {
  Cursor tmp = *this;
  ++*this;
  return tmp;
}

Btree::Cursor Btree::Cursor::make_end(Btree* tree) {
  return {tree};
}

Btree::Btree(TransactionManagerURef&& tm) : tm{std::move(tm)} {}

Btree::~Btree() { assert(tracked_supers.empty()); }

void Btree::mkfs(Transaction& t) {
  auto super = tm->get_super_node(t, *this);
  Node::mkfs(get_context(t), std::move(super));
}

Btree::Cursor Btree::begin(Transaction& t) {
  return {this, get_root(t)->lookup_smallest(get_context(t))};
}

Btree::Cursor Btree::last(Transaction& t) {
  return {this, get_root(t)->lookup_largest(get_context(t))};
}

Btree::Cursor Btree::end() {
  return Cursor::make_end(this);
}

bool Btree::contains(Transaction& t, const onode_key_t& key) {
  // TODO: improve lower_bound()
  return MatchKindBS::EQ == get_root(t)->lower_bound(get_context(t), key).match;
}

Btree::Cursor Btree::find(Transaction& t, const onode_key_t& key) {
  // TODO: improve lower_bound()
  auto result = get_root(t)->lower_bound(get_context(t), key);
  if (result.match == MatchKindBS::EQ) {
    return Cursor(this, result.p_cursor);
  } else {
    return Cursor::make_end(this);
  }
}

Btree::Cursor Btree::lower_bound(Transaction& t, const onode_key_t& key) {
  return Cursor(this, get_root(t)->lower_bound(get_context(t), key).p_cursor);
}

std::pair<Btree::Cursor, bool>
Btree::insert(Transaction& t, const onode_key_t& key, const onode_t& value) {
  auto [cursor, success] = get_root(t)->insert(get_context(t), key, value);
  return {{this, cursor}, success};
}

size_t Btree::erase(Transaction& t, const onode_key_t& key) {
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

size_t Btree::height(Transaction& t) {
  return get_root(t)->level() + 1;
}

std::ostream& Btree::dump(Transaction& t, std::ostream& os) {
  return get_root(t)->dump(os);
}

Ref<Node> Btree::get_root(Transaction& t) {
  auto iter = tracked_supers.find(&t);
  if (iter == tracked_supers.end()) {
    auto super = tm->get_super_node(t, *this);
    assert(tracked_supers.find(&t)->second == super.get());
    auto root = Node::load_root(get_context(t), std::move(super));
    assert(tracked_supers.find(&t)->second->get_p_root() == root.get());
    return root;
  } else {
    return iter->second->get_p_root();
  }
}

void Btree::test_clone_from(
    Transaction& t, Transaction& t_from, Btree& from) {
  // Note: assume the tree to clone is tracked correctly in memory.
  // In some unit tests, parts of the tree are stubbed out that they
  // should not be loaded from TransactionManager.
  auto super = tm->get_super_node(t, *this);
  // TODO: reverse
  from.get_root(t_from)->test_clone_root(get_context(t), std::move(super));
}

}
