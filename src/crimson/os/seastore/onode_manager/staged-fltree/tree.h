// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <optional>
#include <ostream>

#include "crimson/common/type_helpers.h"

#include "tree_types.h"

namespace crimson::os::seastore::onode {

class DummyRootBlock;
class Node;

/*
 * btree interfaces
 * requirements are based on:
 *   ceph::os::Transaction::create/touch/remove()
 *   ceph::ObjectStore::collection_list()
 *   ceph::BlueStore::get_onode()
 *   db->get_iterator(PREFIIX_OBJ) by ceph::BlueStore::fsck()
 */
class Btree {
 public:
  Btree(TransactionManagerURef&& tm);
  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree& operator=(const Btree&) = delete;
  Btree& operator=(Btree&&) = delete;
  ~Btree();

  void mkfs(Transaction&);

  class Cursor;
  // lookup
  Cursor begin(Transaction&);
  Cursor last(Transaction&);
  Cursor end();
  bool contains(Transaction&, const onode_key_t&);
  Cursor find(Transaction&, const onode_key_t&);
  Cursor lower_bound(Transaction&, const onode_key_t&);

  // modifiers
  std::pair<Cursor, bool> insert(Transaction&, const onode_key_t&, const onode_t&);
  size_t erase(Transaction&, const onode_key_t& key);
  Cursor erase(Cursor& pos);
  Cursor erase(Cursor& first, Cursor& last);

  // stats
  size_t height(Transaction&);
  std::ostream& dump(Transaction&, std::ostream& os);

  // test_only
  bool test_is_clean() const { return tracked_supers.empty(); }
  void test_clone_from(Transaction& t, Transaction& t_from, Btree& from);

 private:
  // called by the tracked super node
  void do_track_super(Transaction& t, DummyRootBlock& super) {
    assert(tracked_supers.find(&t) == tracked_supers.end());
    tracked_supers[&t] = &super;
  }
  void do_untrack_super(Transaction& t, DummyRootBlock& super) {
    auto removed = tracked_supers.erase(&t);
    assert(removed);
  }
  context_t get_context(Transaction& t) { return {*tm, t}; }

  Ref<Node> get_root(Transaction& t);

  TransactionManagerURef tm;
  // XXX abstract a root tracker
  std::map<Transaction*, DummyRootBlock*> tracked_supers;

  friend class DummyRootBlock;
};

class tree_cursor_t;
class Btree::Cursor {
 public:
  ~Cursor();

  bool is_end() const;
  const onode_key_t& key();
  const onode_t* value() const;
  bool operator==(const Cursor& x) const;
  bool operator!=(const Cursor& x) const { return !(*this == x); }
  Cursor& operator++();
  Cursor operator++(int);

 private:
  Cursor(Btree*, Ref<tree_cursor_t>);
  Cursor(Btree* p_tree);

  static Cursor make_end(Btree* tree);

  Btree& tree;
  Ref<tree_cursor_t> p_cursor;
  std::optional<onode_key_t> key_copy;

  friend class Btree;
};

}
