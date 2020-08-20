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
  using btree_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  template <class... ValuesT>
  using btree_future = btree_ertr::future<ValuesT...>;

  Btree(TransactionManagerURef&& tm);
  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree& operator=(const Btree&) = delete;
  Btree& operator=(Btree&&) = delete;
  ~Btree();

  btree_future<> mkfs(Transaction&);

  class Cursor;
  // lookup
  btree_future<Cursor> begin(Transaction&);
  btree_future<Cursor> last(Transaction&);
  Cursor end();
  btree_future<bool> contains(Transaction&, const onode_key_t&);
  btree_future<Cursor> find(Transaction&, const onode_key_t&);
  btree_future<Cursor> lower_bound(Transaction&, const onode_key_t&);

  // modifiers
  // TODO: get_or_insert onode
  btree_future<std::pair<Cursor, bool>>
  insert(Transaction&, const onode_key_t&, const onode_t&);
  btree_future<size_t> erase(Transaction&, const onode_key_t& key);
  btree_future<Cursor> erase(Cursor& pos);
  btree_future<Cursor> erase(Cursor& first, Cursor& last);

  // stats
  btree_future<size_t> height(Transaction&);
  std::ostream& dump(Transaction&, std::ostream&);

  // test_only
  bool test_is_clean() const { return tracked_supers.empty(); }
  btree_future<> test_clone_from(Transaction& t, Transaction& t_from, Btree& from);

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

  btree_future<Ref<Node>> get_root(Transaction& t);

  TransactionManagerURef tm;
  // XXX abstract a root tracker
  std::map<Transaction*, DummyRootBlock*> tracked_supers;

  friend class DummyRootBlock;
};

class tree_cursor_t;
class Btree::Cursor {
 public:
  Cursor(const Cursor& other);
  Cursor(Cursor&& other) noexcept;
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
  Cursor(Btree*);

  static Cursor make_end(Btree*);

  Btree* p_tree;
  Ref<tree_cursor_t> p_cursor;

  friend class Btree;
};

}
