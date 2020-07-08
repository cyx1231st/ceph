// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>
#include "crimson/common/type_helpers.h"

#include "tree_types.h"

namespace crimson::os::seastore::onode {

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
  class Cursor;

  // TODO: transaction
  // lookup
  Cursor begin();
  Cursor last();
  Cursor end();
  bool contains(const onode_key_t& key);
  Cursor find(const onode_key_t& key);
  Cursor lower_bound(const onode_key_t& key);

  // modifiers
  std::pair<Cursor, bool> insert(const onode_key_t& key, const onode_t& value);
  size_t erase(const onode_key_t& key);
  Cursor erase(Cursor& pos);
  Cursor erase(Cursor& first, Cursor& last);

  // stats
  size_t height() const;
  std::ostream& dump(std::ostream& os);

  static Btree& get();

 private:
  Btree();
  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree& operator=(const Btree&) = delete;

  Ref<Node> root_node;
};

struct tree_cursor_t;

// TODO: track cursors in LeafNode by position (intrusive)
class Btree::Cursor {
 public:
  Cursor(Btree*, Ref<tree_cursor_t>);
  Cursor(const Cursor& x) = default;
  ~Cursor() = default;

  bool is_end() const;
  const onode_key_t& key();
  const onode_t* value() const;
  bool operator==(const Cursor& x) const;
  bool operator!=(const Cursor& x) const { return !(*this == x); }
  Cursor& operator++();
  Cursor operator++(int) {
    Cursor tmp = *this;
    ++*this;
    return tmp;
  }

  static Cursor make_end(Btree* tree);

 private:
  Cursor(Btree* p_tree) : tree{*p_tree} {}

  Btree& tree;
  Ref<tree_cursor_t> p_cursor;
  std::optional<onode_key_t> key_copy;
};

}
