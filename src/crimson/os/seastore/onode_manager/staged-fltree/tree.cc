// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tree.h"

#include <iostream>

#include "node.h"
#include "node_extent_manager.h"
#include "stages/key_layout.h" // full_key_t<KeyT::HOBJ>
#include "super.h"

namespace crimson::os::seastore::onode {

using btree_ertr = Btree::btree_ertr;
template <class... ValuesT>
using btree_future = Btree::btree_future<ValuesT...>;

Btree::Cursor::Cursor(Btree* p_tree, Ref<tree_cursor_t> _p_cursor)
  : p_tree(p_tree) {
  // for cursors indicating end of tree
  // untrack the leaf node
  if (!_p_cursor->is_end()) {
    p_cursor = _p_cursor;
  }
}
Btree::Cursor::Cursor(Btree* p_tree) : p_tree{p_tree} {}
Btree::Cursor::Cursor(const Cursor& other) = default;
Btree::Cursor::Cursor(Cursor&& other) noexcept = default;
Btree::Cursor::~Cursor() = default;

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

Btree::Cursor Btree::Cursor::make_end(Btree* p_tree) {
  return {p_tree};
}

Btree::Btree(NodeExtentManagerURef&& _nm)
  : nm{std::move(_nm)},
    root_tracker{RootNodeTracker::create(nm->is_read_isolated())} {}

Btree::~Btree() { assert(root_tracker->is_clean()); }

btree_future<> Btree::mkfs(Transaction& t) {
  return nm->get_super(t, *root_tracker).safe_then([this, &t](auto super) {
    return Node::mkfs(get_context(t), std::move(super));
  });
}

btree_future<Btree::Cursor> Btree::begin(Transaction& t) {
  return get_root(t).safe_then([this, &t](auto root) {
    return root->lookup_smallest(get_context(t));
  }).safe_then([this](auto cursor) {
    return Cursor{this, cursor};
  });
}

btree_future<Btree::Cursor> Btree::last(Transaction& t) {
  return get_root(t).safe_then([this, &t](auto root) {
    return root->lookup_largest(get_context(t));
  }).safe_then([this](auto cursor) {
    return Cursor(this, cursor);
  });
}

Btree::Cursor Btree::end() {
  return Cursor::make_end(this);
}

btree_future<bool>
Btree::contains(Transaction& t, const onode_key_t& key) {
  return seastar::do_with(
    full_key_t<KeyT::HOBJ>(key),
    [this, &t](auto& key) -> btree_future<bool> {
      return get_root(t).safe_then([this, &t, &key](auto root) {
        // TODO: improve lower_bound()
        return root->lower_bound(get_context(t), key);
      }).safe_then([](auto result) {
        return MatchKindBS::EQ == result.match;
      });
    }
  );
}

btree_future<Btree::Cursor>
Btree::find(Transaction& t, const onode_key_t& key) {
  return seastar::do_with(
    full_key_t<KeyT::HOBJ>(key),
    [this, &t](auto& key) -> btree_future<Btree::Cursor> {
      return get_root(t).safe_then([this, &t, &key](auto root) {
        // TODO: improve lower_bound()
        return root->lower_bound(get_context(t), key);
      }).safe_then([this](auto result) {
        if (result.match == MatchKindBS::EQ) {
          return Cursor(this, result.p_cursor);
        } else {
          return Cursor::make_end(this);
        }
      });
    }
  );
}

btree_future<Btree::Cursor>
Btree::lower_bound(Transaction& t, const onode_key_t& key) {
  return seastar::do_with(
    full_key_t<KeyT::HOBJ>(key),
    [this, &t](auto& key) -> btree_future<Btree::Cursor> {
      return get_root(t).safe_then([this, &t, &key](auto root) {
        return root->lower_bound(get_context(t), key);
      }).safe_then([this](auto result) {
        return Cursor(this, result.p_cursor);
      });
    }
  );
}

btree_future<std::pair<Btree::Cursor, bool>>
Btree::insert(Transaction& t, const onode_key_t& key, const onode_t& value) {
  return seastar::do_with(
    full_key_t<KeyT::HOBJ>(key),
    [this, &t, &value](auto& key) -> btree_future<std::pair<Btree::Cursor, bool>> {
      return get_root(t).safe_then([this, &t, &key, &value](auto root) {
        return root->insert(get_context(t), key, value);
      }).safe_then([this](auto ret) {
        auto& [cursor, success] = ret;
        return std::make_pair(Cursor(this, cursor), success);
      });
    }
  );
}

btree_future<size_t> Btree::erase(Transaction& t, const onode_key_t& key) {
  // TODO
  return btree_ertr::make_ready_future<size_t>(0u);
}

btree_future<Btree::Cursor> Btree::erase(Btree::Cursor& pos) {
  // TODO
  return btree_ertr::make_ready_future<Cursor>(
      Cursor::make_end(this));
}

btree_future<Btree::Cursor>
Btree::erase(Btree::Cursor& first, Btree::Cursor& last) {
  // TODO
  return btree_ertr::make_ready_future<Cursor>(
      Cursor::make_end(this));
}

btree_future<size_t> Btree::height(Transaction& t) {
  return get_root(t).safe_then([](auto root) {
    return size_t(root->level() + 1);
  });
}

std::ostream& Btree::dump(Transaction& t, std::ostream& os) {
  auto root = root_tracker->get_root(t);
  if (root) {
    root->dump(os);
  } else {
    os << "empty tree!";
  }
  return os;
}

btree_future<Ref<Node>> Btree::get_root(Transaction& t) {
  auto root = root_tracker->get_root(t);
  if (root) {
    return btree_ertr::make_ready_future<Ref<Node>>(root);
  } else {
    return nm->get_super(t, *root_tracker).safe_then([this, &t](auto&& super) {
      return Node::load_root(get_context(t), std::move(super));
    }).safe_then([this, &t](auto root) {
      assert(root == root_tracker->get_root(t));
      return root;
    });
  }
}

bool Btree::test_is_clean() const {
  return root_tracker->is_clean();
}

btree_future<> Btree::test_clone_from(
    Transaction& t, Transaction& t_from, Btree& from) {
  // Note: assume the tree to clone is tracked correctly in memory.
  // In some unit tests, parts of the tree are stubbed out that they
  // should not be loaded from NodeExtentManager.
  return nm->get_super(t, *root_tracker
  ).safe_then([this, &t, &t_from, &from](auto super) {
    return from.get_root(t_from
    ).safe_then([this, &t, super = std::move(super)](auto root) mutable {
      // XXX: reverse
      return root->test_clone_root(get_context(t), std::move(super));
    });
  });
}

}
