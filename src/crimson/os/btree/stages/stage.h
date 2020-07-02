// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <optional>
#include <ostream>
// TODO: remove
#include <iostream>
#include <sstream>
#include <type_traits>

#include "common/likely.h"

#include "sub_items_stage.h"
#include "item_iterator_stage.h"

namespace crimson::os::seastore::onode {

struct search_result_bs_t {
  size_t index;
  MatchKindBS match;
};
template <typename FGetKey>
search_result_bs_t binary_search(
    const onode_key_t& key, size_t begin, size_t end, FGetKey&& f_get_key) {
  assert(begin <= end);
  while (begin < end) {
    auto total = begin + end;
    auto mid = total >> 1;
    // do not copy if return value is reference
    decltype(f_get_key(mid)) target = f_get_key(mid);
    auto match = compare_to(key, target);
    if (match == MatchKindCMP::NE) {
      end = mid;
    } else if (match == MatchKindCMP::PO) {
      begin = mid + 1;
    } else {
      return {mid, MatchKindBS::EQ};
    }
  }
  return {begin , MatchKindBS::NE};
}

template <typename PivotType, typename FGet>
search_result_bs_t binary_search_r(
    size_t rend, size_t rbegin, FGet&& f_get, const PivotType& key) {
  assert(rend <= rbegin);
  while (rend < rbegin) {
    auto total = rend + rbegin + 1;
    auto mid = total >> 1;
    // do not copy if return value is reference
    decltype(f_get(mid)) target = f_get(mid);
    int match = target - key;
    if (match < 0) {
      rend = mid;
    } else if (match > 0) {
      rbegin = mid - 1;
    } else {
      return {mid, MatchKindBS::EQ};
    }
  }
  return {rbegin, MatchKindBS::NE};
}

using match_stat_t = int8_t;
constexpr match_stat_t MSTAT_PO = -2; // index is search_position_t::end()
constexpr match_stat_t MSTAT_EQ = -1; // key == index
constexpr match_stat_t MSTAT_NE0 = 0; // key == index [pool/shard crush ns/oid]; key < index [snap/gen]
constexpr match_stat_t MSTAT_NE1 = 1; // key == index [pool/shard crush]; key < index [ns/oid]
constexpr match_stat_t MSTAT_NE2 = 2; // key < index [pool/shard crush ns/oid] ||
                                      // key == index [pool/shard]; key < index [crush]
constexpr match_stat_t MSTAT_NE3 = 3; // key < index [pool/shard]

inline bool matchable(field_type_t type, match_stat_t mstat) {
  /*
   * compressed prefix by field type:
   * N0: NONE
   * N1: pool/shard
   * N2: pool/shard crush
   * N3: pool/shard crush ns/oid
   *
   * if key matches the node's compressed prefix, return true
   * else, return false
   */
  return mstat + to_unsigned(type) < 4;
}

inline void assert_mstat(const onode_key_t& key, const index_view_t& index, match_stat_t mstat) {
  // key < index ...
  switch (mstat) {
   case MSTAT_EQ:
    break;
   case MSTAT_NE0:
    assert(index.p_snap_gen);
    assert(compare_to(key, *index.p_snap_gen) == MatchKindCMP::NE);
    break;
   case MSTAT_NE1:
    assert(index.p_ns_oid);
    assert(compare_to(key, *index.p_ns_oid) == MatchKindCMP::NE);
    break;
   case MSTAT_NE2:
    assert(index.p_crush);
    if (index.p_shard_pool) {
      assert(compare_to(key, shard_pool_crush_t{*index.p_shard_pool,
                                                *index.p_crush}) ==
             MatchKindCMP::NE);
    } else {
      assert(compare_to(key, *index.p_crush) == MatchKindCMP::NE);
    }
    break;
   default:
    assert(false);
  }
  // key == index ...
  switch (mstat) {
   case MSTAT_EQ:
    assert(index.p_snap_gen);
    assert(compare_to(key, *index.p_snap_gen) == MatchKindCMP::EQ);
   case MSTAT_NE0:
    if (!index.p_ns_oid)
      break;
    assert(index.p_ns_oid->type() == ns_oid_view_t::Type::MAX ||
           compare_to(key, *index.p_ns_oid) == MatchKindCMP::EQ);
   case MSTAT_NE1:
    if (!index.p_crush)
      break;
    assert(compare_to(key, *index.p_crush) == MatchKindCMP::EQ);
    if (!index.p_shard_pool)
      break;
    assert(compare_to(key, *index.p_shard_pool) == MatchKindCMP::EQ);
   default:
    break;
  }
}

template <node_type_t NODE_TYPE, match_stage_t STAGE>
struct staged_result_t {
  using me_t = staged_result_t<NODE_TYPE, STAGE>;
  bool is_end() const { return position.is_end(); }

  static me_t end() {
    return {staged_position_t<STAGE>::end(), MatchKindBS::NE, nullptr, MSTAT_PO};
  }
  template <typename T = me_t>
  static std::enable_if_t<STAGE != STAGE_BOTTOM, T>
  from_nxt(size_t index, const staged_result_t<NODE_TYPE, STAGE - 1>& nxt_stage_result) {
    return {{index, nxt_stage_result.position},
            nxt_stage_result.match,
            nxt_stage_result.p_value,
            nxt_stage_result.mstat};
  }

  staged_position_t<STAGE> position;
  MatchKindBS match;
  const value_type_t<NODE_TYPE>* p_value;
  match_stat_t mstat;
};

template <node_type_t NODE_TYPE>
staged_result_t<NODE_TYPE, STAGE_TOP>&& normalize(
    staged_result_t<NODE_TYPE, STAGE_TOP>&& result) { return std::move(result); }

template <node_type_t NODE_TYPE, match_stage_t STAGE,
          typename = std::enable_if_t<STAGE != STAGE_TOP>>
staged_result_t<NODE_TYPE, STAGE_TOP> normalize(
    staged_result_t<NODE_TYPE, STAGE>&& result) {
  return {normalize(std::move(result.position)), result.match, result.p_value};
}

/*
 * staged infrastructure
 */

template <node_type_t _NODE_TYPE>
struct staged_params_subitems {
  using container_t = sub_items_t<_NODE_TYPE>;
  static constexpr auto NODE_TYPE = _NODE_TYPE;
  static constexpr auto STAGE = STAGE_RIGHT;

  // dummy type in order to make our type system work
  // any better solution to get rid of this?
  using next_param_t = staged_params_subitems<NODE_TYPE>;
};

template <node_type_t _NODE_TYPE>
struct staged_params_item_iterator {
  using container_t = item_iterator_t<_NODE_TYPE>;
  static constexpr auto NODE_TYPE = _NODE_TYPE;
  static constexpr auto STAGE = STAGE_STRING;

  using next_param_t = staged_params_subitems<NODE_TYPE>;
};

template <typename NodeType>
struct staged_params_node_01 {
  using container_t = NodeType;
  static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
  static constexpr auto STAGE = STAGE_LEFT;

  using next_param_t = staged_params_item_iterator<NODE_TYPE>;
};

template <typename NodeType>
struct staged_params_node_2 {
  using container_t = NodeType;
  static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
  static constexpr auto STAGE = STAGE_STRING;

  using next_param_t = staged_params_subitems<NODE_TYPE>;
};

template <typename NodeType>
struct staged_params_node_3 {
  using container_t = NodeType;
  static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
  static constexpr auto STAGE = STAGE_RIGHT;

  // dummy type in order to make our type system work
  // any better solution to get rid of this?
  using next_param_t = staged_params_node_3<NodeType>;
};

#define NXT_STAGE_T staged<typename Params::next_param_t>

enum class TrimType { BEFORE, AFTER, AT };

template <typename Params>
struct staged {
  static_assert(Params::STAGE >= STAGE_BOTTOM);
  static_assert(Params::STAGE <= STAGE_TOP);
  using container_t = typename Params::container_t;
  using key_get_type = typename container_t::key_get_type;
  using position_t = staged_position_t<Params::STAGE>;
  using result_t = staged_result_t<Params::NODE_TYPE, Params::STAGE>;
  using value_t = value_type_t<Params::NODE_TYPE>;
  static constexpr auto CONTAINER_TYPE = container_t::CONTAINER_TYPE;
  static constexpr bool IS_BOTTOM = (Params::STAGE == STAGE_BOTTOM);
  static constexpr auto NODE_TYPE = Params::NODE_TYPE;
  static constexpr auto STAGE = Params::STAGE;

  template <bool is_exclusive>
  static void _left_or_right(size_t& s_index, size_t i_index,
                             std::optional<bool>& i_to_left) {
    assert(!i_to_left.has_value());
    if constexpr (is_exclusive) {
      if (s_index <= i_index) {
        // ...[s_index-1] |!| (i_index) [s_index]...
        // offset i_position to right
        i_to_left = false;
      } else {
        // ...[s_index-1] (i_index)) |?[s_index]| ...
        // ...(i_index)...[s_index-1] |?[s_index]| ...
        i_to_left = true;
        --s_index;
      }
    } else {
      if (s_index < i_index) {
        // ...[s_index-1] |?[s_index]| ...[(i_index)[s_index_k]...
        i_to_left = false;
      } else if (s_index > i_index) {
        // ...[(i_index)s_index-1] |?[s_index]| ...
        // ...[(i_index)s_index_k]...[s_index-1] |?[s_index]| ...
        i_to_left = true;
      } else {
        // ...[s_index-1] |?[(i_index)s_index]| ...
        // i_to_left = std::nullopt;
      }
    }
  }

  template <ContainerType CTYPE, typename Enable = void> class _iterator_t;
  template <ContainerType CTYPE>
  class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::INDEXABLE>> {
   /*
    * indexable container type system:
    *   CONTAINER_TYPE = ContainerType::INDEXABLE
    *   keys() const -> size_t
    *   operator[](size_t) const -> key_get_type
    *   size_before(size_t) const -> size_t
    * IF IS_BOTTOM:
    *   get_p_value(size_t) const -> const value_t*
    * ELSE
    *   size_to_nxt_at(size_t) const -> size_t
    *   get_nxt_container(size_t) const
    * Appender::append(const container_t& src, from, items)
    */
   public:
    using me_t = _iterator_t<CTYPE>;

    _iterator_t(const container_t& container) : container{container} {
      assert(container.keys());
    }

    size_t index() const {
      return _index;
    }
    key_get_type get_key() const {
      assert(!is_end());
      return container[_index];
    }
    size_t size_to_nxt() const {
      assert(!is_end());
      return container.size_to_nxt_at(_index);
    }
    template <typename T = typename NXT_STAGE_T::container_t>
    std::enable_if_t<!IS_BOTTOM, T> get_nxt_container() const {
      assert(!is_end());
      return container.get_nxt_container(_index);
    }
    template <typename T = value_t>
    std::enable_if_t<IS_BOTTOM, const T*> get_p_value() const {
      assert(!is_end());
      return container.get_p_value(_index);
    }
    bool is_last() const {
      return _index + 1 == container.keys();
    }
    bool is_end() const { return _index == container.keys(); }
    size_t size() const {
      assert(!is_end());
      return container.size_before(_index + 1) -
             container.size_before(_index);
    }
    me_t& operator++() {
      assert(!is_end());
      assert(!is_last());
      ++_index;
      return *this;
    }
    void set_end() {
      assert(!is_end());
      assert(is_last());
      ++_index;
    }
    // Note: possible to return an end iterator
    MatchKindBS seek(const onode_key_t& key, bool exclude_last) {
      assert(!is_end());
      assert(index() == 0);
      size_t end_index = container.keys();
      if (exclude_last) {
        assert(end_index);
        --end_index;
        assert(compare_to(key, container[end_index]) == MatchKindCMP::NE);
      }
      auto ret = binary_search(key, _index, end_index,
          [this] (size_t index) { return container[index]; });
      _index = ret.index;
      return ret.match;
    }

    void seek_at(size_t index) {
      assert(!is_end());
      assert(this->index() == 0);
      assert(index < container.keys());
      _index = index;
    }

    void seek_last() {
      assert(!is_end());
      assert(index() == 0);
      _index = container.keys() - 1;
    }

    // Note: possible to return an end iterator when is_exclusive is true
    template <bool is_exclusive>
    size_t seek_split_inserted(size_t start_size, size_t extra_size,
                               size_t target_size, size_t& i_index, size_t i_size,
                               std::optional<bool>& i_to_left) {
      assert(!is_end());
      assert(index() == 0);
      assert(i_index < container.keys() || i_index == INDEX_END);
      if constexpr (!is_exclusive) {
        if (i_index == INDEX_END) {
          i_index = container.keys() - 1;
        }
      }
      auto start_size_1 = start_size + extra_size;
      auto f_get_used_size = [this, start_size, start_size_1,
                              i_index, i_size] (size_t index) {
        size_t current_size;
        if (unlikely(index == 0)) {
          current_size = start_size;
        } else {
          current_size = start_size_1;
          if (index > i_index) {
            current_size += i_size;
            if constexpr (is_exclusive) {
              --index;
            }
          }
          current_size += container.size_before(index);
        }
        return current_size;
      };
      size_t s_end;
      if constexpr (is_exclusive) {
        s_end = container.keys();
      } else {
        s_end = container.keys() - 1;
      }
      _index = binary_search_r(0, s_end, f_get_used_size, target_size).index;
      size_t current_size = f_get_used_size(_index);
      assert(current_size <= target_size);

      _left_or_right<is_exclusive>(_index, i_index, i_to_left);
      return current_size;
    }

    size_t seek_split(size_t start_size, size_t extra_size, size_t target_size) {
      assert(!is_end());
      assert(index() == 0);
      auto start_size_1 = start_size + extra_size;
      auto f_get_used_size = [this, start_size, start_size_1] (size_t index) {
        size_t current_size;
        if (unlikely(index == 0)) {
          current_size = start_size;
        } else {
          current_size = start_size_1 + container.size_before(index);
        }
        return current_size;
      };
      _index = binary_search_r(
          0, container.keys() - 1, f_get_used_size, target_size).index;
      size_t current_size = f_get_used_size(_index);
      assert(current_size <= target_size);
      return current_size;
    }

    // Note: possible to return an end iterater if
    // to_index == INDEX_END && to_stage == STAGE
    void copy_out_until(typename container_t::Appender& appender,
                        size_t& to_index,
                        match_stage_t to_stage) {
      assert(to_stage <= STAGE);
      auto num_keys = container.keys();
      size_t items;
      if (to_index == INDEX_END) {
        if (to_stage == STAGE) {
          items = num_keys - _index;
          appender.append(container, _index, items);
          _index = num_keys;
        } else {
          assert(!is_end());
          items = num_keys - 1 - _index;
          appender.append(container, _index, items);
          _index = num_keys - 1;
        }
        to_index = _index;
      } else {
        assert(_index <= to_index);
        items = to_index - _index;
        appender.append(container, _index, items);
        _index = to_index;
      }
    }

    size_t trim_until(LogicalCachedExtent& extent) {
      return container_t::trim_until(extent, container, _index);
    }

    template <typename T = size_t>
    std::enable_if_t<!IS_BOTTOM, T>
    trim_at(LogicalCachedExtent& extent, size_t trimmed) {
      return container_t::trim_at(extent, container, _index, trimmed);
    }

   private:
    container_t container;
    size_t _index = 0;
  };

  template <ContainerType CTYPE>
  class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::ITERATIVE>> {
    /*
     * iterative container type system (!IS_BOTTOM):
     *   CONTAINER_TYPE = ContainerType::ITERATIVE
     *   index() const -> size_t
     *   get_key() const -> key_get_type
     *   size() const -> size_t
     *   size_to_nxt() const -> size_t
     *   get_nxt_container() const
     *   has_next() const -> bool
     *   operator++()
     */
    // currently the iterative iterator is only implemented with STAGE_STRING
    // for in-node space efficiency
    static_assert(STAGE == STAGE_STRING);
   public:
    using me_t = _iterator_t<CTYPE>;

    _iterator_t(const container_t& container) : container{container} {
      assert(index() == 0);
    }

    size_t index() const {
      if (is_end()) {
        return end_index;
      } else {
        return container.index();
      }
    }
    key_get_type get_key() const {
      assert(!is_end());
      return container.get_key();
    }
    size_t size_to_nxt() const {
      assert(!is_end());
      return container.size_to_nxt();
    }
    const typename NXT_STAGE_T::container_t get_nxt_container() const {
      assert(!is_end());
      return container.get_nxt_container();
    }
    bool is_last() const {
      assert(!is_end());
      return !container.has_next();
    }
    bool is_end() const { return _is_end; }
    me_t& operator++() {
      assert(!is_end());
      assert(!is_last());
      ++container;
      return *this;
    }
    void set_end() {
      assert(!is_end());
      assert(is_last());
      _is_end = true;
      end_index = container.index() + 1;
    }
    size_t size() const {
      assert(!is_end());
      return container.size();
    }
    // Note: possible to return an end iterator
    MatchKindBS seek(const onode_key_t& key, bool exclude_last) {
      assert(!is_end());
      assert(index() == 0);
      do {
        if (exclude_last && is_last()) {
          assert(compare_to(key, get_key()) == MatchKindCMP::NE);
          return MatchKindBS::NE;
        }
        auto match = compare_to(key, get_key());
        if (match == MatchKindCMP::NE) {
          return MatchKindBS::NE;
        } else if (match == MatchKindCMP::EQ) {
          return MatchKindBS::EQ;
        } else {
          if (container.has_next()) {
            ++container;
          } else {
            // end
            break;
          }
        }
      } while (true);
      assert(!exclude_last);
      set_end();
      return MatchKindBS::NE;
    }

    void seek_at(size_t index) {
      assert(!is_end());
      assert(this->index() == 0);
      while (index > 0) {
        assert(container.has_next());
        ++container;
        --index;
      }
    }

    void seek_last() {
      assert(!is_end());
      assert(index() == 0);
      while (container.has_next()) {
        ++container;
      }
    }

    // Note: possible to return an end iterator when is_exclusive is true
    template <bool is_exclusive>
    size_t seek_split_inserted(size_t start_size, size_t extra_size,
                               size_t target_size, size_t& i_index, size_t i_size,
                               std::optional<bool>& i_to_left) {
      assert(!is_end());
      assert(index() == 0);
      size_t current_size = start_size;
      size_t s_index = 0;
      do {
        if constexpr (!is_exclusive) {
          if (is_last()) {
            if (i_index == INDEX_END) {
              i_index = index();
            }
            break;
          }
        }

        size_t nxt_size = current_size;
        if (s_index == 0) {
          nxt_size += extra_size;
        }
        if (s_index == i_index) {
          nxt_size += i_size;
          if constexpr (is_exclusive) {
            if (nxt_size > target_size) {
              break;
            }
            current_size = nxt_size;
            ++s_index;
          }
        }
        nxt_size += size();
        if (nxt_size > target_size) {
          break;
        }
        current_size = nxt_size;

        if constexpr (is_exclusive) {
          if (is_last()) {
            set_end();
            s_index = INDEX_END;
            assert(i_index == INDEX_END);
            break;
          } else {
            ++(*this);
            ++s_index;
          }
        } else {
          ++(*this);
          ++s_index;
        }
      } while (true);
      assert(current_size <= target_size);

      _left_or_right<is_exclusive>(s_index, i_index, i_to_left);

#ifndef NDEBUG
      if (!is_end()) {
        assert(s_index == index());
      }
#endif
      return current_size;
    }

    size_t seek_split(size_t start_size, size_t extra_size, size_t target_size) {
      assert(!is_end());
      assert(index() == 0);
      size_t current_size = start_size;
      do {
        if (is_last()) {
          break;
        }

        size_t nxt_size = current_size;
        if (index() == 0) {
          nxt_size += extra_size;
        }
        nxt_size += size();
        if (nxt_size > target_size) {
          break;
        }
        current_size = nxt_size;
        ++(*this);
      } while (true);
      assert(current_size <= target_size);
      return current_size;
    }

    // Note: possible to return an end iterater if
    // to_index == INDEX_END && to_stage == STAGE
    void copy_out_until(typename container_t::Appender& appender,
                        size_t& to_index,
                        match_stage_t to_stage) {
      assert(to_stage <= STAGE);
      if (is_end()) {
        assert(!container.has_next());
        assert(to_stage == STAGE);
        assert(to_index == index() || to_index == INDEX_END);
        to_index = index();
        return;
      }
      typename container_t::Appender::index_t type;
      size_t items;
      if (to_index == INDEX_END) {
        if (to_stage == STAGE) {
          type = container_t::Appender::index_t::end;
        } else {
          type = container_t::Appender::index_t::last;
        }
        items = INDEX_END;
      } else {
        assert(index() <= to_index);
        type = container_t::Appender::index_t::none;
        items = to_index - index();
      }
      if (appender.append(container, items, type)) {
        set_end();
      }
      to_index = index();
    }

    size_t trim_until(LogicalCachedExtent& extent) {
      if (is_end()) {
        return 0;
      }
      return container_t::trim_until(extent, container);
    }

    size_t trim_at(LogicalCachedExtent& extent, size_t trimmed) {
      assert(!is_end());
      return container_t::trim_at(extent, container, trimmed);
    }

   private:
    container_t container;
    bool _is_end = false;
    size_t end_index;
  };

  /*
   * iterator_t encapsulates both indexable and iterative implementations
   * from a *non-empty* container.
   * cstr(const container_t&)
   * access:
   *   index() -> size_t
   *   get_key() -> key_get_type (const reference or value type)
   *   is_last() -> bool
   *   is_end() -> bool
   *   size() -> size_t
   * IF IS_BOTTOM
   *   get_p_value() -> const value_t*
   * ELSE
   *   get_nxt_container() -> nxt_stage::container_t
   *   size_to_nxt() -> size_t
   * modifiers:
   *   operator++() -> iterator_t&
   *   set_end()
   *   seek_at(index)
   *   seek_last()
   *   seek(key, exclude_last) -> MatchKindBS
   *   seek_split_inserted<bool is_exclusive>(
   *       start_size, extra_size, target_size, i_index, i_size,
   *       std::optional<bool>& i_to_left)
   *           -> insert to left/right/unknown (!exclusive)
   *           -> insert to left/right         (exclusive, can be end)
   *     -> split_size
   *   seek_split(start_size, extra_size, target_size) -> split_size
   *   copy_out_until(appender, to_index, to_stage) (can be end)
   *   trim_until(extent) -> trim_size
   *   (!IS_BOTTOM) trim_at(extent, trimmed) -> trim_size
   */
  using iterator_t = _iterator_t<CONTAINER_TYPE>;

  /*
   * Lookup internals (hide?)
   */

  static result_t
  smallest_result(const iterator_t& iter) {
    static_assert(!IS_BOTTOM);
    assert(!iter.is_end());
    auto pos_smallest = NXT_STAGE_T::position_t::begin();
    auto nxt_container = iter.get_nxt_container();
    auto value_ptr = NXT_STAGE_T::get_p_value(nxt_container, pos_smallest);
    return {{iter.index(), pos_smallest}, MatchKindBS::NE, value_ptr, STAGE};
  }

  static result_t
  nxt_lower_bound(const onode_key_t& key, iterator_t& iter, MatchHistory& history) {
    static_assert(!IS_BOTTOM);
    assert(!iter.is_end());
    auto nxt_container = iter.get_nxt_container();
    auto nxt_result = NXT_STAGE_T::lower_bound(nxt_container, key, history);
    if (nxt_result.is_end()) {
      if (iter.is_last()) {
        return result_t::end();
      } else {
        return smallest_result(++iter);
      }
    } else {
      return result_t::from_nxt(iter.index(), nxt_result);
    }
  }

  static void
  lookup_largest(const container_t& container, position_t& position, const onode_t*& p_value) {
    auto iter = iterator_t(container);
    iter.seek_last();
    position.index = iter.index();
    if constexpr (IS_BOTTOM) {
      p_value = iter.get_p_value();
    } else {
      auto nxt_container = iter.get_nxt_container();
      NXT_STAGE_T::lookup_largest(nxt_container, position.nxt, p_value);
    }
  }

  static const value_t* get_p_value(const container_t& container, const position_t& position) {
    auto iter = iterator_t(container);
    iter.seek_at(position.index);
    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      return NXT_STAGE_T::get_p_value(nxt_container, position.nxt);
    } else {
      return iter.get_p_value();
    }
  }

  static void get_index_view(
      const container_t& container, const position_t& position, index_view_t& output) {
    auto iter = iterator_t(container);
    iter.seek_at(position.index);
    output.set(iter.get_key());
    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      return NXT_STAGE_T::get_index_view(nxt_container, position.nxt, output);
    }
  }

  static result_t
  lower_bound(const container_t& container, const onode_key_t& key, MatchHistory& history) {
    bool exclude_last = false;
    if (history.get<STAGE>().has_value()) {
      if (*history.get<STAGE>() == MatchKindCMP::EQ) {
        // lookup is short-circuited
        if constexpr (!IS_BOTTOM) {
          assert(history.get<STAGE - 1>().has_value());
          if (history.is_PO<STAGE - 1>()) {
            auto iter = iterator_t(container);
            bool test_key_equal;
            if constexpr (STAGE == STAGE_STRING) {
              test_key_equal = (iter.get_key().type() == ns_oid_view_t::Type::MIN);
            } else {
              auto cmp = compare_to(key, iter.get_key());
              assert(cmp != MatchKindCMP::PO);
              test_key_equal = (cmp == MatchKindCMP::EQ);
            }
            if (test_key_equal) {
              return nxt_lower_bound(key, iter, history);
            } else {
              return smallest_result(iter);
            }
          }
        }
        // IS_BOTTOM || !history.is_PO<STAGE - 1>()
        auto iter = iterator_t(container);
        iter.seek_last();
        if constexpr (STAGE == STAGE_STRING) {
          assert(iter.get_key().type() == ns_oid_view_t::Type::MAX);
        } else {
          assert(compare_to(key, iter.get_key()) == MatchKindCMP::EQ);
        }
        if constexpr (IS_BOTTOM) {
          auto value_ptr = iter.get_p_value();
          return {{iter.index()}, MatchKindBS::EQ, value_ptr, MSTAT_EQ};
        } else {
          auto nxt_container = iter.get_nxt_container();
          auto nxt_result = NXT_STAGE_T::lower_bound(nxt_container, key, history);
          assert(!nxt_result.is_end());
          return result_t::from_nxt(iter.index(), nxt_result);
        }
      } else if (*history.get<STAGE>() == MatchKindCMP::NE) {
        exclude_last = true;
      }
    }
    auto iter = iterator_t(container);
    auto bs_match = iter.seek(key, exclude_last);
    if (iter.is_end()) {
      assert(!exclude_last);
      assert(bs_match == MatchKindBS::NE);
      history.set<STAGE>(MatchKindCMP::PO);
      return result_t::end();
    }
    history.set<STAGE>(bs_match == MatchKindBS::EQ ?
                       MatchKindCMP::EQ : MatchKindCMP::NE);
    if constexpr (IS_BOTTOM) {
      auto value_ptr = iter.get_p_value();
      return {{iter.index()}, bs_match, value_ptr,
              (bs_match == MatchKindBS::EQ ? MSTAT_EQ : MSTAT_NE0)};
    } else {
      if (bs_match == MatchKindBS::EQ) {
        return nxt_lower_bound(key, iter, history);
      } else {
        return smallest_result(iter);
      }
    }
  }
  
  /*
   * Lookup interfaces
   */

  static void lookup_largest_normalized(
      const container_t& container, search_position_t& position, const onode_t*& p_value) {
    if constexpr (STAGE == STAGE_LEFT) {
      lookup_largest(container, position, p_value);
      return;
    }
    position.index = 0;
    auto& pos_nxt = position.nxt;
    if constexpr (STAGE == STAGE_STRING) {
      lookup_largest(container, pos_nxt, p_value);
      return;
    }
    pos_nxt.index = 0;
    auto& pos_nxt_nxt = pos_nxt.nxt;
    if constexpr (STAGE == STAGE_RIGHT) {
      lookup_largest(container, pos_nxt_nxt, p_value);
      return;
    }
    assert(false);
  }

  static const value_t* get_p_value_normalized(
      const container_t& container, const search_position_t& position) {
    return get_p_value(container, cast_down<STAGE>(position));
  }

  static void get_index_view_normalized(
      const container_t& container, const search_position_t& position, index_view_t& output) {
    get_index_view(container, cast_down<STAGE>(position), output);
  }

  static staged_result_t<NODE_TYPE, STAGE_TOP> lower_bound_normalized(
      const container_t& container, const onode_key_t& key, MatchHistory& history) {
    auto&& result = lower_bound(container, key, history);
#ifndef NDEBUG
    if (result.is_end()) {
      assert(result.mstat == MSTAT_PO);
    } else {
      index_view_t index;
      get_index_view(container, result.position, index);
      assert_mstat(key, index, result.mstat);
    }
#endif
    if constexpr (container_t::FIELD_TYPE == field_type_t::N0) {
      // currently only internal node checks mstat
      if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
        if (result.mstat == MSTAT_NE2) {
          auto cmp = compare_to(key, container[result.position.index].shard_pool);
          assert(cmp != MatchKindCMP::PO);
          if (cmp != MatchKindCMP::EQ) {
            result.mstat = MSTAT_NE3;
          }
        }
      }
    }
    return normalize(std::move(result));
  }

  static std::ostream& dump(const container_t& container,
                            std::ostream& os,
                            const std::string& prefix,
                            size_t& size) {
    auto iter = iterator_t(container);
    assert(!iter.is_end());
    std::string prefix_blank(prefix.size(), ' ');
    const std::string* p_prefix = &prefix;
    do {
      std::ostringstream sos;
      sos << *p_prefix << iter.get_key() << ": ";
      std::string i_prefix = sos.str();
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        size += iter.size_to_nxt();
        NXT_STAGE_T::dump(nxt_container, os, i_prefix, size);
      } else {
        auto value_ptr = iter.get_p_value();
        size += iter.size();
        os << "\n" << i_prefix << *value_ptr << " " << size << "B";
      }
      if (iter.is_last()) {
        break;
      } else {
        ++iter;
        p_prefix = &prefix_blank;
      }
    } while (true);
    return os;
  }

  /*
   * WIP: Iterative interfaces
   */

  struct _BaseEmpty {};
  class _BaseWithNxtIterator {
   protected:
    typename NXT_STAGE_T::StagedIterator _nxt;
  };
  class StagedIterator
      : std::conditional_t<IS_BOTTOM, _BaseEmpty, _BaseWithNxtIterator> {
   public:
    StagedIterator() = default;
    bool valid() const { return iter.has_value(); }
    size_t index() const {
      return iter->index();
    }
    bool is_end() const { return iter->is_end(); }
    bool in_progress() const {
      assert(valid());
      if constexpr (!IS_BOTTOM) {
        if (this->_nxt.valid()) {
          if (this->_nxt.index() == 0) {
            return this->_nxt.in_progress();
          } else {
            return true;
          }
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    key_get_type get_key() const { return iter->get_key(); }

    iterator_t& get() { return *iter; }
    void set(const container_t& container) {
      assert(!valid());
      iter = iterator_t(container);
    }
    void set_end() { iter->set_end(); }
    typename NXT_STAGE_T::StagedIterator& nxt() {
      if constexpr (!IS_BOTTOM) {
        if (!this->_nxt.valid()) {
          auto nxt_container = iter->get_nxt_container();
          this->_nxt.set(nxt_container);
        }
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    typename NXT_STAGE_T::StagedIterator& get_nxt() {
      if constexpr (!IS_BOTTOM) {
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    StagedIterator& operator++() {
      if (iter->is_last()) {
        iter->set_end();
      } else {
        ++(*iter);
      }
      if constexpr (!IS_BOTTOM) {
        this->_nxt.reset();
      }
      return *this;
    }
    void reset() {
      if (valid()) {
        iter.reset();
        if constexpr (!IS_BOTTOM) {
          this->_nxt.reset();
        }
      }
    }
    std::ostream& print(std::ostream& os, bool is_top) const {
      if (valid()) {
        if (iter->is_end()) {
          return os << "END";
        } else {
          os << index();
        }
      } else {
        if (is_top) {
          return os << "invalid StagedIterator!";
        } else {
          os << "0!";
        }
      }
      if constexpr (!IS_BOTTOM) {
        os << ", ";
        return this->_nxt.print(os, false);
      } else {
        return os;
      }
    }
    friend std::ostream& operator<<(std::ostream& os, const StagedIterator& iter) {
      return iter.print(os, true);
    }
   private:
    std::optional<iterator_t> iter;
    size_t end_index;
  };

  static void recursively_locate_split(
      size_t& current_size, size_t extra_size,
      size_t target_size, StagedIterator& split_at) {
    assert(current_size <= target_size);
    iterator_t& iter = split_at.get();
    current_size = iter.seek_split(current_size, extra_size, target_size);
    if constexpr (!IS_BOTTOM) {
      NXT_STAGE_T::recursively_locate_split(
          current_size, extra_size + iter.size_to_nxt(),
          target_size, split_at.nxt());
    }
  }

  static void recursively_locate_split_inserted(
      size_t& current_size, size_t extra_size, size_t target_size,
      position_t& i_position, match_stage_t i_stage, size_t i_size,
      std::optional<bool>& i_to_left, StagedIterator& split_at) {
    assert(current_size <= target_size);
    assert(!i_to_left.has_value());
    iterator_t& iter = split_at.get();
    auto& i_index = i_position.index;
    if (i_stage == STAGE) {
      current_size = iter.template seek_split_inserted<true>(
          current_size, extra_size, target_size,
          i_index, i_size, i_to_left);
      assert(i_to_left.has_value());
      if (*i_to_left == false &&
          ((iter.is_end() && i_index == INDEX_END) ||
           iter.index() == i_index)) {
        // ...[s_index-1] |!| (i_index) [s_index]...
        return;
      }
      assert(!iter.is_end());
      if (iter.index() != 0) {
        extra_size = 0;
      }
    } else {
      if constexpr (!IS_BOTTOM) {
        assert(i_stage < STAGE);
        current_size = iter.template seek_split_inserted<false>(
            current_size, extra_size, target_size,
            i_index, i_size, i_to_left);
        assert(!iter.is_end());
        if (iter.index() != 0) {
          extra_size = 0;
        }
        if (!i_to_left.has_value()) {
          assert(iter.index() == i_index);
          NXT_STAGE_T::recursively_locate_split_inserted(
              current_size, extra_size + iter.size_to_nxt(), target_size,
              i_position.nxt, i_stage, i_size, i_to_left, split_at.nxt());
          assert(i_to_left.has_value());
          return;
        }
      } else {
        assert(false);
      }
    }
    if constexpr (!IS_BOTTOM) {
      NXT_STAGE_T::recursively_locate_split(
          current_size, extra_size + iter.size_to_nxt(),
          target_size, split_at.nxt());
    }
    return;
  }

  static bool locate_split(
      const container_t& container, size_t target_size,
      position_t& i_position, match_stage_t i_stage, size_t i_size,
      StagedIterator& split_at) {
    split_at.set(container);
    size_t current_size = 0;
    std::optional<bool> i_to_left;
    recursively_locate_split_inserted(
        current_size, 0, target_size,
        i_position, i_stage, i_size, i_to_left, split_at);
    std::cout << "  size_to_left=" << current_size
              << ", target_split_size=" << target_size
              << ", original_size=" << container.size_before(container.keys())
              << ", insert_size=" << i_size;
    assert(current_size <= target_size);
    return *i_to_left;
  }

  /*
   * container appender type system
   *   container_t::Appender(LogicalCachedExtent& dst, char* p_append)
   *   append(const container_t& src, size_t from, size_t items)
   *   wrap() -> char*
   * IF !IS_BOTTOM:
   *   open_nxt(const key_get_type&)
   *   open_nxt(const onode_key_t&)
   *       -> std::tuple<LogicalCachedExtent&, char*>
   *   wrap_nxt(char* p_append)
   * ELSE
   *   append(const onode_key_t& key, const value_t& value)
   */
  struct _BaseWithNxtAppender {
    typename NXT_STAGE_T::StagedAppender _nxt;
  };
  class StagedAppender
      : std::conditional_t<IS_BOTTOM, _BaseEmpty, _BaseWithNxtAppender> {
   public:
    StagedAppender() = default;
    ~StagedAppender() {
      assert(!require_wrap_nxt);
      assert(!valid());
    }
    bool valid() const { return appender.has_value(); }
    size_t index() const {
      assert(valid());
      return _index;
    }
    bool in_progress() const { return require_wrap_nxt; }
    void init(LogicalCachedExtent* p_dst, char* p_start) {
      assert(!valid());
      appender = typename container_t::Appender(p_dst, p_start);
      _index = 0;
    }
    // possible to make src_iter end if
    // to_index == INDEX_END && to_stage == STAGE
    void append_until(
        StagedIterator& src_iter, size_t& to_index, match_stage_t to_stage) {
      assert(!require_wrap_nxt);
      assert(to_stage <= STAGE);
      auto s_index = src_iter.index();
      src_iter.get().copy_out_until(*appender, to_index, to_stage);
      _index += (to_index - s_index);
    }
    void append(const onode_key_t& key, const onode_t& value) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        auto& nxt = open_nxt(key);
        nxt.append(key, value);
        wrap_nxt();
      } else {
        appender->append(key, value);
        ++_index;
      }
    }
    char* wrap() {
      assert(valid());
      assert(_index > 0);
      if constexpr (!IS_BOTTOM) {
        if (require_wrap_nxt) {
          wrap_nxt();
        }
      }
      auto ret = appender->wrap();
      appender.reset();
      return ret;
    }
    typename NXT_STAGE_T::StagedAppender& open_nxt(key_get_type paritial_key) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        require_wrap_nxt = true;
        auto [p_dst, p_append] = appender->open_nxt(paritial_key);
        this->_nxt.init(p_dst, p_append);
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    typename NXT_STAGE_T::StagedAppender& open_nxt(const onode_key_t& key) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        require_wrap_nxt = true;
        auto [p_dst, p_append] = appender->open_nxt(key);
        this->_nxt.init(p_dst, p_append);
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    typename NXT_STAGE_T::StagedAppender& get_nxt() {
      if constexpr (!IS_BOTTOM) {
        assert(require_wrap_nxt);
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    void wrap_nxt() {
      if constexpr (!IS_BOTTOM) {
        assert(require_wrap_nxt);
        require_wrap_nxt = false;
        auto p_append = this->_nxt.wrap();
        appender->wrap_nxt(p_append);
        ++_index;
      } else {
        assert(false);
      }
    }
   private:
    std::optional<typename container_t::Appender> appender;
    size_t _index;
    bool require_wrap_nxt = false;
  };

  static void _append_range(StagedIterator& src_iter, StagedAppender& appender,
                            size_t& to_index, match_stage_t stage) {
    if (src_iter.is_end()) {
      assert(to_index == INDEX_END);
      assert(stage == STAGE);
      to_index = src_iter.index();
      return;
    }
    if constexpr (!IS_BOTTOM) {
      if (appender.in_progress()) {
        // we are in the progress of appending
        auto to_index_nxt = INDEX_END;
        NXT_STAGE_T::_append_range(
            src_iter.nxt(), appender.get_nxt(),
            to_index_nxt, STAGE - 1);
        ++src_iter;
        appender.wrap_nxt();
      } else if (src_iter.in_progress()) {
        // cannot append the current item as-a-whole
        auto to_index_nxt = INDEX_END;
        NXT_STAGE_T::_append_range(
            src_iter.nxt(), appender.open_nxt(src_iter.get_key()),
            to_index_nxt, STAGE - 1);
        ++src_iter;
        appender.wrap_nxt();
      }
    }
    appender.append_until(src_iter, to_index, stage);
  }

  static void _append_into(StagedIterator& src_iter, StagedAppender& appender,
                           position_t& position, match_stage_t stage) {
    // reaches the last item
    if (stage == STAGE) {
      // done, end recursion
      if constexpr (!IS_BOTTOM) {
        position.nxt = position_t::nxt_t::begin();
      }
    } else {
      assert(stage < STAGE);
      // process append in the next stage
      NXT_STAGE_T::append_until(
          src_iter.nxt(), appender.open_nxt(src_iter.get_key()),
          position.nxt, stage);
    }
  }

  static void append_until(StagedIterator& src_iter, StagedAppender& appender,
                           position_t& position, match_stage_t stage) {
    size_t from_index = src_iter.index();
    size_t& to_index = position.index;
    assert(from_index <= to_index);
    if constexpr (IS_BOTTOM) {
      assert(stage == STAGE);
      appender.append_until(src_iter, to_index, stage);
    } else {
      assert(stage <= STAGE);
      if (src_iter.index() == to_index) {
        _append_into(src_iter, appender, position, stage);
      } else {
        _append_range(src_iter, appender, to_index, stage);
        _append_into(src_iter, appender, position, stage);
      }
    }
    to_index -= from_index;
  }

  static bool append_insert(const onode_key_t& key, const onode_t& value,
                            StagedIterator& src_iter, StagedAppender& appender,
                            match_stage_t stage) {
    assert(src_iter.valid());
    if (stage == STAGE) {
      appender.append(key, value);
      if (src_iter.is_end()) {
        return true;
      } else {
        return false;
      }
    } else {
      assert(stage < STAGE);
      if constexpr (!IS_BOTTOM) {
        auto nxt_is_end = NXT_STAGE_T::append_insert(
            key, value, src_iter.get_nxt(), appender.get_nxt(), stage);
        if (nxt_is_end) {
          appender.wrap_nxt();
          ++src_iter;
          if (src_iter.is_end()) {
            return true;
          }
        }
        return false;
      } else {
        assert(false);
      }
    }
  }

  static std::tuple<TrimType, size_t>
  recursively_trim(LogicalCachedExtent& extent, StagedIterator& trim_at) {
    if (!trim_at.valid()) {
      return {TrimType::BEFORE, 0u};
    }
    if (trim_at.is_end()) {
      return {TrimType::AFTER, 0u};
    }

    auto& iter = trim_at.get();
    if constexpr (!IS_BOTTOM) {
      auto [type, trimmed] = NXT_STAGE_T::recursively_trim(
          extent, trim_at.get_nxt());
      size_t trim_size;
      if (type == TrimType::AFTER) {
        if (iter.is_last()) {
          return {TrimType::AFTER, 0u};
        }
        ++iter;
        trim_size = iter.trim_until(extent);
      } else if (type == TrimType::BEFORE) {
        if (iter.index() == 0) {
          return {TrimType::BEFORE, 0u};
        }
        trim_size = iter.trim_until(extent);
      } else {
        trim_size = iter.trim_at(extent, trimmed);
      }
      return {TrimType::AT, trim_size};
    } else {
      if (iter.index() == 0) {
        return {TrimType::BEFORE, 0u};
      } else {
        auto trimmed = iter.trim_until(extent);
        return {TrimType::AT, trimmed};
      }
    }
  }

  static void trim(LogicalCachedExtent& extent, StagedIterator& trim_at) {
    auto [type, trimmed] = recursively_trim(extent, trim_at);
    if (type == TrimType::AFTER) {
      auto& iter = trim_at.get();
      assert(iter.is_end());
      iter.trim_until(extent);
    }
  }
};

template <typename NodeType, typename Enable = void> struct _node_to_stage_t;
template <typename NodeType>
struct _node_to_stage_t<NodeType,
    std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N0 ||
                     NodeType::FIELD_TYPE == field_type_t::N1>> {
  using type = staged<staged_params_node_01<NodeType>>;
};
template <typename NodeType>
struct _node_to_stage_t<NodeType,
    std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N2>> {
  using type = staged<staged_params_node_2<NodeType>>;
};
template <typename NodeType>
struct _node_to_stage_t<NodeType,
    std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N3>> {
  using type = staged<staged_params_node_3<NodeType>>;
};
template <typename NodeType>
using node_to_stage_t = typename _node_to_stage_t<NodeType>::type;

}
