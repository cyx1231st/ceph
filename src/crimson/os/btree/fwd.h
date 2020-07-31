// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <cstring>
#include <limits>
#include <string>

namespace crimson::os::seastore::onode {

using laddr_t = uint64_t;
using loff_t = uint32_t;

constexpr auto INDEX_END = std::numeric_limits<size_t>::max();

enum class MatchKindBS : int8_t { NE = -1, EQ = 0 };

enum class MatchKindCMP : int8_t { NE = -1, EQ = 0, PO };
inline MatchKindCMP toMatchKindCMP(int value) {
  if (value > 0) {
    return MatchKindCMP::PO;
  } else if (value < 0) {
    return MatchKindCMP::NE;
  } else {
    return MatchKindCMP::EQ;
  }
}
template <typename Type>
MatchKindCMP toMatchKindCMP(const Type& l, const Type& r) {
  int match = l - r;
  return toMatchKindCMP(match);
}

template <>
inline MatchKindCMP toMatchKindCMP<std::string>(
    const std::string& l, const std::string& r) {
  return toMatchKindCMP(l.compare(r));
}

inline MatchKindCMP toMatchKindCMP(
    const char* l, size_t l_len, const char* r, size_t r_len) {
  assert(l && l_len);
  assert(r && r_len);
  auto min_len = std::min(l_len, r_len);
  auto match = toMatchKindCMP(std::strncmp(l, r, min_len));
  if (match == MatchKindCMP::EQ) {
    return toMatchKindCMP(l_len, r_len);
  } else {
    return match;
  }
}

inline MatchKindCMP toMatchKindCMP(
    const std::string& l, const char* r, size_t r_len) {
  assert(r && r_len);
  return toMatchKindCMP(l.compare(0u, l.length(), r, r_len));
}

inline MatchKindCMP toMatchKindCMP(
    const char* l, size_t l_len, const std::string& r) {
  assert(l && l_len);
  return toMatchKindCMP(-r.compare(0u, r.length(), l, l_len));
}

inline MatchKindCMP reverse(MatchKindCMP cmp) {
  if (cmp == MatchKindCMP::NE) {
    return MatchKindCMP::PO;
  } else if (cmp == MatchKindCMP::PO) {
    return MatchKindCMP::NE;
  } else {
    return cmp;
  }
}

}
