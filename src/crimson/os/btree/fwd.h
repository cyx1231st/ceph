// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>

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

}
