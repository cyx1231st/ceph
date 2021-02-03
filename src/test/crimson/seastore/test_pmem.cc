// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include "crimson/common/log.h"
#include "crimson/common/pmem.h"

#include "include/ceph_assert.h"
#include "test/crimson/gtest_seastar.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }

  uint64_t* get_p_value(void* addr) {
    return (uint64_t*) (char*)addr + 4096 + 8;
  }
}

struct a_basic_test_t : public seastar_test_suite_t {};

TEST_F(a_basic_test_t, 1_basic_sizes)
{
  using namespace crimson::common::pmem2;

  const char* path = "/dev/dax0.0";
  auto fd = FileDesc::open(path, true);
  ceph_assert(fd);

  auto source = Source::create(std::move(fd));
  ceph_assert(source);
  auto size = source->get_alignment() * 8;

  auto config = Config::create(granularity_t::CACHE_LINE);
  ceph_assert(config);
  config->set_offset(0);
  config->set_length(size);
  auto map = Map::create_new(std::move(config), source);
  ceph_assert(map);

  auto config_dup = Config::create(granularity_t::CACHE_LINE);
  ceph_assert(config_dup);
  config_dup->set_offset(0);
  config_dup->set_length(size);
  auto map_dup = Map::create_new(std::move(config_dup), source);
  ceph_assert(map_dup);

  auto config1 = Config::create(granularity_t::CACHE_LINE);
  ceph_assert(config1);
  config1->set_offset(size);
  config1->set_length(size);
  auto map1 = Map::create_new(std::move(config1), source);
  ceph_assert(map1);

  auto p_value_from_map = get_p_value(map->get_address());
  auto p_value_from_map_dup = get_p_value(map_dup->get_address());
  auto p_value_from_map1 = get_p_value(map1->get_address());
  logger().info("map: {}", *p_value_from_map);
  logger().info("map_dup: {}", *p_value_from_map_dup);
  logger().info("map1: {}", *p_value_from_map1);

  std::random_device rd;
  auto random1 = rd();
  auto random2 = rd();
  auto random3 = rd();
  logger().info("set random value: {}, {}, {}", random1, random2, random3);
  *p_value_from_map = random1;
  *p_value_from_map_dup = random2;
  *p_value_from_map1 = random3;

  map->persist(p_value_from_map, sizeof(uint64_t));
  map_dup->persist(p_value_from_map_dup, sizeof(uint64_t));
  map1->persist(p_value_from_map1, sizeof(uint64_t));

  logger().info("map: {}", *p_value_from_map);
  logger().info("map_dup: {}", *p_value_from_map_dup);
  logger().info("map1: {}", *p_value_from_map1);
}
