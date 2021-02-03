// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pmem.h"

#include <string_view>
#include <type_traits>

#include <fcntl.h>
#include <libpmem2.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "include/ceph_assert.h"
#include "include/compat.h"
#include "common/errno.h"
#include "crimson/common/log.h"

namespace {

[[maybe_unused]] seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_pmem);
}

#define X_ASSERT(EXPR) do {                                     \
  if (!(EXPR)) { throw std::logic_error("Assertion failed!"); } \
} while(0)

template <typename ValueT, ValueT... _values>
class ValueToName {
 public:
  static constexpr auto SIZE = sizeof...(_values);
  constexpr ValueToName(const char* cstr_names)
      : names(), lengths(), values{_values...} {
    if (SIZE != 0) {
      X_ASSERT(std::strcmp(cstr_names, "") != 0);
      std::size_t index_name = 0;
      std::size_t name_length = 0;
      const char* iter_chars = cstr_names;
      names[index_name] = iter_chars;
      while (*iter_chars != '\0') {
        if (*iter_chars == ',') {
          X_ASSERT(name_length != 0);
          lengths[index_name] = name_length;
          name_length = 0;
          ++index_name;
          X_ASSERT(index_name < SIZE);
          ++iter_chars;
          X_ASSERT(*iter_chars == ' ');
          ++iter_chars;
          names[index_name] = iter_chars;
        }
        ++iter_chars;
        ++name_length;
      }
      X_ASSERT(name_length != 0);
      lengths[index_name] = name_length;
      X_ASSERT(index_name + 1 == SIZE);
    } else {
      X_ASSERT(std::strcmp(cstr_names, "") == 0);
    }
  }

  std::optional<std::string_view> toName(const ValueT& value) const {
    for (std::size_t i = 0; i < SIZE; ++i) {
      if (value == values[i]) {
        return std::make_optional<std::string_view>(names[i], lengths[i]);
      }
    }
    return std::nullopt;
  }

 private:
  const char* names[SIZE];
  std::size_t lengths[SIZE];
  ValueT values[SIZE];
};

template <bool HAS_ERRNO, typename ValueT>
inline void print_error(
    const char* operation,
    const std::optional<std::string_view>& e_name,
    const ValueT& e_value)
{
  assert(e_value < 0);
  if (e_name.has_value()) {
    logger().error("{} failed -- {}", operation, *e_name);
  } else {
    if constexpr (HAS_ERRNO) {
      logger().error("{} failed -- {}", operation, cpp_strerror(-e_value));
    } else {
      ceph_abort_msgf("%s unexpected error -- %d", operation, e_value);
    }
  }
}

#define PRINT_ERROR(operation, error_v, ...) do {       \
  static constexpr auto parser =                        \
    ValueToName<std::decay_t<decltype(error_v)>,        \
                ##__VA_ARGS__>(#__VA_ARGS__);           \
  static_assert(parser.SIZE != 0);                      \
  print_error<false>(operation,                         \
                     parser.toName(error_v),            \
                     error_v);                          \
} while(0)

#define PRINT_ERROR_ERRNO(operation, error_v, ...) do { \
  static constexpr auto parser =                        \
    ValueToName<std::decay_t<decltype(error_v)>,        \
                ##__VA_ARGS__>(#__VA_ARGS__);           \
  print_error<true>(operation,                          \
                    parser.toName(error_v),             \
                    error_v);                           \
} while(0)

}

namespace crimson::common::pmem2 {

pmem2_granularity to_pmem2_gran(const granularity_t& g)
{
  switch (g) {
  case granularity_t::BYTE:
    return PMEM2_GRANULARITY_BYTE;
  case granularity_t::CACHE_LINE:
    return PMEM2_GRANULARITY_CACHE_LINE;
  case granularity_t::PAGE:
    return PMEM2_GRANULARITY_PAGE;
  default:
    ceph_abort("impossible path");
  }
}

const char* errormsg()
{
  return pmem2_errormsg();
}

FileDesc::~FileDesc()
{
  logger().debug("pmem2::FileDesc: destructing {} ...", *this);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
}

FileDesc::URef FileDesc::open(const char* path, bool lock)
{
  logger().debug("pmem2::FileDesc: opening from \"{}\", lock={} ...",
                 path, lock);

  int fd = ::open(path, O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    logger().error("pmem2::FileDesc::open \"{}\" failed, got {}",
                   path, cpp_strerror(-errno));
    return nullptr;
  }

  struct flock l;
  std::memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fd, F_SETLK, &l);
  if (r < 0) {
    logger().error("pmem2::FileDesc::open: lock \"{}\" failed, got {}",
                   path, cpp_strerror(-errno));
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return nullptr;
  }

  auto ret = std::unique_ptr<FileDesc>(new FileDesc(fd, path));
  logger().debug("pmem2::FileDesc: created {}", *ret);
  return ret;
}

Source::~Source()
{
  logger().debug("pmem2::Source: destructing {} ...", *this);
  auto ret = pmem2_source_delete(&src);
  assert(ret == 0);
}

Source::IRef Source::create(FileDesc::URef&& fd)
{
  logger().debug("pmem2::Source: creating from {} ...", *fd);
  assert(fd);
  pmem2_source *src;
  int ret = pmem2_source_from_fd(&src, fd->fd);
  if (ret < 0) {
    PRINT_ERROR("pmem2::Source::create_from_fd", ret,
                PMEM2_E_INVALID_FILE_TYPE,
                -ENOMEM);
    return nullptr;
  }
  assert(ret == 0);

  std::size_t alignment;
  ret = pmem2_source_alignment(src, &alignment);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Source::create: get_alignment", ret,
                      PMEM2_E_INVALID_ALIGNMENT_VALUE,
                      PMEM2_E_INVALID_FILE_TYPE,
                      PMEM2_E_INVALID_ALIGNMENT_FORMAT);
    ret = pmem2_source_delete(&src);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);

  size_t device_id_len = 0;
  ret = pmem2_source_device_id(src, nullptr, &device_id_len);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Source::create: get_device_id_len", ret,
                      PMEM2_E_BUFFER_TOO_SMALL,
                      PMEM2_E_NOSUPP);
    ret = pmem2_source_delete(&src);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);
  assert(device_id_len > 0);

  auto ref_device_id = std::make_unique<char[]>(device_id_len);
  ret = pmem2_source_device_id(src, ref_device_id.get(), &device_id_len);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Source::create: get_device_id", ret,
                      PMEM2_E_BUFFER_TOO_SMALL,
                      PMEM2_E_NOSUPP);
    ret = pmem2_source_delete(&src);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);
  auto device_id = std::string(ref_device_id.get(), device_id_len);
  ref_device_id.reset();

  uint64_t device_usc;
  ret = pmem2_source_device_usc(src, &device_usc);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Source::create: get_device_usc", ret,
                      PMEM2_E_NOSUPP);
    ret = pmem2_source_delete(&src);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);

  std::size_t size;
  ret = pmem2_source_size(src, &size);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Source::create: get_size", ret,
                      PMEM2_E_NOSUPP, // not well documented
                      PMEM2_E_INVALID_FILE_HANDLE,
                      PMEM2_E_INVALID_FILE_TYPE,
                      PMEM2_E_INVALID_SIZE_FORMAT);
    ret = pmem2_source_delete(&src);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);

/* Not available in v1.10 yet.
  int numa_node;
  ret = pmem2_source_numa_node(src, &numa_node);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Source::create: get_numa_node", ret,
                      PMEM2_E_NOSUPP,
                      PMEM2_E_DAX_REGION_NOT_FOUND,
                      PMEM2_E_INVALID_FILE_TYPE);
    ret = pmem2_source_delete(&src);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);
*/

  auto source = IRef(new Source(
        src, std::move(fd), alignment, device_id, device_usc, size));
  logger().debug("pmem2::Source: created {}", *source);
  return source;
}

std::ostream& operator<<(std::ostream& os, const Source& src) {
  os << "Source(" << *src.fd
     << std::hex << ", align=0x" << src.align
     << ", size=0x" << src.size << std::dec
     << ", device_id=" << src.device_id
     << ", device_usc=" << src.device_usc
     << ")";
  return os;
}

Config::~Config()
{
  auto ret = pmem2_config_delete(&cfg);
  assert(ret == 0);
}

void Config::set_offset(std::size_t offset)
{
  auto ret = pmem2_config_set_offset(cfg, offset);
  assert(ret == 0);
}

void Config::set_length(std::size_t length)
{
  auto ret = pmem2_config_set_length(cfg, length);
  assert(ret == 0);
}

void Config::set_protection(
    bool exec, bool read, bool write, bool none)
{
  unsigned prot = 0;
  if (none) {
    assert(!exec && !read && !write);
    prot = PMEM2_PROT_NONE;
  } else {
    if (exec) {
      prot |= PMEM2_PROT_EXEC;
    }
    if (read) {
      prot |= PMEM2_PROT_READ;
    }
    if (write) {
      prot |= PMEM2_PROT_WRITE;
    }
  }
  auto ret = pmem2_config_set_protection(cfg, prot);
  if (ret < 0) {
    PRINT_ERROR("pmem2::Config::set_protection", ret,
                PMEM2_E_INVALID_PROT_FLAG);
    ceph_abort("fatal error");
  }
  assert(ret == 0);
}

void Config::set_sharing(Config::sharing_t type)
{
  pmem2_sharing_type pmem2_type;
  switch (type) {
  case sharing_t::SHARED:
    pmem2_type = PMEM2_SHARED;
    break;
  case sharing_t::PRIVATE:
    pmem2_type = PMEM2_PRIVATE;
    break;
  default:
    ceph_abort("impossible path");
  }
  auto ret = pmem2_config_set_sharing(cfg, pmem2_type);
  if (ret < 0) {
    PRINT_ERROR("pmem2::Config::set_sharing", ret,
                PMEM2_E_INVALID_SHARING_VALUE);
    ceph_abort("fatal error");
  }
  assert(ret == 0);
}

Config::URef Config::create(granularity_t g)
{
  pmem2_config* cfg = nullptr;
  auto ret = pmem2_config_new(&cfg);
  if (ret < 0) {
    PRINT_ERROR("pmem2::Config::create(): config_new", ret,
                -ENOMEM);
    return nullptr;
  }
  assert(ret == 0);
  assert(cfg != nullptr);

  // The application must explicityly set the granularity value for the
  // mapping.
  ret = pmem2_config_set_required_store_granularity(cfg, to_pmem2_gran(g));
  if (ret < 0) {
    PRINT_ERROR("pmem2::Config::create(): set_granularity", ret,
                PMEM2_E_GRANULARITY_NOT_SUPPORTED);
    ret = pmem2_config_delete(&cfg);
    assert(ret == 0);
    return nullptr;
  }
  assert(ret == 0);

  return std::unique_ptr<Config>(new Config(cfg, g));
}

std::ostream& operator<<(std::ostream& os, const Config& config)
{
  os << "Config(" << config.gran;
  if (config.prot.has_value()) {
    unsigned _prot = *config.prot;
    if (_prot == PMEM2_PROT_NONE) {
      os << ", NONE";
    } else {
      if (_prot & PMEM2_PROT_EXEC) {
        os << ", EXEC";
      }
      if (_prot & PMEM2_PROT_READ) {
        os << ", READ";
      }
      if (_prot & PMEM2_PROT_WRITE) {
        os << ", WRITE";
      }
    }
  }
  if (config.sharing.has_value()) {
    os << ", " << *config.sharing;
  }
  if (config.offset.has_value()) {
    os << ", offset=" << *config.offset;
  }
  if (config.length.has_value()) {
    os << ", length=" << *config.length;
  }
  os << ")";
  return os;
}

Map::Map(pmem2_map* map, Source::IRef src, void* addr, std::size_t size, granularity_t gran)
    : map{map}, src{src}, addr{addr}, size{size}, gran{gran}
{
  fp_memmove = pmem2_get_memmove_fn(map);
  fp_memcpy = pmem2_get_memcpy_fn(map);
  fp_memset = pmem2_get_memset_fn(map);
  fp_persist = pmem2_get_persist_fn(map);
  fp_flush = pmem2_get_flush_fn(map);
  fp_drain = pmem2_get_drain_fn(map);
}

const Map::flag_t Map::flags_t::NODRAIN = PMEM2_F_MEM_NODRAIN;
const Map::flag_t Map::flags_t::NOFLUSH = PMEM2_F_MEM_NOFLUSH;
const Map::flag_t Map::flags_t::NONTEMPORAL = PMEM2_F_MEM_NONTEMPORAL;
const Map::flag_t Map::flags_t::TEMPORAL = PMEM2_F_MEM_TEMPORAL;
const Map::flag_t Map::flags_t::WB = PMEM2_F_MEM_WB;
const Map::flag_t Map::flags_t::WC = PMEM2_F_MEM_WC;

Map::~Map()
{
  logger().debug("pmem2::Map: deleting {} ...", *this);
  auto ret = pmem2_map_delete(&map);
  if (ret < 0) {
    PRINT_ERROR("pmem2::Map::delete", ret,
                PMEM2_E_MAPPING_NOT_FOUND,
                -EINVAL);
    ceph_abort("fatal error");
  }
  assert(ret == 0);
}

void Map::deep_flush(void* ptr, size_t size)
{
  auto ret = pmem2_deep_flush(map, ptr, size);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Map::deep_flush", ret,
                      PMEM2_E_DEEP_FLUSH_RANGE,
                      PMEM2_E_DAX_REGION_NOT_FOUND);
    ceph_abort("fatal error");
  }
  assert(ret == 0);
}

Map::URef Map::create_new(Config::URef&& config, Source::IRef source)
{
  logger().debug("pmem2::Map: creating new from {}, {} ...", *config, *source);
  pmem2_map* map;
  auto ret = pmem2_map_new(&map, config->cfg, source->src);
  if (ret < 0) {
    PRINT_ERROR_ERRNO("pmem2::Map::new", ret,
                      PMEM2_E_GRANULARITY_NOT_SET,
                      PMEM2_E_GRANULARITY_NOT_SUPPORTED, // not well documented
                      PMEM2_E_MAP_RANGE,
                      PMEM2_E_SOURCE_EMPTY,
                      PMEM2_E_MAPPING_EXISTS,
                      PMEM2_E_OFFSET_UNALIGNED,
                      PMEM2_E_LENGTH_UNALIGNED,
                      PMEM2_E_SRC_DEVDAX_PRIVATE,
                      PMEM2_E_ADDRESS_UNALIGNED,
                      PMEM2_E_NOSUPP,
                      PMEM2_E_NO_ACCESS,
                      -EACCES,
                      -EAGAIN,
                      -EBADF,
                      -ENFILE,
                      -ENODEV,
                      -ENOMEM,
                      -EPERM,
                      -ETXTBSY,
                      // from pmem2_source_size and alignment
                      PMEM2_E_INVALID_FILE_HANDLE,
                      PMEM2_E_INVALID_FILE_TYPE,
                      PMEM2_E_INVALID_SIZE_FORMAT,
                      PMEM2_E_INVALID_ALIGNMENT_VALUE,
                      PMEM2_E_INVALID_ALIGNMENT_FORMAT);
    return nullptr;
  }
  assert(ret == 0);

  void* addr = pmem2_map_get_address(map);
  std::size_t size = pmem2_map_get_size(map);

  auto new_map =  std::unique_ptr<Map>(new Map(map, source, addr, size, config->gran));
  logger().debug("pmem2::Map: created {}", *new_map);
  return new_map;
}

Map::URef Map::create_from_existing(
    void* addr, size_t size, granularity_t gran, Source::IRef source)
{
  logger().debug("pmem2::Map: creating from existing {:x}-{:x}, {}, {} ...",
                 addr, size, gran, *source);
  pmem2_map* map;
  auto ret = pmem2_map_from_existing(
      &map, source->src, addr, size, to_pmem2_gran(gran));
  if (ret < 0) {
    PRINT_ERROR("pmem2::Map::from_existing", ret,
                PMEM2_E_MAPPING_EXISTS,
                PMEM2_E_MAP_EXISTS, // not well documented
                -ENOMEM);
    return nullptr;
  }
  assert(ret == 0);
  return std::unique_ptr<Map>(new Map(map, source, addr, size, gran));
}

std::ostream& operator<<(std::ostream& os, const Map& map)
{
  os << "Map(" << map.gran
     << std::hex << ", 0x" << map.addr
     << "-0x" << map.size << std::dec
     << ", from: " << *map.src
     << ")";
  return os;
}

}
