// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <ostream>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <crimson/common/type_helpers.h>

struct pmem2_config;
struct pmem2_map;
struct pmem2_source;

namespace crimson::common::pmem2 {

const char* errormsg();

// XXX: generalize FileDesc for reuse.
class FileDesc {
 public:
  FileDesc(const FileDesc&) = delete;
  FileDesc(FileDesc&&) = delete;
  FileDesc& operator=(const FileDesc&) = delete;
  FileDesc& operator=(FileDesc&&) = delete;
  ~FileDesc();

  using URef = std::unique_ptr<FileDesc>;
  static URef open(const char* path, bool lock);

 private:
  FileDesc(int fd, const char* path)
    : fd{fd}, path{path} { assert(fd >= 0); };

  int fd;
  const char* path;

  friend class Source;
  friend std::ostream& operator<<(std::ostream&, const FileDesc&);
};

inline std::ostream& operator<<(std::ostream& os, const FileDesc& fd) {
  return os << "FileDesc(fd=" << fd.fd << ", \"" << fd.path << "\")";
}

class Source : public boost::intrusive_ref_counter<
   Source, boost::thread_unsafe_counter> {
 public:
  Source(const Source&) = delete;
  Source(Source&&) = delete;
  Source& operator=(const Source&) = delete;
  Source& operator=(Source&&) = delete;
  ~Source();

  std::size_t get_alignment() const { return align; }
  std::string get_device_id() const { return device_id; }
  uint64_t get_device_usc() const { return device_usc; }
  std::size_t get_size() const { return size; }

  // Not available in v1.10 yet.
  // int get_numa_node() const { return numa_node; }

  using IRef = Ref<Source>;
  static IRef create(FileDesc::URef&&);

 private:
  Source(pmem2_source* src, FileDesc::URef&& fd,
         std::size_t align, const std::string& device_id,
         uint64_t device_usc, std::size_t size)
    : src{src}, fd{std::move(fd)}, align{align}, device_id{device_id},
      device_usc{device_usc}, size{size} {}

  pmem2_source* src;
  FileDesc::URef fd;
  std::size_t align;
  std::string device_id;
  uint64_t device_usc;
  std::size_t size;

  friend class Map;
  friend std::ostream& operator<<(std::ostream&, const Source&);

  /**
   * not implemented:
   *  - pmem2_source_fron_anon
   *  - pmem2_source_from_handle
   *  - pmem2_source_get_fd
   */
};

std::ostream& operator<<(std::ostream&, const Source&);

enum class granularity_t {
  BYTE = 0,
  CACHE_LINE,
  PAGE,
};

inline std::ostream& operator<<(
    std::ostream& os, const granularity_t& gran) {
  const char *const gran_names[] = {"BYTE", "CACHE_LINE", "PAGE"};
  return os << gran_names[static_cast<int>(gran)];
}

class Config {
 public:
  Config(const Config&) = delete;
  Config(Config&&) = delete;
  Config& operator=(const Config&) = delete;
  Config& operator=(Config&&) = delete;
  ~Config();

  void set_offset(std::size_t);
  void set_length(std::size_t);
  void set_protection(bool exec, bool read, bool write, bool none);

  enum class sharing_t {
    SHARED = 0,
    PRIVATE,
  };
  void set_sharing(sharing_t);

  using URef = std::unique_ptr<Config>;
  static URef create(granularity_t);

 private:
  Config(pmem2_config* cfg, const granularity_t& gran)
    : cfg{cfg}, gran{gran} {}

  pmem2_config* cfg;
  granularity_t gran;

  // optional settings
  std::optional<std::size_t> offset;
  std::optional<std::size_t> length;
  std::optional<unsigned> prot;
  std::optional<sharing_t> sharing;

  friend class Map;
  friend std::ostream& operator<<(std::ostream&, const Config&);

  /**
   * not implemented:
   *  - pmem2_config_set_vm_reservation
   */
};

inline std::ostream& operator<<(
    std::ostream& os, const Config::sharing_t& s_type) {
  const char *const type_names[] = {"SHARED", "PRIVATE"};
  return os << type_names[static_cast<int>(s_type)];
}

std::ostream& operator<<(std::ostream&, const Config&);

class Map {
 public:
  Map(const Map&) = delete;
  Map(Map&&) = delete;
  Map& operator=(const Map&) = delete;
  Map& operator=(Map&&) = delete;
  ~Map();

  void* get_address() const { return addr; }
  std::size_t get_size() const { return size; }
  granularity_t get_granularity() const { return gran; }

  using URef = std::unique_ptr<Map>;
  std::optional<URef> from_existing(void*, size_t, granularity_t);

  using flag_t = unsigned;
  struct flags_t {
    static const flag_t NODRAIN;
    static const flag_t NOFLUSH;
    static const flag_t NONTEMPORAL;
    static const flag_t TEMPORAL;
    static const flag_t WB;
    static const flag_t WC;
  };
  // PMem operations
  void* memmove(void* dst, const void* src, std::size_t len, flag_t flags) {
    return (*fp_memmove)(dst, src, len, flags);
  }
  void* memcpy(void* dst, const void* src, std::size_t len, flag_t flags) {
    return (*fp_memcpy)(dst, src, len, flags);
  }
  void* memset(void* dst, int c, std::size_t len, flag_t flags) {
    return (*fp_memset)(dst, c, len, flags);
  }
  void persist(const void* ptr, std::size_t size) {
    return (*fp_persist)(ptr, size);
  }
  void flush(const void* ptr, std::size_t size) {
    return (*fp_flush)(ptr, size);
  }
  void drain() {
    return (*fp_drain)();
  }
  void deep_flush(void* ptr, std::size_t size);

  static URef create_new(Config::URef&&, Source::IRef);

  static URef create_from_existing(void*, size_t, granularity_t, Source::IRef);

 private:
  Map(pmem2_map* map, Source::IRef src, void* addr, std::size_t size, granularity_t gran);

  pmem2_map* map;
  Source::IRef src;
  void* addr;
  std::size_t size;
  granularity_t gran;

  template <typename T>
  struct remove_member_type {};

  template <typename C, typename T>
  struct remove_member_type<T C::*> {
    using type = T;
  };

  template <typename T>
  using remove_member_t = typename remove_member_type<T>::type;

  using fn_memmove_t = remove_member_t<decltype(&Map::memmove)>;
  using fn_memcpy_t = remove_member_t<decltype(&Map::memcpy)>;
  using fn_memset_t = remove_member_t<decltype(&Map::memset)>;
  using fn_persist_t = remove_member_t<decltype(&Map::persist)>;
  using fn_flush_t = remove_member_t<decltype(&Map::flush)>;
  using fn_drain_t = remove_member_t<decltype(&Map::drain)>;

  fn_memmove_t* fp_memmove;
  fn_memcpy_t* fp_memcpy;
  fn_memset_t* fp_memset;
  fn_persist_t* fp_persist;
  fn_flush_t* fp_flush;
  fn_drain_t* fp_drain;

  friend std::ostream& operator<<(std::ostream&, const Map&);
};

std::ostream& operator<<(std::ostream&, const Map&);

/**
 * not implemented:
 *  - pmem2_badblock_*
 *  - pmem2_vm_reservation_*
 */

};
