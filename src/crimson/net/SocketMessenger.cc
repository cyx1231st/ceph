// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "SocketMessenger.h"

#include <tuple>
#include <boost/functional/hash.hpp>

#include "auth/Auth.h"
#include "Errors.h"
#include "Dispatcher.h"
#include "Socket.h"

using namespace ceph::net;

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_ms);
  }
}

template <class Placement>
SocketMessenger<Placement>::SocketMessenger(const entity_name_t& myname,
                                 const std::string& logic_name,
                                 uint32_t nonce,
                                 const Placement& placement)
  : Messenger{myname},
    placement{placement},
    sid{seastar::engine().cpu_id()},
    logic_name{logic_name},
    nonce{nonce}
{}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::set_myaddrs(const entity_addrvec_t& addrs)
{
  auto my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
  return this->container().invoke_on_all([my_addrs](auto& msgr) {
      return msgr.Messenger::set_myaddrs(my_addrs);
    });
}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::bind(const entity_addrvec_t& addrs)
{
  ceph_assert(addrs.legacy_addr().get_family() == AF_INET);
  auto my_addrs = addrs;
  for (auto& addr : my_addrs.v) {
    addr.nonce = nonce;
  }
  return this->container().invoke_on_all([my_addrs](auto& msgr) {
      msgr.do_bind(my_addrs);
    });
}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::start(Dispatcher *disp) {
  return this->container().invoke_on_all([disp](auto& msgr) {
      return msgr.do_start(disp->get_local_shard());
    });
}

template <class Placement>
seastar::future<ceph::net::ConnectionXRef>
SocketMessenger<Placement>::connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type)
{
  auto shard = placement.locate(peer_addr);
  return this->container().invoke_on(shard, [peer_addr, peer_type](auto& msgr) {
      return msgr.do_connect(peer_addr, peer_type);
    }).then([](seastar::foreign_ptr<ConnectionRef>&& conn) {
      return seastar::make_lw_shared<seastar::foreign_ptr<ConnectionRef>>(std::move(conn));
    });
}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::shutdown()
{
  return this->container().invoke_on_all([](auto& msgr) {
      return msgr.do_shutdown();
    }).finally([this] {
      return this->container().invoke_on_all([](auto& msgr) {
          msgr.shutdown_promise.set_value();
        });
    });
}

template <class Placement>
void SocketMessenger<Placement>::do_bind(const entity_addrvec_t& addrs)
{
  Messenger::set_myaddrs(addrs);

  // TODO: v2: listen on multiple addresses
  seastar::socket_address address(addrs.legacy_addr().in4_addr());
  seastar::listen_options lo;
  lo.reuse_address = true;
  listener = seastar::listen(address, lo);
}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::do_start(Dispatcher *disp)
{
  dispatcher = disp;

  // start listening if bind() was called
  if (listener) {
    seastar::keep_doing([this] {
        return listener->accept()
          .then([this] (seastar::connected_socket socket,
                        seastar::socket_address paddr) {
            // allocate the connection
            entity_addr_t peer_addr;
            peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
            auto shard = placement.locate(peer_addr);
#warning fixme
            // we currently do dangerous i/o from a Connection core, different from the Socket core.
            auto sock = seastar::make_foreign(std::make_unique<Socket>(std::move(socket)));
            // don't wait before accepting another
            this->container().invoke_on(shard, [sock = std::move(sock), peer_addr, this](auto& msgr) mutable {
                SocketConnectionRef<Placement> conn = seastar::make_shared<SocketConnection<Placement>>(msgr, *msgr.dispatcher);
                conn->start_accept(std::move(sock), peer_addr);
              });
          });
      }).handle_exception_type([this] (const std::system_error& e) {
        // stop gracefully on connection_aborted
        if (e.code() != error::connection_aborted) {
          logger().error("{} unexpected error during accept: {}", *this, e);
        }
      });
  }

  return seastar::now();
}

template <class Placement>
seastar::foreign_ptr<ceph::net::ConnectionRef>
SocketMessenger<Placement>::do_connect(const entity_addr_t& peer_addr, const entity_type_t& peer_type)
{
  if (auto found = lookup_conn(peer_addr); found) {
    return seastar::make_foreign(found->shared_from_this());
  }
  SocketConnectionRef<Placement> conn = seastar::make_shared<SocketConnection<Placement>>(*this, *dispatcher);
  conn->start_connect(peer_addr, peer_type);
  return seastar::make_foreign(conn->shared_from_this());
}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::do_shutdown()
{
  if (listener) {
    listener->abort_accept();
  }
  // close all connections
  return seastar::parallel_for_each(accepting_conns, [] (auto conn) {
      return conn->close();
    }).then([this] {
      ceph_assert(accepting_conns.empty());
      return seastar::parallel_for_each(connections, [] (auto conn) {
          return conn.second->close();
        });
    }).finally([this] {
      ceph_assert(connections.empty());
    });
}

template <class Placement>
seastar::future<> SocketMessenger<Placement>::learned_addr(const entity_addr_t &peer_addr_for_me)
{
  if (!get_myaddr().is_blank_ip()) {
    // already learned or binded
    return seastar::now();
  }

  // Only learn IP address if blank.
  entity_addr_t addr = get_myaddr();
  addr.u = peer_addr_for_me.u;
  addr.set_type(peer_addr_for_me.get_type());
  addr.set_port(get_myaddr().get_port());
  return set_myaddrs(entity_addrvec_t{addr});
}

template <class Placement>
void SocketMessenger<Placement>::set_default_policy(const SocketPolicy& p)
{
  policy_set.set_default(p);
}

template <class Placement>
void SocketMessenger<Placement>::set_policy(entity_type_t peer_type,
				 const SocketPolicy& p)
{
  policy_set.set(peer_type, p);
}

template <class Placement>
void SocketMessenger<Placement>::set_policy_throttler(entity_type_t peer_type,
					   Throttle* throttle)
{
  // only byte throttler is used in OSD
  policy_set.set_throttlers(peer_type, throttle, nullptr);
}

template <class Placement>
ceph::net::SocketConnectionRef<Placement> SocketMessenger<Placement>::lookup_conn(const entity_addr_t& addr)
{
  if (auto found = connections.find(addr);
      found != connections.end()) {
    return found->second;
  } else {
    return nullptr;
  }
}

template <class Placement>
void SocketMessenger<Placement>::accept_conn(SocketConnectionRef<Placement> conn)
{
  accepting_conns.insert(conn);
}

template <class Placement>
void SocketMessenger<Placement>::unaccept_conn(SocketConnectionRef<Placement> conn)
{
  accepting_conns.erase(conn);
}

template <class Placement>
void SocketMessenger<Placement>::register_conn(SocketConnectionRef<Placement> conn)
{
  auto [i, added] = connections.emplace(conn->get_peer_addr(), conn);
  std::ignore = i;
  ceph_assert(added);
}

template <>
void SocketMessenger<placement_policy::Pinned>::register_conn(SocketConnectionRef<placement_policy::Pinned> conn)
{
  ceph_assert(sid == placement.where);
  auto [i, added] = connections.emplace(conn->get_peer_addr(), conn);
  std::ignore = i;
  ceph_assert(added);
}

template <class Placement>
void SocketMessenger<Placement>::unregister_conn(SocketConnectionRef<Placement> conn)
{
  ceph_assert(conn);
  auto found = connections.find(conn->get_peer_addr());
  ceph_assert(found != connections.end());
  ceph_assert(found->second == conn);
  connections.erase(found);
}

template class SocketMessenger<placement_policy::Pinned>;
template class SocketMessenger<placement_policy::ShardByAddr>;
