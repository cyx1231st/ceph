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

#include <tuple>
#include "auth/Auth.h"
#include "SocketMessenger.h"
#include "SocketConnection.h"
#include "Dispatcher.h"
#include "msg/Message.h"

using namespace ceph::net;

SocketMessenger::SocketMessenger(const entity_name_t& myname)
  : Messenger{myname}
{}

void SocketMessenger::bind(const entity_addr_t& addr)
{
  if (addr.get_family() != AF_INET) {
    throw std::system_error(EAFNOSUPPORT, std::generic_category());
  }

  set_myaddr(addr);

  seastar::socket_address address(addr.in4_addr());
  seastar::listen_options lo;
  lo.reuse_address = true;
  listener = seastar::listen(address, lo);
}

seastar::future<> SocketMessenger::start(Dispatcher *disp)
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
            peer_addr.set_type(entity_addr_t::TYPE_DEFAULT);
            peer_addr.set_sockaddr(&paddr.as_posix_sockaddr());
            ConnectionRef conn = new SocketConnection(this, dispatcher,
                                                      get_myaddr(), peer_addr,
                                                      std::move(socket));
            // initiate the handshake
            // don't wait before accepting another, no throw
            conn->start_accept();
          });
      }).handle_exception_type([this] (const std::system_error& e) {
        // stop gracefully on connection_aborted
        if (e.code() != error::connection_aborted) {
          throw e;
        }
      });
  }

  return seastar::now();
}

ceph::net::ConnectionRef
SocketMessenger::connect(const entity_addr_t& peer_addr, entity_type_t peer_type)
{
  if (auto found = lookup_conn(peer_addr); found) {
    return found;
  }
  ConnectionRef conn = new SocketConnection(this, dispatcher,
                                            get_myaddr(), peer_addr);
  conn->start_connect(peer_type);
  return conn;
}

seastar::future<> SocketMessenger::shutdown()
{
  if (listener) {
    listener->abort_accept();
  }
  return seastar::parallel_for_each(acceptings.begin(), acceptings.end(),
    [this] (auto conn) {
      return conn->close();
    }).then([this] {
      ceph_assert(acceptings.empty());
      return seastar::parallel_for_each(connections.begin(), connections.end(),
        [this] (auto conn) {
          return conn.second->close();
        });
    }).finally([this] {
      ceph_assert(connections.empty());
    });
}

void SocketMessenger::set_default_policy(const SocketPolicy& p)
{
  policy_set.set_default(p);
}

void SocketMessenger::set_policy(entity_type_t peer_type,
				 const SocketPolicy& p)
{
  policy_set.set(peer_type, p);
}

void SocketMessenger::set_policy_throttler(entity_type_t peer_type,
					   Throttle* throttle)
{
  // only byte throttler is used in OSD
  policy_set.set_throttlers(peer_type, throttle, nullptr);
}

ceph::net::ConnectionRef SocketMessenger::lookup_conn(const entity_addr_t& addr)
{
  if (auto found = connections.find(addr);
      found != connections.end()) {
    return found->second;
  } else {
    return nullptr;
  }
}

void SocketMessenger::accept_conn(ConnectionRef conn)
{
  acceptings.insert(conn);
}

void SocketMessenger::unaccept_conn(ConnectionRef conn)
{
  acceptings.erase(conn);
}

void SocketMessenger::register_conn(ConnectionRef conn)
{
  auto [i, added] = connections.emplace(conn->get_peer_addr(), conn);
  std::ignore = i;
  ceph_assert(added);
}

void SocketMessenger::unregister_conn(ConnectionRef conn)
{
  ceph_assert(conn);
  auto found = connections.find(conn->get_peer_addr());
  ceph_assert(found != connections.end());
  ceph_assert(found->second == conn);
  connections.erase(found);
}

seastar::future<msgr_tag_t, bufferlist>
SocketMessenger::verify_authorizer(peer_type_t peer_type,
				   auth_proto_t protocol,
				   bufferlist& auth)
{
  return dispatcher->ms_verify_authorizer(peer_type, protocol, auth);
}

seastar::future<std::unique_ptr<AuthAuthorizer>>
SocketMessenger::get_authorizer(peer_type_t peer_type, bool force_new)
{
  return dispatcher->ms_get_authorizer(peer_type, force_new);
}
