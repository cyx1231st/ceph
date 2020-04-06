// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "heartbeat.h"

#include <boost/range/join.hpp>

#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/formatter.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/shard_services.h"
#include "crimson/mon/MonClient.h"

#include "osd/OSDMap.h"

using crimson::common::local_conf;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

Heartbeat::Heartbeat(osd_id_t whoami,
                     const crimson::osd::ShardServices& service,
                     crimson::mon::Client& monc,
                     crimson::net::MessengerRef front_msgr,
                     crimson::net::MessengerRef back_msgr)
  : whoami{whoami},
    service{service},
    monc{monc},
    front_msgr{front_msgr},
    back_msgr{back_msgr},
    // do this in background
    timer{[this] {
      check_and_report_failure();
      (void)send_heartbeats();
    }},
    failing_peers{*this}
{}

seastar::future<> Heartbeat::start(entity_addrvec_t front_addrs,
                                   entity_addrvec_t back_addrs)
{
  logger().info("heartbeat: start");
  // i only care about the address, so any unused port would work
  for (auto& addr : boost::join(front_addrs.v, back_addrs.v)) {
    addr.set_port(0);
  }

  using crimson::net::SocketPolicy;
  front_msgr->set_policy(entity_name_t::TYPE_OSD,
                         SocketPolicy::lossy_client(0));
  back_msgr->set_policy(entity_name_t::TYPE_OSD,
                        SocketPolicy::lossy_client(0));
  return seastar::when_all_succeed(start_messenger(*front_msgr, front_addrs),
                                   start_messenger(*back_msgr, back_addrs))
    .then([this] {
      timer.arm_periodic(
        std::chrono::seconds(local_conf()->osd_heartbeat_interval));
    });
}

seastar::future<>
Heartbeat::start_messenger(crimson::net::Messenger& msgr,
                           const entity_addrvec_t& addrs)
{
  return msgr.try_bind(addrs,
                       local_conf()->ms_bind_port_min,
                       local_conf()->ms_bind_port_max).then([&msgr, this] {
    return msgr.start(this);
  });
}

seastar::future<> Heartbeat::stop()
{
  return seastar::when_all_succeed(front_msgr->shutdown(),
                                   back_msgr->shutdown());
}

const entity_addrvec_t& Heartbeat::get_front_addrs() const
{
  return front_msgr->get_myaddrs();
}

const entity_addrvec_t& Heartbeat::get_back_addrs() const
{
  return back_msgr->get_myaddrs();
}

void Heartbeat::set_require_authorizer(bool require_authorizer)
{
  if (front_msgr->get_require_authorizer() != require_authorizer) {
    front_msgr->set_require_authorizer(require_authorizer);
    back_msgr->set_require_authorizer(require_authorizer);
  }
}

void Heartbeat::add_peer(osd_id_t peer, epoch_t epoch)
{
  auto [iter, added] = peers.try_emplace(peer, *this, peer);
  auto& peer_info = iter->second;
  peer_info.set_epoch(epoch);
}

Heartbeat::osds_t Heartbeat::remove_down_peers()
{
  osds_t osds;
  for (auto& [osd, peer_info] : peers) {
    auto osdmap = service.get_osdmap_service().get_map();
    if (!osdmap->is_up(osd)) {
      remove_peer(osd);
    } else if (peer_info.get_epoch() < osdmap->get_epoch()) {
      osds.push_back(osd);
    }
  }
  return osds;
}

void Heartbeat::add_reporter_peers(int whoami)
{
  auto osdmap = service.get_osdmap_service().get_map();
  // include next and previous up osds to ensure we have a fully-connected set
  set<int> want;
  if (auto next = osdmap->get_next_up_osd_after(whoami); next >= 0) {
    want.insert(next);
  }
  if (auto prev = osdmap->get_previous_up_osd_before(whoami); prev >= 0) {
    want.insert(prev);
  }
  // make sure we have at least **min_down** osds coming from different
  // subtree level (e.g., hosts) for fast failure detection.
  auto min_down = local_conf().get_val<uint64_t>("mon_osd_min_down_reporters");
  auto subtree = local_conf().get_val<string>("mon_osd_reporter_subtree_level");
  osdmap->get_random_up_osds_by_subtree(
    whoami, subtree, min_down, want, &want);
  auto epoch = osdmap->get_epoch();
  for (int osd : want) {
    add_peer(osd, epoch);
  };
}

void Heartbeat::update_peers(int whoami)
{
  const auto min_peers = static_cast<size_t>(
    local_conf().get_val<int64_t>("osd_heartbeat_min_peers"));
  add_reporter_peers(whoami);
  auto extra = remove_down_peers();
  // too many?
  for (auto& osd : extra) {
    if (peers.size() <= min_peers) {
      break;
    }
    remove_peer(osd);
  }
  // or too few?
  auto osdmap = service.get_osdmap_service().get_map();
  auto epoch = osdmap->get_epoch();
  for (auto next = osdmap->get_next_up_osd_after(whoami);
    peers.size() < min_peers && next >= 0 && next != whoami;
    next = osdmap->get_next_up_osd_after(next)) {
    add_peer(next, epoch);
  }
}

void Heartbeat::remove_peer(osd_id_t peer)
{
  auto found = peers.find(peer);
  assert(found != peers.end());
  peers.erase(peer);
}

seastar::future<> Heartbeat::ms_dispatch(crimson::net::Connection* conn,
                                         MessageRef m)
{
  switch (m->get_type()) {
  case MSG_OSD_PING:
    return handle_osd_ping(conn, boost::static_pointer_cast<MOSDPing>(m));
  default:
    return seastar::now();
  }
}

seastar::future<> Heartbeat::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  auto peer = conn->get_peer_id();
  if (conn->get_peer_type() != entity_name_t::TYPE_OSD ||
      peer == entity_name_t::NEW) {
    return seastar::now();
  }
  if (auto found = peers.find(peer);
      found != peers.end()) {
    found->second.handle_reset(conn, is_replace);
  }
  return seastar::now();
}

seastar::future<> Heartbeat::ms_handle_connect(crimson::net::ConnectionRef conn)
{
  auto peer = conn->get_peer_id();
  if (conn->get_peer_type() != entity_name_t::TYPE_OSD ||
      peer == entity_name_t::NEW) {
    return seastar::now();
  }
  if (auto found = peers.find(peer);
      found != peers.end()) {
    found->second.handle_connect(conn);
  }
  return seastar::now();
}

seastar::future<> Heartbeat::ms_handle_accept(crimson::net::ConnectionRef conn)
{
  auto peer = conn->get_peer_id();
  if (conn->get_peer_type() != entity_name_t::TYPE_OSD ||
      peer == entity_name_t::NEW) {
    return seastar::now();
  }
  if (auto found = peers.find(peer);
      found != peers.end()) {
    found->second.handle_accept(conn);
  }
  return seastar::now();
}

seastar::future<> Heartbeat::handle_osd_ping(crimson::net::Connection* conn,
                                             Ref<MOSDPing> m)
{
  switch (m->op) {
  case MOSDPing::PING:
    return handle_ping(conn, m);
  case MOSDPing::PING_REPLY:
    return handle_reply(conn, m);
  case MOSDPing::YOU_DIED:
    return handle_you_died();
  default:
    return seastar::now();
  }
}

seastar::future<> Heartbeat::handle_ping(crimson::net::Connection* conn,
                                         Ref<MOSDPing> m)
{
  auto min_message = static_cast<uint32_t>(
    local_conf()->osd_heartbeat_min_size);
  auto reply =
    make_message<MOSDPing>(
      m->fsid,
      service.get_osdmap_service().get_map()->get_epoch(),
      MOSDPing::PING_REPLY,
      m->ping_stamp,
      m->mono_ping_stamp,
      service.get_mnow(),
      service.get_osdmap_service().get_up_epoch(),
      min_message);
  return conn->send(reply);
}

seastar::future<> Heartbeat::handle_reply(crimson::net::Connection* conn,
                                          Ref<MOSDPing> m)
{
  const osd_id_t from = m->get_source().num();
  auto found = peers.find(from);
  if (found == peers.end()) {
    // stale reply
    return seastar::now();
  }
  auto& peer_info = found->second;
  return peer_info.handle_reply(conn, m);
}

seastar::future<> Heartbeat::handle_you_died()
{
  // TODO: ask for newer osdmap
  return seastar::now();
}

void Heartbeat::check_and_report_failure()
{
  const auto now = clock::now();
  for (const auto& [osd, peer_info] : peers) {
    auto failed_since = peer_info.failed_since(now);
    if (!clock::is_zero(failed_since)) {
      // safe to ignore the future because there is no dependent reference
      // and messages will be sent in order
      (void) failing_peers.add_pending(osd, failed_since, now);
    }
  }
}

seastar::future<> Heartbeat::send_heartbeats()
{
  const auto mnow = service.get_mnow();
  const auto now = clock::now();

  std::vector<seastar::future<>> futures;
  for (auto& [osd, peer_info] : peers) {
    peer_info.send_heartbeat(now, mnow, futures);
  }
  return seastar::when_all_succeed(futures.begin(), futures.end());
}

Heartbeat::Peer::Peer(Heartbeat& heartbeat, osd_id_t peer)
  : heartbeat(heartbeat), peer(peer)
{
  logger().info("Heartbeat::Peer: osd.{} added", peer);
  connect_front();
  connect_back();
}

Heartbeat::Peer::~Peer()
{
  logger().info("Heartbeat::Peer: osd.{} removed", peer);
  if (con_front) {
    con_front->mark_down();
  }
  if (con_back) {
    con_back->mark_down();
  }
}

Heartbeat::clock::time_point
Heartbeat::Peer::failed_since(clock::time_point now) const
{
  if (inspect_health_state(now) == health_state::UNHEALTHY) {
    auto oldest_deadline = ping_history.begin()->second.deadline;
    auto failed_since = std::min(last_rx_back, last_rx_front);
    if (clock::is_zero(failed_since)) {
      logger().error("Heartbeat::Peer::check(): no reply from osd.{} "
                     "ever on either front or back, first ping sent {} "
                     "(oldest deadline {})",
                     peer, first_tx, oldest_deadline);
      failed_since = first_tx;
    } else {
      logger().error("Heartbeat::Peer::check(): no reply from osd.{} "
                     "since back {} front {} (oldest deadline {})",
                     peer, last_rx_back, last_rx_front, oldest_deadline);
    }
    return failed_since;
  } else {
    return clock::zero();
  }
}

void Heartbeat::Peer::do_send_heartbeat(
    clock::time_point now,
    ceph::signedspan mnow,
    std::vector<seastar::future<>>* futures)
{
  assert(session_started);
  const utime_t sent_stamp{now};
  const auto deadline =
    now + std::chrono::seconds(local_conf()->osd_heartbeat_grace);
  [[maybe_unused]] auto [reply, added] =
    ping_history.emplace(sent_stamp, reply_t{deadline, 0});
  for (auto& con : {con_front, con_back}) {
    auto min_message = static_cast<uint32_t>(
      local_conf()->osd_heartbeat_min_size);
    auto ping = make_message<MOSDPing>(
      heartbeat.monc.get_fsid(),
      heartbeat.service.get_osdmap_service().get_map()->get_epoch(),
      MOSDPing::PING,
      sent_stamp,
      mnow,
      mnow,
      heartbeat.service.get_osdmap_service().get_up_epoch(),
      min_message);
    reply->second.unacknowledged++;
    if (futures) {
      futures->push_back(con->send(std::move(ping)));
    }
  }
}

void Heartbeat::Peer::send_heartbeat(
    clock::time_point now,
    ceph::signedspan mnow,
    std::vector<seastar::future<>>& futures)
{
  if (never_send_never_receive()) {
    first_tx = now;
  }
  last_tx = now;

  if (session_started) {
    do_send_heartbeat(now, mnow, &futures);

    // validate connection addresses
    const auto osdmap = heartbeat.service.get_osdmap_service().get_map();
    const auto front_addr = osdmap->get_hb_front_addrs(peer).front();
    if (con_front->get_peer_addr() != front_addr) {
      logger().info("Heartbeat::Peer::send_heartbeat(): "
                    "peer osd.{} con_front has new address {} over {}, reset",
                    peer, front_addr, con_front->get_peer_addr());
      con_front->mark_down();
      has_racing = false;
      handle_reset(con_front, false);
    }
    const auto back_addr = osdmap->get_hb_back_addrs(peer).front();
    if (con_back->get_peer_addr() != back_addr) {
      logger().info("Heartbeat::Peer::send_heartbeat(): "
                    "peer osd.{} con_back has new address {} over {}, reset",
                    peer, back_addr, con_back->get_peer_addr());
      con_back->mark_down();
      has_racing = false;
      handle_reset(con_back, false);
    }
  } else {
    // we should send MOSDPing but still cannot at this moment
    if (pending_send) {
      // we have already pending for a entire heartbeat interval
      logger().warn("Heartbeat::Peer::send_heartbeat(): "
                    "heartbeat to {} is still pending...", peer);
      has_racing = false;
      // retry con_front if still pending
      if (!front_ready) {
        if (con_front) {
          con_front->mark_down();
        }
        connect_front();
      }
      // retry con_back if still pending
      if (!back_ready) {
        if (con_back) {
          con_back->mark_down();
        }
        connect_back();
      }
    } else {
      logger().info("Heartbeat::Peer::send_heartbeat(): "
                    "heartbeat to {} is pending send...", peer);
      // maintain an entry in ping_history for unhealthy check
      if (ping_history.empty()) {
        const utime_t sent_stamp{now};
        const auto deadline =
          now + std::chrono::seconds(local_conf()->osd_heartbeat_grace);
        [[maybe_unused]] auto [reply, added] =
          ping_history.emplace(sent_stamp, reply_t{deadline, 0});
      } else { // the entry is already added
        assert(ping_history.size() == 1);
      }
      pending_send = true;
    }
  }
}

seastar::future<> Heartbeat::Peer::handle_reply(
    crimson::net::Connection* conn, Ref<MOSDPing> m)
{
  if (!session_started) {
    // we haven't sent any ping yet
    return seastar::now();
  }
  auto ping = ping_history.find(m->ping_stamp);
  if (ping == ping_history.end()) {
    // old replies, deprecated by newly sent pings.
    return seastar::now();
  }
  const auto now = clock::now();
  auto& unacked = ping->second.unacknowledged;
  assert(unacked);
  if (conn == con_back.get()) {
    last_rx_back = now;
    unacked--;
  } else if (conn == con_front.get()) {
    last_rx_front = now;
    unacked--;
  }
  if (unacked == 0) {
    ping_history.erase(ping_history.begin(), ++ping);
  }
  if (inspect_health_state(now) == health_state::HEALTHY) {
    return heartbeat.failing_peers.cancel_one(peer);
  }
  return seastar::now();
}

void Heartbeat::Peer::handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  if (con_front == conn) {
    con_front = nullptr;
    if (is_replace) {
      assert(!front_ready);
      assert(!session_started);
      // set the racing connection, will be handled by handle_accept()
      con_front = heartbeat.front_msgr->connect(
          conn->get_peer_addr(), entity_name_t(CEPH_ENTITY_TYPE_OSD, peer));
      has_racing = true;
      logger().warn("Heartbeat::Peer::handle_reset(): "
                    "con_front racing with osd.{}, updated by {}",
                    peer, con_front);
    } else {
      if (front_ready) {
        front_ready = false;
      }
      if (session_started) {
        reset_session();
      }
      if (heartbeat.whoami == peer) {
        logger().error("Heartbeat::Peer::handle_reset(): "
                       "peer is myself ({})", peer);
      } else if (heartbeat.whoami > peer || !has_racing) {
        connect_front();
      } else { // whoami < peer && has_racing
        logger().info("Heartbeat::Peer::handle_reset(): "
                      "con_front racing detected and lose, "
                      "waiting for osd.{} connect me", peer);
      }
    }
  } else if (con_back == conn) {
    con_back = nullptr;
    if (is_replace) {
      assert(!back_ready);
      assert(!session_started);
      // set the racing connection, will be handled by handle_accept()
      con_back = heartbeat.back_msgr->connect(
          conn->get_peer_addr(), entity_name_t(CEPH_ENTITY_TYPE_OSD, peer));
      has_racing = true;
      logger().warn("Heartbeat::Peer::handle_reset(): "
                    "con_back racing with osd.{}, updated by {}",
                    peer, con_back);
    } else {
      if (back_ready) {
        back_ready = false;
      }
      if (session_started) {
        reset_session();
      }
      if (heartbeat.whoami == peer) {
        logger().error("Heartbeat::Peer::handle_reset(): "
                       "peer is myself ({})", peer);
      } else if (heartbeat.whoami > peer || !has_racing) {
        connect_back();
      } else { // whoami < peer && has_racing
        logger().info("Heartbeat::Peer::handle_reset(): "
                      "con_back racing detected and lose, "
                      "waiting for osd.{} connect me", peer);
      }
    }
  } else {
    // ignore the unrelated conn
  }
}

void Heartbeat::Peer::handle_connect(crimson::net::ConnectionRef conn)
{
  if (con_front == conn) {
    assert(!front_ready);
    assert(!session_started);
    notify_front_ready();
  } else if (con_back == conn) {
    assert(!back_ready);
    assert(!session_started);
    notify_back_ready();
  } else {
    // ignore the unrelated connection
  }
}

void Heartbeat::Peer::handle_accept(crimson::net::ConnectionRef conn)
{
  handle_connect(conn);

  const auto peer_addr = conn->get_peer_addr();
  const auto osdmap = heartbeat.service.get_osdmap_service().get_map();
  const auto front_addr = osdmap->get_hb_front_addrs(peer).front();
  if (!con_front && front_addr == peer_addr) {
    logger().info("Heartbeat::Peer::handle_accept(): "
                  "con_front racing resolved for osd.{}", peer);
    con_front = conn;
    notify_front_ready();
  }
  const auto back_addr = osdmap->get_hb_back_addrs(peer).front();
  if (!con_back && back_addr == peer_addr) {
    logger().info("Heartbeat::Peer::handle_accept(): "
                  "con_back racing resolved for osd.{}", peer);
    con_back = conn;
    notify_back_ready();
  }
}

void Heartbeat::Peer::start_session()
{
  logger().info("Heartbeat::Peer: osd.{} started (send={})",
                peer, pending_send);
  assert(!session_started);
  session_started = true;
  ping_history.clear();
  if (pending_send) {
    pending_send = false;
    do_send_heartbeat(clock::now(), heartbeat.service.get_mnow(), nullptr);
  }
}

void Heartbeat::Peer::reset_session()
{
  logger().info("Heartbeat::Peer: osd.{} reset", peer);
  assert(session_started);
  if (!ping_history.empty()) {
    // we lost our ping_history of the last session, but still need to keep
    // the oldest deadline for unhealthy check.
    auto oldest = ping_history.begin();
    auto sent_stamp = oldest->first;
    auto deadline = oldest->second.deadline;
    ping_history.clear();
    ping_history.emplace(sent_stamp, reply_t{deadline, 0});
  }
  session_started = false;
}

void Heartbeat::Peer::notify_front_ready()
{
  assert(con_front);
  assert(!front_ready);
  assert(!session_started);
  front_ready = true;
  if (front_ready && back_ready) {
    start_session();
  }
}

void Heartbeat::Peer::connect_front()
{
  assert(!con_front);
  assert(!front_ready);
  assert(!session_started);
  auto osdmap = heartbeat.service.get_osdmap_service().get_map();
  // TODO: use addrs
  con_front = heartbeat.front_msgr->connect(
      osdmap->get_hb_front_addrs(peer).front(),
      entity_name_t(CEPH_ENTITY_TYPE_OSD, peer));
  if (con_front->is_connected()) {
    notify_front_ready();
  }
}

void Heartbeat::Peer::notify_back_ready()
{
  assert(con_back);
  assert(!back_ready);
  assert(!session_started);
  back_ready = true;
  if (front_ready && back_ready) {
    start_session();
  }
}

void Heartbeat::Peer::connect_back()
{
  assert(!con_back);
  assert(!back_ready);
  assert(!session_started);
  auto osdmap = heartbeat.service.get_osdmap_service().get_map();
  // TODO: use addrs
  con_back = heartbeat.back_msgr->connect(
      osdmap->get_hb_back_addrs(peer).front(),
      entity_name_t(CEPH_ENTITY_TYPE_OSD, peer));
  if (con_back->is_connected()) {
    notify_back_ready();
  }
}

seastar::future<> Heartbeat::FailingPeers::add_pending(
  osd_id_t peer,
  clock::time_point failed_since,
  clock::time_point now)
{
  if (failure_pending.count(peer)) {
    return seastar::now();
  }
  auto failed_for = chrono::duration_cast<chrono::seconds>(
      now - failed_since).count();
  auto osdmap = heartbeat.service.get_osdmap_service().get_map();
  auto failure_report =
      make_message<MOSDFailure>(heartbeat.monc.get_fsid(),
                                peer,
                                osdmap->get_addrs(peer),
                                static_cast<int>(failed_for),
                                osdmap->get_epoch());
  failure_pending.emplace(peer, failure_info_t{failed_since,
                                               osdmap->get_addrs(peer)});
  return heartbeat.monc.send_message(failure_report);
}

seastar::future<> Heartbeat::FailingPeers::cancel_one(osd_id_t peer)
{
  if (auto pending = failure_pending.find(peer);
      pending != failure_pending.end()) {
    auto fut = send_still_alive(peer, pending->second.addrs);
    failure_pending.erase(peer);
    return fut;
  }
  return seastar::now();
}

seastar::future<>
Heartbeat::FailingPeers::send_still_alive(
    osd_id_t osd, const entity_addrvec_t& addrs)
{
  auto still_alive = make_message<MOSDFailure>(
    heartbeat.monc.get_fsid(),
    osd,
    addrs,
    0,
    heartbeat.service.get_osdmap_service().get_map()->get_epoch(),
    MOSDFailure::FLAG_ALIVE);
  return heartbeat.monc.send_message(still_alive);
}
