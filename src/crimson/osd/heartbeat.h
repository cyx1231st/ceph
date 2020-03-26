// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <seastar/core/future.hh>
#include "common/ceph_time.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"

class MOSDPing;

namespace crimson::osd {
  class ShardServices;
}

namespace crimson::mon {
  class Client;
}

template<typename Message> using Ref = boost::intrusive_ptr<Message>;

class Heartbeat : public crimson::net::Dispatcher {
public:
  using osd_id_t = int;

  Heartbeat(const crimson::osd::ShardServices& service,
	    crimson::mon::Client& monc,
	    crimson::net::MessengerRef front_msgr,
	    crimson::net::MessengerRef back_msgr);

  seastar::future<> start(entity_addrvec_t front,
			  entity_addrvec_t back);
  seastar::future<> stop();

  void add_peer(osd_id_t peer, epoch_t epoch);
  void update_peers(int whoami);
  void remove_peer(osd_id_t peer);

  const entity_addrvec_t& get_front_addrs() const;
  const entity_addrvec_t& get_back_addrs() const;

  void set_require_authorizer(bool);

  // Dispatcher methods
  seastar::future<> ms_dispatch(crimson::net::Connection* conn,
				MessageRef m) override;
  seastar::future<> ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) override;

private:
  seastar::future<> handle_osd_ping(crimson::net::Connection* conn,
				    Ref<MOSDPing> m);
  seastar::future<> handle_ping(crimson::net::Connection* conn,
				Ref<MOSDPing> m);
  seastar::future<> handle_reply(crimson::net::Connection* conn,
				 Ref<MOSDPing> m);
  seastar::future<> handle_you_died();

  seastar::future<> send_still_alive(osd_id_t, const entity_addrvec_t&);

  using osds_t = std::vector<osd_id_t>;
  /// remove down OSDs
  /// @return peers not needed in this epoch
  osds_t remove_down_peers();
  /// add enough reporters for fast failure detection
  void add_reporter_peers(int whoami);

  seastar::future<> start_messenger(crimson::net::Messenger& msgr,
				    const entity_addrvec_t& addrs);
private:
  const crimson::osd::ShardServices& service;
  crimson::mon::Client& monc;
  crimson::net::MessengerRef front_msgr;
  crimson::net::MessengerRef back_msgr;

  seastar::timer<seastar::lowres_clock> timer;
  // use real_clock so it can be converted to utime_t
  using clock = ceph::coarse_real_clock;

  class Peer;
  using peers_map_t = std::map<osd_id_t, Peer>;
  peers_map_t peers;

  // osds which are considered failed
  // osd_id => when was the last time that both front and back pings were acked
  //           or sent.
  //           use for calculating how long the OSD has been unresponsive
  using failure_queue_t = std::map<osd_id_t, clock::time_point>;
  seastar::future<> send_failures(failure_queue_t&& failure_queue);
  seastar::future<> send_heartbeats();
  void heartbeat_check();

  struct failure_info_t {
    clock::time_point failed_since;
    entity_addrvec_t addrs;
  };
  // osds we've reported to monior as failed ones, but they are not marked down
  // yet
  std::map<osd_id_t, failure_info_t> failure_pending;
};

class Heartbeat::Peer{
 public:
  Peer(Heartbeat&, osd_id_t);
  ~Peer();
  Peer(Peer&&) = delete;
  Peer(const Peer&) = delete;
  Peer& operator=(const Peer&) = delete;

  void set_epoch(epoch_t epoch_) { epoch = epoch_; }
  epoch_t get_epoch() const { return epoch; }

  // if failure, return time_point since last active
  // else, return clock::zero()
  clock::time_point failed_since(clock::time_point now) const;
  void send_heartbeat(clock::time_point now,
                      ceph::signedspan mnow,
                      std::vector<seastar::future<>>&);
  seastar::future<> handle_reply(crimson::net::Connection*, Ref<MOSDPing>);
  void handle_reset(crimson::net::ConnectionRef);

 private:
  bool never_send_never_receive() const {
    // it should never happen when I haven't sent any heartbeat but received
    // any reply.
    assert(!(clock::is_zero(first_tx) &&
             (!clock::is_zero(last_rx_front) || !clock::is_zero(last_rx_back))));
    return clock::is_zero(first_tx);
  }
  bool both_received() const {
    return !clock::is_zero(last_rx_front) && !clock::is_zero(last_rx_back); }

  enum class health_state {
    NONE = 0, // intermediate state between HEALTHY and UNHEALTHY
    UNHEALTHY,
    HEALTHY,
  };
  health_state inspect_health_state(clock::time_point now) const {
    if (never_send_never_receive()) {
      // we are not healty nor unhealty because we haven't sent anything yet
      return health_state::NONE;
    } else if (!ping_history.empty() && ping_history.begin()->second.deadline < now) {
      return health_state::UNHEALTHY;
    } else if (both_received()) {
      // only declare to be healthy until we have received the first
      // replies from both front/back connections
      return health_state::HEALTHY;
    } else {
      // we are not healty because we haven't received both reply
      // but we are also not unhealty becase we haven't pass the deadline
      return health_state::NONE;
    }
  }

  void connect();
  void disconnect();

 private:
  Heartbeat& heartbeat;
  const osd_id_t peer;

  /// peer connection (front)
  crimson::net::ConnectionRef con_front;
  /// peer connection (back)
  crimson::net::ConnectionRef con_back;
  /// time we sent our first ping request
  clock::time_point first_tx;
  /// last time we sent a ping request
  clock::time_point last_tx;
  /// last time we got a ping reply on the front side
  clock::time_point last_rx_front;
  /// last time we got a ping reply on the back side
  clock::time_point last_rx_back;
  /// most recent epoch we wanted this peer
  epoch_t epoch;

  struct reply_t {
    clock::time_point deadline;
    // one sent over front conn, another sent over back conn
    uint8_t unacknowledged = 0;
  };
  /// history of inflight pings, arranging by timestamp we sent
  std::map<utime_t, reply_t> ping_history;
};
