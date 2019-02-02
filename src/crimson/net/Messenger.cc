// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messenger.h"
#include "SocketMessenger.h"

namespace ceph::net {

seastar::future<Messenger*>
Messenger::create(const entity_name_t& name,
                  const std::string& lname,
                  const uint64_t nonce,
                  const int master_sid)
{
  if (master_sid < 0) {
    return create_sharded<SocketMessenger<placement_policy::ShardByAddr>>(
        name, lname, nonce, placement_policy::ShardByAddr{})
      .then([](Messenger *msgr) {
        return msgr;
      });
  } else {
    return create_sharded<SocketMessenger<placement_policy::Pinned>>(
        name, lname, nonce, placement_policy::Pinned{static_cast<seastar::shard_id>(master_sid)})
      .then([](Messenger *msgr) {
        return msgr;
      });
  }
}

} // namespace ceph::net
