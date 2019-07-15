// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_request.h"

#include "common/Formatter.h"
#include "messages/MOSDRepOp.h"

#include "crimson/osd/osd.h"
#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

RepRequest::RepRequest(OSD &osd,
		       ceph::net::ConnectionRef&& conn,
		       Ref<MOSDRepOp> &&req)
  : osd{osd},
    conn{std::move(conn)},
    req{req}
{}

void RepRequest::print(std::ostream &) const
{}

void RepRequest::dump_detail(Formatter *f) const
{}

RepRequest::ConnectionPipeline &RepRequest::cp()
{
  return get_osd_priv(conn.get()).replicated_request_conn_pipeline;
}

RepRequest::PGPipeline &RepRequest::pp(PG &pg)
{
  return pg.replicated_request_pg_pipeline;
}

seastar::future<> RepRequest::start()
{
  IRef ref = this;
  return seastar::now();
  return with_blocking_future(handle.enter(cp().await_map))
    .then([this]() {
      return with_blocking_future(osd.osdmap_gate.wait_for_map(req->get_min_epoch()));
    }).then([this](epoch_t epoch) {
      return with_blocking_future(handle.enter(cp().get_pg));
    }).then([this] {
      return with_blocking_future(osd.wait_for_pg(req->get_spg()));
    }).then([this, ref=std::move(ref)](Ref<PG> pg) {
      return seastar::do_with(
        std::move(pg), std::move(ref), [this](auto pg, auto op) {
        return with_blocking_future(
          handle.enter(pp(*pg).await_map))
        .then([this, pg] {
          return with_blocking_future(
            pg->osdmap_gate.wait_for_map(req->get_map_epoch()));
        }).then([this, pg] (auto) {
          return with_blocking_future(handle.enter(pp(*pg).process));
        }).then([this, pg] {
          return pg->handle_rep_op(std::move(req));
        });
      });
    });
}
}
