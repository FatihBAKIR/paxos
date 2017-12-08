//
// Created by fatih on 12/7/17.
//

#pragma once

#include <paxos/paxos.hpp>
#include <rpc/rpc.h>
#include <rpc/client.h>
#include <mutex>
#include <boost/utility/string_view.hpp>

namespace paxos
{
class remote_end {
    std::string host;
    int port;

    //rpc::client m_c;

    std::mutex m_call_prot;

    template <int timeout = 400, class... Args>
    auto async_call(Args&&... args)
    {
        std::lock_guard<std::mutex> lk{m_call_prot};

        auto c = std::make_shared<rpc::client>(host, port);
        c->set_timeout(timeout);
        return std::make_pair(c, c->async_call(std::forward<Args>(args)...));
    }

public:
    remote_end(boost::string_view host, int port) /*: m_c(std::string(host), port)*/ {
        this->host = std::string(host);
        this->port = port;
        /*m_c.set_timeout(1000);
        while (m_c.get_connection_state() != rpc::connection_state::connected)
        {
            m_c.reconnect();
        }*/
    }

    std::future<bool>
    heartbeat(int node_id);

    std::future<paxos::promise>
    prepare(paxos::ballot b);

    std::future<bool>
    accept(paxos::ballot b, paxos::value v);

    std::future<uint8_t>
    get_leader_id();

    std::future<std::map<int, log_entry>>
    get_log_entry(int index);

    void inform(paxos::ballot b, paxos::value v);
};
}
