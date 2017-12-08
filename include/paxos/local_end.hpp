//
// Created by fatih on 12/7/17.
//

#pragma once

#include <chrono>
#include <rpc/server.h>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>
#include <paxos/paxos.hpp>
#include <spdlog/spdlog.h>

namespace paxos
{
class remote_end;
class local_end {
public:
    using clock = std::chrono::high_resolution_clock;
    explicit local_end(uint16_t port, int n_id);

    void add_endpoint(uint8_t node_id, boost::string_view host, uint16_t port);

    boost::optional<std::pair<paxos::ballot, paxos::value>> phase_one(const paxos::value& val, int log_index);

    bool phase_two(const std::pair<paxos::ballot, paxos::value>& p1res);

    bool send_heartbeats();

    bool am_i_leader() const;

    paxos::remote_end* get_leader();

    uint8_t get_leader_id();

    int get_first_hole() const
    {
        if (m_log.size() == m_state.last_log)
        {
            return -1;
        }

        auto it = m_log.end();
        --it;
        while (it->second.m_commited)
        {
            --it;
        }
        return it->first;
    }

    int get_last_log() const
    {
        if (m_log.empty())
        {
            return 0;
        }

        auto it = m_log.end();
        --it;
        while (!it->second.m_commited)
        {
            if (it == m_log.begin())
            {
                return 0;
            }
            --it;
        }
        return it->first;
    }

    void detect_leader();
    uint8_t discover_leader() const;

    void show(std::ostream& to);

    void learn_log();

    ~local_end();

private:

    void start_hb_thread();

    void dump_log();
    void load_log();

    paxos::promise prepare(paxos::ballot bal);

    bool accept(paxos::ballot bal, paxos::value val);

    void inform(paxos::ballot b, paxos::value val);

    std::map<uint8_t, paxos::remote_end *> m_conns_;
    std::atomic<clock::time_point> m_last_hb;
    std::atomic<uint8_t> m_curr_leader = 0xFF;
    std::atomic<bool> m_running = false;

    rpc::server m_server;

    struct state
    {
        uint8_t m_node_id;
        int last_log = 0;
        int sold_tickets = 0;

        void apply(int log, const value& v);
        std::vector<uint8_t> get_config(int for_log) const;

    private:

        struct chg
        {
            int log_index;
            config_chg chg;
        };
        std::vector<chg> m_changes;
    };

    state m_state;

    std::map<int, log_entry> m_log;

    uint8_t m_node_id = 0;

    std::shared_ptr<spdlog::logger> m_l;

    std::thread m_hb_thread;
};
}

