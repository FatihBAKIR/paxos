//
// Created by fatih on 12/7/17.
//

#include <paxos/remote_end.hpp>
#include <iostream>
#include <thread>
#include <rpc/rpc_error.h>
#include <rpc/msgpack.hpp>
#include <fstream>
#include <paxos/local_end.hpp>
#include <rpc/this_handler.h>

namespace paxos
{
    local_end::local_end(uint16_t port, int n_id) :
            m_server(port), m_node_id(n_id), m_last_hb(clock::now())
    {
        m_l = spdlog::stderr_color_mt("le_log" + std::to_string(n_id));
        m_server.bind("heartbeat", [this](int node)
        {
            //std::cout << "Got heartbeat from " << node << "\n";
            if (node == m_curr_leader)
            {
                m_last_hb = clock::now();
                return true;
            }
            return false;
        });

        m_server.bind("prepare", [this](paxos::ballot bal) {
            if (bal.node_id == m_curr_leader)
            {
                m_last_hb = clock::now();
            }
            return prepare(bal);
        });

        m_server.bind("accept", [this](paxos::ballot bal, paxos::value val){
            if (bal.node_id == m_curr_leader)
            {
                m_last_hb = clock::now();
            }
            return accept(bal, val);
        });

        m_server.bind("inform", [this](paxos::ballot b, paxos::value v) {
            if (b.node_id == m_curr_leader)
            {
                m_last_hb = clock::now();
            }
            return inform(b, v);
        });

        m_server.bind("get_leader", [this] {
            return get_leader_id();
        });

        m_server.bind("get_log", [this](int index) {
            auto it = m_log.lower_bound(index);
            std::map<int, log_entry> res;
            for (; it != m_log.end(); ++it)
            {
                if (it->second.m_commited)
                {
                    res.insert(*it);
                }
            }
            return res;
        });

        m_server.suppress_exceptions(true);
        m_server.async_run(1);

        m_state.m_node_id = m_node_id;

        load_log();
    }

    void local_end::show(std::ostream &to) {
        to << "##### SHOW #####\n";
        to << "Current leader: " << int(get_leader_id()) << '\n';
        to << "Sold Tickets: " << m_state.sold_tickets << '\n';
        for (auto& log : m_log)
        {
            if (!log.second.m_commited) continue;

            to << "Log " << log.first << " : ";
            if (log.second.m_val.type == 0)
            {
                to << log.second.m_val.ts;
            } else {
                to << log.second.m_val.cc;
            }
            to << '\n';
        }
    }

    boost::optional<std::pair<ballot, value>> local_end::phase_one(const paxos::value &val, int log_index) {
        using namespace paxos;
        using namespace std;
        vector<future<paxos::promise>> futs;

        if (m_log[log_index].m_commited)
        {
            return {};
        }

        m_log[log_index].m_cur_bal.number++;
        m_log[log_index].m_cur_bal.node_id = m_node_id;
        m_log[log_index].m_cur_bal.log_index = log_index;

        auto config = m_state.get_config(log_index);
        for (auto& remote : config)
        {
            futs.emplace_back(m_conns_[remote]->prepare(m_log[log_index].m_cur_bal));
        }

        vector<paxos::promise> proms;

        for (auto& fut : futs)
        {
            try
            {
                auto p = fut.get();
                if (p.valid)
                {
                    proms.emplace_back(std::move(p));
                } else{
                    accept(p.accept_num, p.accept_val);
                    inform(p.accept_num, p.accept_val);
                    return {};
                }
            }
            catch (rpc::timeout& err)
            {
                cerr << err.what() << '\n';
                // swallow timeouts
            }
        }

        if (proms.size() >= config.size() / 2)
        {
            bool all_null_val = std::all_of(proms.begin(), proms.end(), [](const auto& prom){
                return prom.accept_val != paxos::value{};
            });

            paxos::value v = {};
            if (all_null_val)
            {
                v = val;
            }
            else
            {
                paxos::promise max_prom = proms[0];
                for (auto& prom : proms)
                {
                    if (prom.accept_num > max_prom.accept_num)
                    {
                        max_prom = prom;
                    }
                }
                v = max_prom.accept_val;
            }

            // done
            return std::make_pair(m_log[log_index].m_cur_bal, v);
        }

        return {};
    }

    bool local_end::phase_two(const std::pair<paxos::ballot, paxos::value> &p1res) {
        using namespace std;
        vector<future<bool>> futs;

        auto config = m_state.get_config(p1res.first.log_index);
        for (auto& remote : config)
        {
            futs.emplace_back(m_conns_[remote]->accept(p1res.first, p1res.second));
        }

        vector<bool> results;

        for (auto& fut : futs)
        {
            try
            {
                auto p = fut.get();
                results.emplace_back(p);
            }
            catch (rpc::timeout& err)
            {
                cerr << err.what() << '\n';
                results.emplace_back(false);
                // swallow timeouts
            }
        }
        results.emplace_back(accept(p1res.first, p1res.second));

        int count = std::count(results.begin(), results.end(), true);

        if (count >= ((config.size() / 2) + 1))
        {
            // decide
            for (auto& remote : config)
            {
                m_conns_[remote]->inform(p1res.first, p1res.second);
            }
            inform(p1res.first, p1res.second);
            m_curr_leader = m_node_id;
            m_last_hb = clock::now();
            start_hb_thread();
            return true;
        }
        return false;
    }

    void local_end::start_hb_thread()
    {
        if (m_running) return;
        m_running = true;
        m_hb_thread = std::thread([this] {
           while (m_running) {
               if (!am_i_leader()) continue;
               auto began = clock::now();
               send_heartbeats();
               auto spent = clock::now() - began;
               //m_l->info("Send took {} ms", std::chrono::duration_cast<std::chrono::milliseconds>(spent).count());
               std::this_thread::sleep_for(std::chrono::milliseconds(300) - spent);
           }
        });
    }

    local_end::~local_end() {
        m_running = false;
        if (m_hb_thread.joinable())
        {
            m_hb_thread.join();
        }
    }

    bool local_end::send_heartbeats() {
        if (!am_i_leader())
        {
            return false;
        }

        using namespace std;
        vector<future<bool>> proms;

        auto config = m_state.get_config(get_last_log());
        for (auto& remote : config)
        {
            proms.push_back(m_conns_[remote]->heartbeat(m_node_id));
        }

        vector<bool> results;

        for (auto& prom : proms)
        {
            try
            {
                results.push_back(prom.get());
            }
            catch (std::exception&)
            {
                results.push_back(false);
            }
        }

        int count = std::count(results.begin(), results.end(), true);

        if (count >= config.size() / 2)
        {
            m_last_hb = clock::now();
            return true;
        }

        return false;
    }

    bool local_end::am_i_leader() const {
        if (clock::now() - m_last_hb.load(std::memory_order_relaxed) > std::chrono::milliseconds(750))
        {
            return false;
        }
        return m_curr_leader == m_node_id;
    }

    paxos::remote_end *local_end::get_leader() {
        if (clock::now() - m_last_hb.load(std::memory_order_relaxed) > std::chrono::seconds(1) || m_curr_leader == 0xFF)
        {
            return nullptr;
        }
        return m_conns_[m_curr_leader];
    }

    uint8_t local_end::get_leader_id() {
        if (am_i_leader())
        {
            return m_node_id;
        }

        if (clock::now() - m_last_hb.load(std::memory_order_relaxed) < std::chrono::seconds(1))
        {
            return m_curr_leader;
        }

        return 0xFF;
    }

    paxos::promise local_end::prepare(paxos::ballot bal) {
        m_l->info("Got prepare {}", bal);
        if (bal > m_log[bal.log_index].m_cur_bal && !m_log[bal.log_index].m_commited)
        {
            m_log[bal.log_index].m_cur_bal = bal;
            dump_log();
            m_l->info("Sending {}", m_log[bal.log_index].m_val.ts);
            return { bal, m_log[bal.log_index].m_accept_bal, m_log[bal.log_index].m_val, true };
        }
        m_l->info("Rejecting...");
        return { bal, m_log[bal.log_index].m_accept_bal, m_log[bal.log_index].m_val, false };
    }

    bool local_end::accept(paxos::ballot bal, paxos::value val) {
        if (val.type == 0) {
            if (m_state.sold_tickets + val.ts.ticket_count > 100) {
                return false;
            }
            /*if (val.ts.client_id != bal.node_id) {
                return false;
            }*/
        }
        if (bal >= m_log[bal.log_index].m_cur_bal && !m_log[bal.log_index].m_commited)
        {
            m_log[bal.log_index].m_accept_bal = bal;
            m_log[bal.log_index].m_val = val;
            m_curr_leader = bal.node_id;
            m_last_hb = clock::now();
            m_l->info("Accepted: {}, {}", val.ts, bal);
            dump_log();
            return true;
        }
        return false;
    }

    void local_end::inform(paxos::ballot b, paxos::value val) {
        if (val != m_log[b.log_index].m_val)
        {
            throw std::runtime_error("bad");
        }

        m_log[b.log_index].m_commited = true;
        /*for (auto it = m_log.find(b.log_index); it != m_log.end(); ++it)
        {
            if (!it->second.m_commited) break;
            m_state.apply(it->first, it->second.m_val);
        }*/
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        learn_log();
        dump_log();

        std::cout << int(m_node_id) << " - " << b.log_index << " DECIDED " << val.ts << " " << b << "\n";
    }

    void local_end::state::apply(int log, const value &v) {
        if (last_log + 1 != log) return;
        std::cout << v.type << "\n";

        if (v.type == 0)
        {
            sold_tickets += v.ts.ticket_count;
        }
        else if(v.type == 1)
        {
            m_changes.push_back({ log, v.cc });
        }

        last_log = log;
        std::cout << log << " STATE APPLIED\n";
    }

    std::vector<uint8_t> local_end::state::get_config(int for_log) const {
        std::vector<uint8_t> res{0, 1, 2};
        for (auto& cc : m_changes)
        {
            if (cc.log_index + 3 <= for_log)
            {
                res.push_back(cc.chg.new_node1);
                res.push_back(cc.chg.new_node2);
            }
        }
        res.erase(std::remove(res.begin(), res.end(), m_node_id), res.end());
        return res;
    }

    void local_end::add_endpoint(uint8_t node_id, boost::string_view host, uint16_t port) {
        auto it = m_conns_.find(node_id);
        if (it != m_conns_.end())
        {
            // already exists, return
            return;
        }
        m_conns_.emplace(node_id, new paxos::remote_end(host, port));
    }

    uint8_t local_end::discover_leader() const {
        using namespace std;
        vector<future<uint8_t>> proms;

        auto config = m_state.get_config(get_last_log() + 1);
        for (auto& remote : config)
        {
            proms.push_back(m_conns_.find(remote)->second->get_leader_id());
        }

        map<uint8_t, int> results;

        for (auto& prom : proms)
        {
            try
            {
                auto l = prom.get();
                results[l]++;
                m_l->info("Discovering... {}", l);
                if (results[l] >= (config.size() / 2))
                {
                    return l;
                }
            }
            catch (std::exception&)
            {
                results[0xFF]++;
                m_l->info("Down... 255");
                if (results[0xFF] >= (config.size() / 2))
                {
                    return 0xFF;
                }
            }
        }

        return 0xFF;
    }

    void local_end::dump_log() {
        namespace msgpack = RPCLIB_MSGPACK;
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, m_log);
        std::ofstream out("log" + std::to_string(m_node_id) + ".mpk");
        out.write(sbuf.data(), sbuf.size());
    }

    void local_end::load_log()
    {
        namespace msgpack = RPCLIB_MSGPACK;

        std::ifstream in("log" + std::to_string(m_node_id) + ".mpk");

        if (!in.good())
        {
            return;
        }

        std::string buffer((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        msgpack::unpacker pac;
        pac.reserve_buffer(buffer.size());
        std::copy(buffer.begin(), buffer.end(), pac.buffer());
        pac.buffer_consumed(buffer.size());

        msgpack::object_handle oh;
        pac.next(oh);

        oh.get().convert(m_log);
        for (auto& l : m_log)
        {
            if (!l.second.m_commited) break;
            m_state.apply(l.first, l.second.m_val);
        }
    }

    void local_end::learn_log() {
        auto leader = get_leader();
        if (leader)
        {
            auto rest = leader->get_log_entry(m_state.last_log);
            for (auto& l : rest.get())
            {
                m_log[l.first] = l.second;
            }
        }

        for (auto& l : m_log)
        {
            if (!l.second.m_commited) break;
            m_state.apply(l.first, l.second.m_val);
        }
    }

    void local_end::detect_leader() {
        m_curr_leader = discover_leader();
        m_last_hb = clock::now();

        learn_log();
        dump_log();
    }
}
