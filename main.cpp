#include <iostream>
#include <rpc/server.h>
#include <rpc/client.h>
#include <rpc/this_handler.h>
#include <rpc/this_server.h>
#include <rpc/this_session.h>
#include <rpc/rpc_error.h>
#include <memory>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>
#include <spdlog/spdlog.h>

namespace paxos {
    struct ballot {
        int number = -1;
        int node_id = -1;
        int log_index = -1;
        MSGPACK_DEFINE_MAP(number, node_id, log_index);

        bool operator!=(const ballot& rhs) const
        {
            return std::tie(number, node_id, log_index) !=
                   std::tie(rhs.number, rhs.node_id, rhs.log_index);
        }

        bool operator==(const ballot& rhs) const
        {
            return !(*this != rhs);
        }

        bool operator>(const ballot& rhs) const
        {
            return number > rhs.number || node_id > rhs.node_id;
        }

        bool operator>=(const ballot& rhs) const
        {
            return number >= rhs.number || node_id >= rhs.node_id;
        }

        friend std::ostream& operator<<(std::ostream& os, const ballot& b)
        {
            return os << "<" << b.number << ", " << b.node_id << ">";
        }
    };

    struct ticket_sell {
        int client_id;
        int ticket_id;
        MSGPACK_DEFINE_MAP(client_id, ticket_id);

        bool operator!=(const ticket_sell& rhs) const
        {
            return std::tie(client_id, ticket_id) != std::tie(rhs.client_id, rhs.ticket_id);
        }

        friend std::ostream& operator<<(std::ostream& os, const ticket_sell& ts)
        {
            return os << "ts(" << ts.client_id << ", " << ts.ticket_id << ")";
        }
    };

    struct config_chg {
        int new_node1;
        int new_node2;
        MSGPACK_DEFINE_MAP(new_node1, new_node2);

        bool operator!=(const config_chg& rhs) const
        {
            return std::tie(new_node1, new_node2) != std::tie(rhs.new_node1, rhs.new_node2);
        }
    };

    struct value {
        int type;
        ticket_sell ts;
        config_chg cc;
        MSGPACK_DEFINE_MAP(type, ts, cc);

        value()
        {
            type = -1;
        }

        value(int, ticket_sell tsell)
                : type(0), ts(tsell) {}

        value(int, ticket_sell, config_chg cchg)
                : type(1), cc(cchg) {}

        bool operator!=(const value& rhs) const
        {
            if (rhs.type != type)
            {
                return false;
            }

            if (type == 0)
            {
                return ts != rhs.ts;
            }
            else
            {
                return cc != rhs.cc;
            }
        }

        bool operator==(const value& rhs) const
        {
            return !(*this != rhs);
        }
    };

    struct promise {
        ballot bal;
        ballot accept_num;
        value accept_val;
        MSGPACK_DEFINE_MAP(bal, accept_num, accept_val);

        bool operator!=(const promise& rhs) const
        {
            return std::tie(bal, accept_num, accept_val) !=
                   std::tie(rhs.bal, rhs.accept_num, rhs.accept_val);
        }

        bool operator==(const promise& rhs) const
        {
            return !(*this != rhs);
        }
    };

    using log_entry = value;

    struct persist {
        int remaining_tickets;
        std::vector<log_entry> log;
    };
}

class remote_end;

class remote_end {
    rpc::client m_c;
    std::mutex m_call_prot;

    template <class... Args>
    auto async_call(Args&&... args)
    {
        std::lock_guard<std::mutex> lk{m_call_prot};
        return m_c.async_call(std::forward<Args>(args)...);
    }

public:
    remote_end(boost::string_view host, int port) :
            m_c(std::string(host), port) {
        m_c.set_timeout(1000);
    }

    std::future<bool>
    heartbeat(int node_id) {
        auto p = std::make_shared<std::promise<bool>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, node_id]() mutable {
            try {
                auto fut = async_call("heartbeat", node_id);
                auto r = fut.get().as<bool>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<paxos::promise>
    prepare(paxos::ballot b) {
        auto p = std::make_shared<std::promise<paxos::promise>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, b]() mutable {
            try {
                auto fut = async_call("prepare", b);
                auto r = fut.get().as<paxos::promise>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<bool>
    accept(paxos::ballot b, paxos::value v)
    {
        auto p = std::make_shared<std::promise<bool>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, b, v]() mutable {
            try {
                auto fut = async_call("accept", b, v);
                auto r = fut.get().as<bool>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<uint8_t>
    get_leader_id()
    {
        auto p = std::make_shared<std::promise<uint8_t>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p]() mutable {
            try {
                auto fut = async_call("get_leader");
                auto r = fut.get().as<uint8_t>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    void inform(paxos::ballot b, paxos::value v)
    {
        std::async(std::launch::async, [this, b, v]() mutable {
            try {
                auto fut = async_call("inform", b, v);
                fut.wait();
            }
            catch (std::exception& e) {
                //std::cerr << "the guy's dead\n";
            }
        });
    }
};

class local_end {
public:
    using clock = std::chrono::high_resolution_clock;
    explicit local_end(uint16_t port, int n_id) :
            m_server(port), m_node_id(n_id)
    {
        m_server.bind("heartbeat", [this](int node)
        {
            std::cout << "Got heartbeat from " << node << "\n";
            if (node == m_curr_leader)
            {
                m_last_hb = clock::now();
                return true;
            }
            return false;
        });

        m_server.bind("prepare", [this](paxos::ballot bal) {
            return prepare(bal);
        });

        m_server.bind("accept", [this](paxos::ballot bal, paxos::value val){
            return accept(bal, val);
        });

        m_server.bind("inform", [this](paxos::ballot b, paxos::value v) {
            return inform(b, v);
        });

        m_server.bind("get_leader", [this] {
            return get_leader_id();
        });

        m_server.suppress_exceptions(true);
        m_server.async_run(1);
    }

    void add_endpoint(uint8_t node_id, boost::string_view host, uint16_t port)
    {
        auto it = m_conns.find(node_id);
        if (it != m_conns.end())
        {
            // already exists, return
            return;
        }
        m_conns.emplace(node_id, new remote_end(host, port));
    }

    boost::optional<std::pair<paxos::ballot, paxos::value>> phase_one(const paxos::value& val, int log_index)
    {
        using namespace paxos;
        using namespace std;
        vector<future<paxos::promise>> futs;

        m_instances[log_index].m_cur_bal.number++;
        m_instances[log_index].m_cur_bal.node_id = m_node_id;
        m_instances[log_index].m_cur_bal.log_index = log_index;

        for (auto& remotes : m_conns)
        {
            futs.emplace_back(remotes.second->prepare(m_instances[log_index].m_cur_bal));
        }

        vector<paxos::promise> proms;

        for (auto& fut : futs)
        {
            try
            {
                auto p = fut.get();
                proms.emplace_back(std::move(p));
            }
            catch (rpc::timeout& err)
            {
                cerr << err.what() << '\n';
                // swallow timeouts
            }
        }
        proms.emplace_back(prepare(m_instances[log_index].m_cur_bal));

        if ((proms.size()) > (m_conns.size() + 1) / 2)
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
            return std::make_pair(m_instances[log_index].m_cur_bal, v);
        }

        return {};
    }

    bool phase_two(const std::pair<paxos::ballot, paxos::value>& p1res)
    {
        using namespace std;
        vector<future<bool>> futs;
        for (auto& remote : m_conns)
        {
            futs.emplace_back(remote.second->accept(p1res.first, p1res.second));
        }

        vector<bool> results;

        for (auto& fut : futs)
        {
            try
            {
                auto p = fut.get();
                results.emplace_back(std::move(p));
            }
            catch (rpc::timeout& err)
            {
                cerr << err.what() << '\n';
                // swallow timeouts
            }
        }
        results.emplace_back(accept(p1res.first, p1res.second));

        int count = std::count(results.begin(), results.end(), true);

        if (count > (m_conns.size() + 1) / 2)
        {
            // decide
            for (auto& remote : m_conns)
            {
                remote.second->inform(p1res.first, p1res.second);
            }
            inform(p1res.first, p1res.second);
            m_am_i_leader = true;
            return true;
        }
        return false;
    }

    bool send_heartbeats()
    {
        if (!am_i_leader())
        {
            return false;
        }

        using namespace std;
        vector<future<bool>> proms;

        for (auto& remote : m_conns)
        {
            proms.push_back(remote.second->heartbeat(m_node_id));
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

        if (count + 1 > (m_conns.size() + 1) / 2)
        {
            return true;
        }

        m_am_i_leader = false;
        return false;
    }

    bool am_i_leader() const
    {
        return m_am_i_leader;
    }

    remote_end* get_leader()
    {
        if (clock::now() - m_last_hb > std::chrono::seconds(1) || m_curr_leader == 0xFF)
        {
            return nullptr;
        }
        return m_conns[m_curr_leader];
    }

    uint8_t get_leader_id()
    {
        if (m_am_i_leader)
        {
            return m_node_id;
        }

        if (clock::now() - m_last_hb > std::chrono::seconds(1))
        {
            return m_curr_leader;
        }

        return 0xFF;
    }

private:

    paxos::promise prepare(paxos::ballot bal)
    {
        if (bal > m_instances[bal.log_index].m_cur_bal)
        {
            m_instances[bal.log_index].m_cur_bal = bal;
            return { bal, m_instances[bal.log_index].m_accept_bal, m_instances[bal.log_index].m_val };
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        return {};
    }

    bool accept(paxos::ballot bal, paxos::value val)
    {
        if (val.type == 0 && m_sold_tickets.find(val.ts.ticket_id) != m_sold_tickets.end())
        {
            return false;
        }
        if (val.ts.client_id != bal.node_id)
        {
            return false;
        }
        if (bal >= m_instances[bal.log_index].m_cur_bal)
        {
            m_instances[bal.log_index].m_accept_bal = bal;
            m_instances[bal.log_index].m_val = val;
            m_am_i_leader = false;
            m_curr_leader = bal.node_id;
            std::cout << int(m_node_id) << " accepted " << val.ts << " with " << bal << '\n';
            return true;
        }
        return false;
    }

    void inform(paxos::ballot b, paxos::value val)
    {
        m_sold_tickets.insert(val.ts.ticket_id);
        if (val.type == 1)
        {
            m_config_indices.insert(b.log_index);
        }
        m_instances[b.log_index].m_commited = true;
        std::cout << int(m_node_id) << " - " << b.log_index << " DECIDED " << val.ts << " " << b << "\n";
    }

    std::vector<uint8_t> get_configuration(int for_index)
    {

    }

    std::map<uint8_t, remote_end *> m_conns;
    clock::time_point m_last_hb;
    uint8_t m_curr_leader = 0xFF;

    bool m_am_i_leader = false;

    rpc::server m_server;

    struct instance_state
    {
        paxos::ballot m_cur_bal = { 0, -1 };
        paxos::ballot m_accept_bal = { 0, -1 };
        paxos::value m_val;
        bool m_commited = false;
    };

    std::set<int> m_config_indices;
    std::set<int> m_sold_tickets;
    std::map<int, instance_state> m_instances;

    uint8_t m_node_id = 0;
};

int main() {
    //local_end l1(8080, 1);
    //local_end l2(8081, 2);
    local_end l3(8082, 3);
    local_end l4(8083, 4);
    local_end l5(8084, 5);

    auto log = spdlog::stderr_color_mt("log");

    l5.add_endpoint(1, "localhost", 8080);
    l5.add_endpoint(2, "localhost", 8081);
    l5.add_endpoint(3, "localhost", 8082);
    l5.add_endpoint(4, "localhost", 8083);

    l3.add_endpoint(1, "localhost", 8080);
    l3.add_endpoint(2, "localhost", 8081);
    l3.add_endpoint(4, "localhost", 8083);
    l3.add_endpoint(5, "localhost", 8084);

    l4.add_endpoint(1, "localhost", 8080);
    l4.add_endpoint(2, "localhost", 8081);
    l4.add_endpoint(3, "localhost", 8082);
    l4.add_endpoint(5, "localhost", 8084);

    {
        auto r4 = std::async(std::launch::async, [&] {
            auto p1res = l4.phase_one(paxos::value{0, {4, 53}}, 1);
            if (p1res) {
                log->info("4: {}", l4.phase_two(*p1res));
            }
        });

        auto r = std::async(std::launch::async, [&] {
            auto p1res = l3.phase_one(paxos::value{0, {3, 52}}, 1);
            if (p1res) {
                log->info("3: {}", l3.phase_two(*p1res));
            }
        });

        //std::this_thread::sleep_for(std::chrono::milliseconds(5000));

        auto r2 = std::async(std::launch::async, [&] {
            auto p1res = l5.phase_one(paxos::value{0, {5, 51}}, 1);
            if (p1res) {
                log->info("5: {}", l5.phase_two(*p1res));
            }
        });
    }

    //std::cout << int(l1.get_leader_id()) << '\n';
    //std::cout << int(l2.get_leader_id()) << '\n';
    std::cout << int(l3.get_leader_id()) << '\n';
    std::cout << int(l4.get_leader_id()) << '\n';
    std::cout << int(l5.get_leader_id()) << '\n';

    //std::cout << l5.phase_two({{ 1, 0, 3 }, { 0, { 0, 7 } }});

    //auto res = r.accept(paxos::ballot{ 10, 0 }, paxos::value{ 'c' }).get();

    /*auto fut = r.prepare(paxos::ballot{3, 5});
    fut.get();

    std::this_thread::sleep_for(std::chrono::seconds(2));*/

    return 0;
}