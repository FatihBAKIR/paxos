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

namespace paxos {
    struct ballot {
        int number;
        int node_id;
        int log_index;
        MSGPACK_DEFINE_MAP(number, node_id, log_index);
    };

    struct ticket_sell {
        int client_id;
        int ticket_id;
        MSGPACK_DEFINE_MAP(client_id, ticket_id);
    };

    struct config_chg {
        int new_node1;
        int new_node2;
        MSGPACK_DEFINE_MAP(new_node1, new_node2);
    };

    struct value {
        int type;
        ticket_sell ts;
        config_chg cc;
        MSGPACK_DEFINE_MAP(type, ts, cc);
    };

    struct promise {
        ballot bal;
        ballot accept_num;
        value accept_val;
        MSGPACK_DEFINE_MAP(bal, accept_num, accept_val);
    };

    struct log_entry {
        int type;
        union {
            ticket_sell ts;
            config_chg cc;
        };
    };

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
        m_c.set_timeout(5000);
    }

    std::future<void>
    heartbeat(int node_id) {
        auto p = std::make_shared<std::promise<void>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, node_id]() mutable {
            try {
                auto fut = async_call("heartbeat", node_id);
                auto r = fut.get().as<bool>();
                if (r) {
                    p->set_value();
                } else {
                    throw std::runtime_error("err");
                }
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
};

class local_end {


public:
    using clock = std::chrono::high_resolution_clock;
    explicit local_end(uint16_t port) :
            m_server(port)
    {
        m_server.bind("heartbeat", [this](int node) {
            std::cout << "Got heartbeat from " << node << "\n";
            if (node == m_curr_leader)
            {
                m_last_hb = clock::now();
            }
            return true;
        });

        m_server.bind("prepare", [this](paxos::ballot bal) {
            return prepare(bal);
        });

        m_server.bind("accept", [this](paxos::ballot bal, paxos::value val){
            return accept(bal, val);
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

    paxos::ballot phase_one()
    {
        using namespace paxos;
        using namespace std;
        vector<future<paxos::promise>> futs;

        m_cur_bal.number++;
        paxos::ballot b = m_cur_bal;
        b.node_id = m_node_id;

        for (auto& remotes : m_conns)
        {
            futs.emplace_back(remotes.second->prepare(b));
        }

        vector<paxos::promise> proms;

        for (auto& fut : futs)
        {
            try
            {
                proms.push_back(fut.get());
            }
            catch (rpc::timeout& err)
            {
                cerr << err.what() << '\n';
                // swallow timeouts
            }
        }

        if ((proms.size() + 1) > (m_conns.size() + 1) / 2)
        {
            // done
            return b;
        }

        return {};
    }

    remote_end* get_leader()
    {
        if (clock::now() - m_last_hb > std::chrono::seconds(1) || m_curr_leader == 0xFF)
        {
            return nullptr;
        }
        return m_conns[m_curr_leader];
    }

private:

    paxos::promise prepare(paxos::ballot bal)
    {
        if (bal.number >= m_cur_bal.number)
        {
            m_cur_bal = bal;
        }
        return { bal, m_cur_bal, m_val };
    }

    bool accept(paxos::ballot bal, paxos::value val)
    {
        if (bal.number >= m_cur_bal.number)
        {
            m_cur_bal = bal;
            m_val = val;
            return true;
        }
        return false;
    }

    std::map<uint8_t, remote_end *> m_conns;
    clock::time_point m_last_hb;
    uint8_t m_curr_leader = 0xFF;

    rpc::server m_server;
    paxos::persist m_state;
    paxos::ballot m_cur_bal = { 0, -1 };
    paxos::value m_val;
    uint8_t m_node_id = 0;
};

int main() {
    local_end l(8080);
    local_end l1(8081);
    local_end l2(8082);
    local_end l3(8083);
    local_end l5(8084);

    l5.add_endpoint(1, "localhost", 8080);
    l5.add_endpoint(2, "localhost", 8081);
    l5.add_endpoint(3, "localhost", 8082);
    l5.add_endpoint(4, "localhost", 8083);

    auto p1res = l5.phase_one();
    auto p1res_1 = l5.phase_one();

    //auto res = r.accept(paxos::ballot{ 10, 0 }, paxos::value{ 'c' }).get();

    /*auto fut = r.prepare(paxos::ballot{3, 5});
    fut.get();

    std::this_thread::sleep_for(std::chrono::seconds(2));*/

    return 0;
}