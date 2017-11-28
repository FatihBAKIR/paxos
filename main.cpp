#include <iostream>
#include <rpc/server.h>
#include <rpc/client.h>
#include <rpc/this_handler.h>
#include <rpc/this_server.h>
#include <rpc/this_session.h>
#include <memory>

namespace paxos {
    struct ballot {
        int number;
        int node_id;
    };

    struct value {
    };

    struct promise {
        ballot bal;
        ballot accept_num;
        value accept_val;
    };

    struct ticket_sell {
        int client_id;
        int ticket_id;
    };

    struct config_chg {
        int new_node1;
        int new_node2;
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

class local_end {
public:
    local_end() :
            m_server(8080) {
        m_server.bind("init_conn", [](int node) {
            std::cout << "Got connection from " << node << "\n";
            return true;
        });
        m_server.suppress_exceptions(true);
        m_server.async_run(1);
    }

private:
    std::map<int, remote_end *> m_conns;
    rpc::server m_server;
    paxos::persist m_state;


};

class remote_end {
    rpc::client m_c;

public:

    remote_end(const std::string &host, int port) :
            m_c(host, port) {
        m_c.set_timeout(5000);
    }

    std::future<void>
    initiate(int node_id) {
        auto p = std::make_shared<std::promise<void>>();
        auto res = p->get_future();

        std::async(std::launch::async, [p, fut = m_c.async_call("init_conn", node_id)]() mutable {
            try {
                auto r = fut.get().as<bool>();
                if (r) {
                    p->set_value();
                } else {
                    throw std::runtime_error("err");
                }
            }
            catch (std::exception &e) {
                std::cout << "holy shit\n";
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<paxos::promise>
    prepare(paxos::ballot b) {
        auto p = std::promise<paxos::promise>{};
        auto res = p.get_future();
        auto fut = m_c.async_call("prepare", b.node_id, b.number);

        return res;
    }
};

int main() {
    local_end l;

    remote_end r("localhost", 8080);

    auto init_res = r.initiate(0);
    init_res.get();

    /*auto fut = r.prepare(paxos::ballot{3, 5});
    fut.get();

    std::this_thread::sleep_for(std::chrono::seconds(2));*/

    return 0;
}