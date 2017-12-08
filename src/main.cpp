#include <iostream>
#include <spdlog/spdlog.h>
#include <paxos/paxos.hpp>
#include <paxos/local_end.hpp>
#include <future>
#include <nlohmann/json.hpp>
#include <fstream>
#include <rpc/this_handler.h>


int main(int argc, char** argv) {
    std::ifstream in("config.json");
    nlohmann::json config;
    in >> config;

    struct node
    {
        std::string host;
        int port;
    };

    std::vector<node> nodes;
    for (auto& p : config["nodes"])
    {
        node n;
        n.host = p["ip"].get<std::string>();
        n.port = p["port"].get<int>();
        nodes.push_back(n);
    }

    auto log = spdlog::stderr_color_mt("log");
    auto node_id = std::stoi(argv[1]);
    using namespace paxos;

    rpc::server serv(nodes[node_id].port*2);
    local_end me(nodes[node_id].port, node_id);

    serv.bind("buy", [&me, &log] (int num_ticks, int node_id) -> uint8_t {
        auto log_index = me.get_first_hole();
        if (log_index == -1)
        {
            log_index = me.get_last_log() + 1;
        }
        log->info("Proposing log index: {}", log_index);
        if (me.am_i_leader())
        {
            log->info("Taking the fast route");
            auto p1res = std::make_pair(paxos::ballot{ 1, node_id, log_index }, paxos::value{ 0, { node_id, num_ticks } });
            log->info("{}: {}", node_id, me.phase_two(p1res));
        }
        else if (!me.get_leader())
        {
            log->info("Taking the slow route :(");
            auto p1res = me.phase_one(paxos::value{0, {node_id, num_ticks}}, log_index);
            if (p1res) {
                log->info("{}: {}", node_id, me.phase_two(*p1res));
            }
        }

        auto id = me.get_leader_id();
        rpc::this_handler().respond(id);
        return id;
    });

    serv.bind("cc", [&me, &log, &node_id] () {
        auto log_index = me.get_first_hole();
        if (log_index == -1)
        {
            log_index = me.get_last_log() + 1;
        }
        log->info("Adding 3, 4 to the config");
        log->info("Proposing log index: {}", log_index);

        if (me.am_i_leader())
        {
            log->info("Taking the fast route");
            auto p1res = std::make_pair(paxos::ballot{ 1, node_id, log_index }, paxos::value{ 1, { }, { 3, 4 } });
            log->info("{}: {}", node_id, me.phase_two(p1res));
        }
        else
        {
            log->info("Taking the slow route :(");
            auto p1res = me.phase_one(paxos::value{0, {}, { 3, 4 }}, log_index);
            if (p1res) {
                log->info("{}: {}", node_id, me.phase_two(*p1res));
            }
        }

        log->info("Leader: {}", int(me.get_leader_id()));
        log->info("Heartbeat result: {}", me.send_heartbeats());

        return me.get_leader_id();
    });

    serv.bind("show", [&me] () {
        std::ostringstream oss;
        me.show(oss);
        return oss.str();
    });

    serv.bind("hb", [&me]{
        return true;
    });

    log->info("creating local end on {}, with id {}", nodes[node_id].port, node_id);

    for (int i = 0; i < nodes.size(); ++i)
    {
        if (i == node_id) continue;
        me.add_endpoint(i, nodes[i].host, nodes[i].port);
    }

    me.detect_leader();

    /*for (std::string cmd; std::cin >> cmd;)
    {
        if (cmd == "hb")
        {
            log->info("Heartbeat result: {}", me.send_heartbeats());
        }
        else if (cmd == "ldr")
        {
            log->info("My leader is: {}", me.get_leader_id());
        }
        else if (cmd == "show")
        {
            me.show(std::cout);
        }
    }*/

    serv.run();

    return 0;
}