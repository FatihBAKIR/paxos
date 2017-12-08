//
// Created by fatih on 12/7/17.
//

#include <iostream>
#include <rpc/client.h>
#include <fstream>
#include <nlohmann/json.hpp>
#include <rpc/rpc_error.h>

int main(int argc, char *argv[])
{
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

    const auto node_id = std::stoi(argv[1]);
    constexpr auto timeout = 10000;

    auto connect = [&nodes](uint8_t to) -> std::unique_ptr<rpc::client> {
        try
        {
            auto c = std::make_unique<rpc::client>(nodes[to].host, nodes[to].port * 2);
            c->set_timeout(400);
            if (c->call("hb").as<bool>())
            {
                c->set_timeout(timeout);
                return c;
            }
        }
        catch (rpc::timeout&)
        {
            std::cerr << "Leader is down\n";
            exit(0);
            return nullptr;
        }
        return nullptr;
    };



    uint8_t curr_leader_id = node_id;
    auto client = connect(curr_leader_id);
    while (!client)
    {
        std::cerr << "Node " << int(curr_leader_id) << " seems to be down, will try next...\n";
        curr_leader_id++;
        curr_leader_id %= 5;
        client = connect(curr_leader_id);
    }
    std::cout << "Connected to " << int(curr_leader_id) << "!\n";

    retry:
    std::cout << "> ";
    for (std::string cmd; std::cin >> cmd; std::cout << "> ") {
        if (cmd == "cc") {
            auto leader_id = client->call("cc").as<uint8_t>();
            while (leader_id != curr_leader_id)
            {
                if (leader_id == 0xFF)
                {
                    std::cout << "Election failed\n";
                    goto retry;
                }
                curr_leader_id = leader_id;
                client = connect(curr_leader_id);

                leader_id = client->call("cc").as<uint8_t>();
            }
            std::cout << "Curr Leader: " << int(curr_leader_id) << std::endl;
        } else if (cmd == "buy") {
            int num;
            std::cin >> num;
            auto leader_id = client->call("buy", num, node_id).as<uint8_t>();

            while (leader_id != curr_leader_id)
            {
                if (leader_id == 0xFF)
                {
                    std::cout << "Election failed\n";
                    goto retry;
                }
                curr_leader_id = leader_id;
                client = connect(curr_leader_id);

                leader_id = client->call("buy", num, node_id).as<uint8_t>();
            }
            std::cout << "Curr Leader: " << int(curr_leader_id) << std::endl;
        } else if (cmd == "show") {
            std::cout << client->call("show").as<std::string>();
        }
    }
}