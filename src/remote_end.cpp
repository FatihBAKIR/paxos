//
// Created by fatih on 12/7/17.
//

#include <paxos/remote_end.hpp>

namespace paxos
{
    std::future<bool> remote_end::heartbeat(int node_id) {
        auto p = std::make_shared<std::promise<bool>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, node_id]() mutable {
            try {
                auto [c, fut] = async_call<100>("heartbeat", node_id);
                auto r = fut.get().as<bool>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<paxos::promise> remote_end::prepare(paxos::ballot b) {
        auto p = std::make_shared<std::promise<paxos::promise>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, b]() mutable {
            try {
                auto [c, fut] = async_call("prepare", b);
                auto r = fut.get().as<paxos::promise>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<bool> remote_end::accept(paxos::ballot b, paxos::value v) {
        auto p = std::make_shared<std::promise<bool>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, b, v]() mutable {
            try {
                auto [c, fut] = async_call("accept", b, v);
                auto r = fut.get().as<bool>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<uint8_t> remote_end::get_leader_id() {
        auto p = std::make_shared<std::promise<uint8_t>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p]() mutable {
            try {
                auto [c, fut] = async_call("get_leader");
                auto r = fut.get().as<uint8_t>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    std::future<std::map<int, log_entry>> remote_end::get_log_entry(int index) {
        auto p = std::make_shared<std::promise<std::map<int, log_entry>>>();
        auto res = p->get_future();

        std::async(std::launch::async, [this, p, index]() mutable {
            try {
                auto [c, fut] = async_call("get_log", index);
                auto r = fut.get().as<std::map<int, log_entry>>();
                p->set_value(r);
            }
            catch (std::exception &e) {
                p->set_exception(std::current_exception());
            }
        });

        return res;
    }

    void remote_end::inform(paxos::ballot b, paxos::value v) {
        std::async(std::launch::async, [this, b, v]() mutable {
            try {
                auto [c, fut] = async_call("inform", b, v);
                fut.wait();
            }
            catch (std::exception& e) {
                //std::cerr << "the guy's dead\n";
            }
        });
    }
}
