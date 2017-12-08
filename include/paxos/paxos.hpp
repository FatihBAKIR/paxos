//
// Created by fatih on 12/7/17.
//

#pragma once

#include <fmt/ostream.h>
#include <rpc/msgpack.hpp>
#include <ostream>
#include <vector>
#include <tuple>

namespace paxos {
    struct ballot {
        int number = -1;
        int node_id = -1;
        int log_index = -1;
        MSGPACK_DEFINE_MAP(number, node_id, log_index);

        bool operator!=(const ballot& rhs) const;

        bool operator==(const ballot& rhs) const;

        bool operator>(const ballot& rhs) const
        {
            return number > rhs.number || node_id > rhs.node_id;
        }

        bool operator>=(const ballot& rhs) const;

        friend std::ostream& operator<<(std::ostream& os, const ballot& b);
    };

    struct ticket_sell {
        int client_id;
        int ticket_count;
        MSGPACK_DEFINE_MAP(client_id, ticket_count);

        bool operator!=(const ticket_sell& rhs) const;

        friend std::ostream& operator<<(std::ostream& os, const ticket_sell& ts);
    };

    struct config_chg {
        int new_node1;
        int new_node2;
        MSGPACK_DEFINE_MAP(new_node1, new_node2);

        bool operator!=(const config_chg& rhs) const;
        friend std::ostream& operator<<(std::ostream& os, const config_chg& cc);
    };

    struct value {
        int type;
        ticket_sell ts;
        config_chg cc;
        MSGPACK_DEFINE_MAP(type, ts, cc);

        value();

        value(int, ticket_sell tsell);

        value(int, ticket_sell, config_chg cchg);

        bool operator!=(const value& rhs) const;

        bool operator==(const value& rhs) const;
    };

    struct promise {
        ballot bal;
        ballot accept_num;
        value accept_val;
        bool valid = false;
        MSGPACK_DEFINE_MAP(bal, accept_num, accept_val, valid);

        bool operator!=(const promise& rhs) const;

        bool operator==(const promise& rhs) const;
    };

    struct log_entry
    {
        paxos::ballot m_cur_bal = { 0, -1 };
        paxos::ballot m_accept_bal = { 0, -1 };
        paxos::value m_val;
        bool m_commited = false;
        MSGPACK_DEFINE_MAP(m_cur_bal, m_accept_bal, m_val, m_commited);
    };
}

