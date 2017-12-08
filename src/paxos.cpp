//
// Created by fatih on 12/7/17.
//

#include <paxos/paxos.hpp>

namespace paxos
{

    bool ballot::operator!=(const ballot &rhs) const {
        return std::tie(number, node_id, log_index) !=
               std::tie(rhs.number, rhs.node_id, rhs.log_index);
    }

    bool ballot::operator==(const ballot &rhs) const {
        return !(*this != rhs);
    }

    bool ballot::operator>=(const ballot &rhs) const {
        return number >= rhs.number || node_id >= rhs.node_id;
    }

    std::ostream &operator<<(std::ostream &os, const ballot &b) {
        return os << "<" << b.number << ", " << b.node_id << ">";
    }

    bool ticket_sell::operator!=(const ticket_sell &rhs) const {
        return std::tie(client_id, ticket_count) != std::tie(rhs.client_id, rhs.ticket_count);
    }

    std::ostream &operator<<(std::ostream &os, const ticket_sell &ts) {
        return os << "ts(" << ts.client_id << ", " << ts.ticket_count << ")";
    }

    std::ostream &operator<<(std::ostream &os, const config_chg &cc) {
        return os << "cc(" << cc.new_node1 << ", " << cc.new_node2 << ")";
    }

    bool config_chg::operator!=(const config_chg &rhs) const {
        return std::tie(new_node1, new_node2) != std::tie(rhs.new_node1, rhs.new_node2);
    }

    value::value() {
        type = -1;
    }

    value::value(int, ticket_sell tsell)
            : type(0), ts(tsell) {}

    value::value(int, ticket_sell, config_chg cchg)
            : type(1), cc(cchg) {}

    bool value::operator!=(const value &rhs) const {
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

    bool value::operator==(const value &rhs) const {
        return !(*this != rhs);
    }

    bool promise::operator!=(const promise &rhs) const {
        return std::tie(bal, accept_num, accept_val) !=
               std::tie(rhs.bal, rhs.accept_num, rhs.accept_val);
    }

    bool promise::operator==(const promise &rhs) const {
        return !(*this != rhs);
    }
}