
#include "node/replica.hh"
#include "replica.hh"
#include <optional>

using namespace std;

template <Replica::State T>
auto Replica::rx_payload_handler(const Replica::RequestVariant& variant)
    -> tuple<State, Replica::ReplyVariant> {

    State s = impl.state;
    ReplyVariant rv;
    switch (variant.index()) {
    case 0: {
        /* append entries */
        auto [s, reply] = add_entries<T>(get<0>(variant));
        rv = ReplyVariant(reply);
    } break;
    case 1: {
        auto [s, reply] = request_vote<T>(get<1>(variant));
        rv = ReplyVariant(reply);
    } break;
    }

    return {s, rv};
};

/* instantiate specialization */
template auto Replica::rx_payload_handler<Replica::Candidate>(
    const Replica::RequestVariant& variant)
    -> tuple<State, Replica::ReplyVariant>;

template auto Replica::rx_payload_handler<Replica::Leader>(
    const Replica::RequestVariant& variant)
    -> tuple<State, Replica::ReplyVariant>;