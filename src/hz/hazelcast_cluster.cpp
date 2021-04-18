//
// Created by nerull on 18.04.2021.
//
#include <hazelcast/client/hazelcast_client.h>
#include <hazelcast/client/client_config.h>

using namespace hazelcast::client;

extern "C" void join (int port, const char * host) {

    client_config config{};
    auto hz = hazelcast::new_client(std::move(config)).get();


}