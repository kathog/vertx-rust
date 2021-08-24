//
// Created by nerull on 18.04.2021.
//
#include <hazelcast/client/hazelcast_client.h>
#include <hazelcast/client/client_config.h>
#include <queue>

using namespace hazelcast::client;

class CServerID {
public:
    char * host;
    int port;

    CServerID() {}

    CServerID(char *host, int port) : host(host), port(port) {}
};

class CClusterNodeInfo {
public:
    CClusterNodeInfo() {

    }

    char * node_id;
    CServerID server_id;

    CClusterNodeInfo(char *nodeId, int port, char * host) : node_id(nodeId), server_id({host, port}) {}
};

std::shared_ptr<hazelcast_client> client;
std::shared_ptr<multi_map> subs;
std::shared_ptr<imap> ha_infos;
std::unordered_multimap<std::string, CClusterNodeInfo> local_subs;
std::unordered_map<std::string, std::queue<CServerID>> local_endpoints;

extern "C" void refresh_subs();


entry_listener make_subs_listener() {
    return entry_listener().on_added([](const entry_event &&event) {
        auto value = event.get_value();
        std::cout << "on_added: " << value.get_type() << std::endl;
        refresh_subs();
    }).on_removed([](const entry_event &&event) {
        refresh_subs();
    });
}

entry_listener make_ha_listener() {
    return entry_listener().on_added([](const entry_event &&event) {

    }).on_removed([](const entry_event &&event) {

    });
}

membership_listener make_membership_listener() {
    return membership_listener{}.on_left([](const membership_event &event) {

        std::cout << event.get_member() << std::endl;

    });
}
namespace hazelcast::client::serialization {
            template<>
            struct hz_serializer<CClusterNodeInfo> : custom_serializer {
                static constexpr int32_t get_type_id() noexcept {
                    return -2;
                }

                static void write(const CClusterNodeInfo &object, object_data_output &out) {
                    out.write(std::string(object.node_id));
                    out.write(object.server_id.port);
                    out.write(std::string(object.server_id.host));
                }

                static CClusterNodeInfo read(object_data_input &in) {
                    auto host = in.read<std::string>();
                    std::cout << host << std::endl;
//                    auto port = in.read<int32_t>();
//                    std::cout << port << std::endl;
////                    auto metadata = in.read<std::vector<byte>>();
                    return CClusterNodeInfo{

                    };
                }
            };
        }

class CClusterNodeInfoGlobalSerializer : public hazelcast::client::serialization::global_serializer {
public:

    void write(const boost::any &obj, hazelcast::client::serialization::object_data_output &out) override {
        auto const &object = boost::any_cast<CClusterNodeInfo>(obj);
        out.write(std::string(object.node_id));
        out.write(object.server_id.port);
        out.write(std::string(object.server_id.host));
    }

    boost::any read(hazelcast::client::serialization::object_data_input &in) override {
        std::cout << in.position() << std::endl;
        return boost::any(CClusterNodeInfo{
//            in.read<std::string>().data(),
//            in.read<int32_t>(),
//            in.read<std::string>().data()
        });
    }
};


extern "C" void join (int port, const char * host) {
    client_config config{};
    config.get_network_config().add_address(address(host, port));
    config.set_property(hazelcast::client::client_properties::INVOCATION_TIMEOUT_SECONDS, "2");
    config.get_serialization_config().set_global_serializer(std::make_shared<CClusterNodeInfoGlobalSerializer>());
    client = std::make_shared<hazelcast_client>(hazelcast::new_client(std::move(config)).get());
    client->get_cluster().add_membership_listener(std::move(make_membership_listener()));

    subs = client->get_multi_map("__vertx.subs").get();
    subs->add_entry_listener(make_subs_listener(), true);

    std::cout << subs->size().get() << std::endl;
    for (auto & [key, node] : subs->entry_set<std::string, CClusterNodeInfo>().get()) {
        std::cout <<  node.node_id << std::endl;
    }

    ha_infos = client->get_map("__vertx.haInfo").get();
    ha_infos->add_entry_listener(make_ha_listener(), true);
}

extern "C" void add_sub(const char * address, CClusterNodeInfo &node) {
    std::cout << "add_sub: " << node.node_id <<std::endl;
//    subs->put<std::string, CClusterNodeInfo>(address, node);
//    ha_infos->put<std::string, std::string>(node.node_id, "{\"verticles\":[],\"group\":\"__DISABLED__\",\"server_id\":{\"host\":\"" + std::string(node.server_id.host) +"\",\"port\":" + std::to_string(node.server_id.port) + "}}");
//    local_subs.emplace(address, node);
}