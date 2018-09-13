#include <map.h>
#include <thread>
#include <cassert>

#define DUMMY_INTERESTING_COLOR         100000

void BaseMap::init_fuzzylog_client(std::vector<std::string>* log_addr) {
        // XXX This color is not used
        ColorSpec c = {};

        // Initialize fuzzylog connection
        if (m_replication) {
                assert (log_addr->size() > 0 && log_addr->size() % 2 == 0);
                size_t num_chain_servers = log_addr->size() / 2;
                const char *chain_server_head_ips[num_chain_servers];
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_head_ips[i] = log_addr->at(i).c_str();
                }
                const char *chain_server_tail_ips[num_chain_servers];
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_tail_ips[i] = log_addr->at(num_chain_servers+i).c_str();
                }
                ServerSpec servers = {
                        .num_ips = num_chain_servers,
                        .head_ips = const_cast<char **>(&*chain_server_head_ips),
                        .tail_ips = const_cast<char **>(&*chain_server_tail_ips),
                };
                m_fuzzylog_client = new_fuzzylog_instance(servers, c, NULL);
        } else {
                size_t num_chain_servers = log_addr->size();
                const char *chain_server_ips[num_chain_servers];
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_ips[i] = log_addr->at(i).c_str();
                }
                ServerSpec servers = {
                        .num_ips = num_chain_servers,
                        .head_ips = const_cast<char **>(&*chain_server_ips),
                        .tail_ips = NULL,
                };
                m_fuzzylog_client = new_fuzzylog_instance(servers, c, NULL);
        }

        std::this_thread::sleep_for(std::chrono::seconds(3));
}

void BaseMap::put(uint64_t key, uint64_t value, ColorSpec op_color) {
        uint64_t data[2];
        data[0] = key;
        data[1] = value;
        size_t data_size = sizeof(uint64_t) * 2;
        char *bytes = reinterpret_cast<char*>(data);
        fuzzylog_append(m_fuzzylog_client, bytes, data_size, &op_color, 1);
}

void BaseMap::remove(uint64_t key, ColorSpec op_color) {
        // XXX: remove is the same as putting a zero value
        uint64_t data[2];
        data[0] = key;
        data[1] = 0;
        size_t data_size = sizeof(uint64_t) * 2;
        char *bytes = reinterpret_cast<char*>(data);
        fuzzylog_append(m_fuzzylog_client, bytes, data_size, &op_color, 1);
}

WriteId BaseMap::async_put(uint64_t key, uint64_t value, ColorSpec op_color) {
        // Async append
        uint64_t data[2];
        data[0] = key;
        data[1] = value;
        size_t data_size = sizeof(uint64_t) * 2;
        char *bytes = reinterpret_cast<char*>(data);
        return fuzzylog_async_append(m_fuzzylog_client, bytes, data_size, &op_color, 1);
}

extern "C" {
        void flush_completed_appends(FLPtr);
}

void BaseMap::flush_completed_puts() {
        // Flush completed requests with best-effort
        flush_completed_appends(m_fuzzylog_client);
}

WriteId BaseMap::try_wait_for_any_put() {
        // Wait for any append to be completed (non-blocking)
        return fuzzylog_try_wait_for_any_append(m_fuzzylog_client);
}

WriteId BaseMap::wait_for_any_put() {
        // Wait for any append to be completed (blocking)
        return fuzzylog_wait_for_any_append(m_fuzzylog_client);
}

void BaseMap::wait_for_all_puts() {
        fuzzylog_wait_for_all_appends(m_fuzzylog_client);
}
