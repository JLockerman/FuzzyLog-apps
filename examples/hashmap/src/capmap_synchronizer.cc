#include <algorithm>
#include <capmap_synchronizer.h>
#include <cstring>
#include <cassert>
#include <thread>
#include <capmap.h>

CAPMapSynchronizer::CAPMapSynchronizer(CAPMap* map, std::vector<std::string>* log_addr, std::vector<uint64_t>& interesting_colors, bool replication): m_map(map), m_replication(replication) {
        uint32_t i;
        ColorSpec c;
        c.num_remote_chains = interesting_colors.size();
        c.remote_chains = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * c.num_remote_chains));
        for (i = 0; i < c.num_remote_chains; i++) {
                c.remote_chains[i] = interesting_colors[i];
        }
        this->m_interesting_colors = c;
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
        this->m_running = true;
}

void CAPMapSynchronizer::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void CAPMapSynchronizer::join() {
        m_running = false;
        pthread_join(m_thread, NULL);
        fuzzylog_close(m_fuzzylog_client);
}

void* CAPMapSynchronizer::bootstrap(void *arg) {
        CAPMapSynchronizer *synchronizer= static_cast<CAPMapSynchronizer*>(arg);
        CAPMap::ProtocolVersion protocol = synchronizer->m_map->get_protocol_version();
        if (protocol == CAPMap::ProtocolVersion::VERSION_1) {
                assert(false);

        } else if (protocol == CAPMap::ProtocolVersion::VERSION_2) {
                std::string role = synchronizer->m_map->get_role();
                assert(role == "primary" || role == "secondary");
                if (role == "primary") {
                        synchronizer->ExecuteProtocol2Primary();
                } else if (role == "secondary") {
                        synchronizer->ExecuteProtocol2Secondary();
                }

        } else {
                assert(false);
        }
        return NULL;
}

void CAPMapSynchronizer::ExecuteProtocol2Primary() {
        std::cout << "Start CAPMap synchronizer protocol 2 for primary..." << std::endl;
        uint64_t read_count = 0;
        uint64_t total_read_count = 0;

        while (m_running) {
                swap_queue();

                // Update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        read_count = sync_with_log_ver2_primary();
                        total_read_count += read_count;
                        //std::cout << "read_count: " << read_count << ", total_read_count: " << total_read_count << std::endl;
                }
                // Wake up all waiting worker threads
                while (!m_current_queue.empty()) {
                        std::condition_variable* cv = m_current_queue.front().first;
                        std::atomic_bool* cv_spurious_wake_up = m_current_queue.front().second;
                        assert(*cv_spurious_wake_up == true);
                        *cv_spurious_wake_up = false;
                        cv->notify_one();
                        m_current_queue.pop();
                }
                std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }
}

void CAPMapSynchronizer::ExecuteProtocol2Secondary() {
        std::cout << "Start CAPMap synchronizer protocol 2 for secondary..." << std::endl;
        uint64_t read_count = 0;
        uint64_t total_read_count = 0;

        while (m_running) {
                swap_queue();

                // Update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        read_count = sync_with_log_ver2_secondary();
                        total_read_count += read_count;
                        //std::cout << "read_count: " << read_count << ", total_read_count: " << total_read_count << std::endl;
                }
                // Wake up all waiting worker threads
                while (!m_current_queue.empty()) {
                        std::condition_variable* cv = m_current_queue.front().first;
                        std::atomic_bool* cv_spurious_wake_up = m_current_queue.front().second;
                        assert(*cv_spurious_wake_up == true);
                        *cv_spurious_wake_up = false;
                        cv->notify_one();
                        m_current_queue.pop();
                }
                std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }
}

extern "C" {
        SnapId fuzzylog_sync_chain(FLPtr handle,
                           uint64_t chain,
                           void (*callback)(void*, FuzzyLogEvent),
                           void *callback_state);

        bool fuzzylog_event_inhabits_chain(FuzzyLogEvent event, uint64_t chain);
}

uint64_t CAPMapSynchronizer::sync_with_log_ver2_primary() {
        uint64_t snapshot_chain = clone_local_chain();
        uint64_t events_read = 0;

        std::tuple<CAPMapSynchronizer*, uint64_t*> args = std::make_tuple(this, &events_read);

        auto snap = fuzzylog_sync_chain(
                m_fuzzylog_client,
                snapshot_chain,
                [](void *args, FuzzyLogEvent event) {

                uint64_t *read_count;
                CAPMapSynchronizer *self;
                auto a = reinterpret_cast<const std::tuple<CAPMapSynchronizer*, uint64_t*>*>(args);
                std::tie(self, read_count) = *a;

                *read_count += 1;
                assert(event.inhabits_len == 1);
                assert(event.data_size == sizeof(struct Node));

                struct Node* data = (struct Node*)event.data;
                uint64_t key = data->key;
                uint64_t val = data->value;
                uint8_t flag = data->flag;

                if (self->is_remote(event)) {
                        switch (flag) {
                        case CAPMap::PartitioningNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Partitioning node found in primary" << std::endl;
                                // XXX: no break
                        case CAPMap::NormalNode:
                                self->buffer_nodes(data->clone());
                                break;
                        default:
                                assert(false);
                        }
                } else if (self->is_local(event)) {
                        switch (flag) {
                        case CAPMap::HealingNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Healing node found in primary" << std::endl;
                                assert(self->m_map->get_network_partition_status() == CAPMap::PartitionStatus::NORMAL);
                                self->m_map->set_network_partition_status(CAPMap::PartitionStatus::HEALING);
                                self->apply_buffered_nodes(false);
                                self->m_local_map[key] = val;
                                self->m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                                break;
                        case CAPMap::NormalNode:
                                self->m_local_map[key] = val;
                                break;
                        default:
                                assert(false);
                        }
                }
        }, &args);
        delete_snap_id(snap);

        return events_read;
}

uint64_t CAPMapSynchronizer::sync_with_log_ver2_secondary() {
        SnapId snap;

        uint64_t events_read = 0;

        std::tuple<CAPMapSynchronizer*, uint64_t*> args = std::make_tuple(this, &events_read);

        auto callback = [](void *args, FuzzyLogEvent event) {
                uint64_t *read_count;
                CAPMapSynchronizer *self;
                auto a = reinterpret_cast<const std::tuple<CAPMapSynchronizer*, uint64_t*>*>(args);
                std::tie(self, read_count) = *a;

                *read_count += 1;

                assert(event.inhabits_len == 1);
                assert(event.data_size == sizeof(struct Node));

                struct Node* data = (struct Node*)event.data;
                uint64_t key = data->key;
                uint64_t val = data->value;
                uint8_t flag = data->flag;

                if (self->is_remote(event)) {
                        switch (flag) {
                        case CAPMap::NormalNode:
                                self->m_local_map[key] = val;
                                break;
                        case CAPMap::HealingNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Healing node found in secondary" << std::endl;
                                self->apply_buffered_nodes(false);
                                self->m_local_map[key] = val;
                                self->m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                                break;
                        default:
                                assert(false);
                        }
                } else if (self->is_local(event)) {
                        switch (flag) {
                        case CAPMap::PartitioningNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Partitioning node found in secondary" << std::endl;
                                // XXX: no break
                        case CAPMap::NormalNode:
                                self->buffer_nodes(data->clone());
                                break;
                        default:
                                assert(false);
                        }
                }
        };

        if (m_map->get_network_partition_status() == CAPMap::PartitionStatus::PARTITIONED) {
                snap = fuzzylog_sync_chain(
                        m_fuzzylog_client, clone_local_chain(), callback, &args);
        } else {
                snap = fuzzylog_sync_events(m_fuzzylog_client, callback,&args);
        }

        delete_snap_id(snap);

        return events_read;
}


void CAPMapSynchronizer::enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up) {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        m_pending_queue.push(std::make_pair(cv, cv_spurious_wake_up));
}

void CAPMapSynchronizer::swap_queue() {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        std::swap(m_pending_queue, m_current_queue);
}

std::mutex* CAPMapSynchronizer::get_local_map_lock() {
        return &m_local_map_mtx;
}

uint64_t CAPMapSynchronizer::get(uint64_t key) {
        return m_local_map[key];
}

uint64_t CAPMapSynchronizer::clone_local_chain() {
        return m_interesting_colors.remote_chains[0];
}

uint64_t CAPMapSynchronizer::clone_remote_chain() {
        return m_interesting_colors.remote_chains[1];
}

ColorSpec CAPMapSynchronizer::clone_all_colors() {
        return clone_color(m_interesting_colors);
}

ColorSpec CAPMapSynchronizer::clone_color(ColorSpec color) {
        ColorSpec cloned_color = {
                .local_chain = color.local_chain,
                .num_remote_chains = color.num_remote_chains,
                .remote_chains = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * color.num_remote_chains)),
        };
        for (size_t i = 0; i < color.num_remote_chains; i++) {
                cloned_color.remote_chains[i] = color.remote_chains[i];
        }
        return cloned_color;
}

bool CAPMapSynchronizer::is_local(FuzzyLogEvent event) {
        return fuzzylog_event_inhabits_chain(
                event, m_interesting_colors.remote_chains[0]);
}

bool CAPMapSynchronizer::is_remote(FuzzyLogEvent event) {
        return fuzzylog_event_inhabits_chain(
                event, m_interesting_colors.remote_chains[1]);
}

bool compare_nodes(struct Node* i, struct Node* j) {
        return i->ts < j->ts;
}

void CAPMapSynchronizer::apply_buffered_nodes(bool reorder) {
        if (reorder)
                std::sort(m_buffered_nodes.begin(), m_buffered_nodes.end(), compare_nodes);

        // After sorted, apply updates
        for (auto w : m_buffered_nodes) {
                m_local_map[w->key] = w->value;
                // Deallocate memory
                delete w;
        }
        m_buffered_nodes.clear();
}

void CAPMapSynchronizer::buffer_nodes(struct Node* node) {
        m_buffered_nodes.push_back(node);
}
