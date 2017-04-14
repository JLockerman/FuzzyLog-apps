#include <algorithm>
#include <capmap_synchronizer.h>
#include <cstring>
#include <cassert>
#include <thread>
#include <capmap.h>

CAPMapSynchronizer::CAPMapSynchronizer(CAPMap* map, std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication): m_map(map), m_replication(replication) {
        uint32_t i;
        struct colors* c;
        c = static_cast<struct colors*>(malloc(sizeof(struct colors)));
        c->numcolors = interesting_colors.size();
        c->mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID) * c->numcolors));
        for (i = 0; i < c->numcolors; i++) {
                c->mycolors[i] = interesting_colors[i];
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
                m_fuzzylog_client = new_dag_handle_with_replication(num_chain_servers, chain_server_head_ips, chain_server_tail_ips, c);

        } else {
                size_t num_chain_servers = log_addr->size();
                const char *chain_server_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_ips[i] = log_addr->at(i).c_str();
                }
                m_fuzzylog_client = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, m_interesting_colors);
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
        close_dag_handle(m_fuzzylog_client);
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

uint64_t CAPMapSynchronizer::sync_with_log_ver2_primary() {
        uint64_t key, val;
        uint8_t flag;
        size_t size = 0;
        struct colors* snapshot_color = NULL;
        struct colors* playback_color = NULL;
        struct colors* mutable_playback_color = NULL;

        snapshot_color = clone_local_color();
        playback_color = clone_all_colors();
        snapshot_colors(m_fuzzylog_client, snapshot_color);
        uint64_t read_count = 0;

        while (true) {
                mutable_playback_color = clone_color(playback_color);
                get_next(m_fuzzylog_client, m_read_buf, &size, mutable_playback_color);
                if (mutable_playback_color->numcolors == 0) break;
                read_count++;

                assert(mutable_playback_color->numcolors == 1);
                assert(size == sizeof(struct Node));

                struct Node* data = reinterpret_cast<struct Node*>(m_read_buf);
                key = data->key;
                val = data->value;
                flag = data->flag;
                
                if (is_remote_color(mutable_playback_color)) {
                        switch (flag) {
                        case CAPMap::PartitioningNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Partitioning node found in primary" << std::endl;
                                // XXX: no break
                        case CAPMap::NormalNode:
                                buffer_nodes(data->clone());
                                break;
                        default:
                                assert(false);
                        }
                } else if (is_local_color(mutable_playback_color)) {
                        switch (flag) {
                        case CAPMap::HealingNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Healing node found in primary" << std::endl;
                                assert(m_map->get_network_partition_status() == CAPMap::PartitionStatus::NORMAL);
                                m_map->set_network_partition_status(CAPMap::PartitionStatus::HEALING);
                                apply_buffered_nodes(false);
                                m_local_map[key] = val;
                                m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                                break;
                        case CAPMap::NormalNode:
                                m_local_map[key] = val;
                                break;
                        default:
                                assert(false);
                        }
                }
                delete mutable_playback_color;
        }
        delete snapshot_color;
        delete playback_color;
        delete mutable_playback_color;
        return read_count;
}

uint64_t CAPMapSynchronizer::sync_with_log_ver2_secondary() {
        uint64_t key, val;
        uint8_t flag;
        size_t size = 0;
        struct colors* snapshot_color = NULL;
        struct colors* playback_color = NULL;
        struct colors* mutable_playback_color = NULL;
        CAPMap::PartitionStatus previous_status, current_status;
        previous_status = CAPMap::PartitionStatus::UNINITIALIZED;
        uint64_t read_count = 0;
        
        while (true) {
                current_status = m_map->get_network_partition_status();

                if (previous_status != current_status) {
                        // Deallocate memory
                        if (snapshot_color != NULL) delete snapshot_color;
                        if (playback_color != NULL) delete playback_color;

                        // Take new colors
                        if (current_status == CAPMap::PartitionStatus::PARTITIONED) {
                                snapshot_color = clone_local_color();
                                snapshot_colors(m_fuzzylog_client, snapshot_color);

                                playback_color = clone_local_color();

                        } else {
                                snapshot_color = clone_remote_color();
                                snapshot_colors(m_fuzzylog_client, snapshot_color);

                                playback_color = clone_all_colors();
                        }
                }
                mutable_playback_color = clone_color(playback_color);
                get_next(m_fuzzylog_client, m_read_buf, &size, mutable_playback_color);
                if (mutable_playback_color->numcolors == 0) break;
                read_count++;

                assert(mutable_playback_color->numcolors == 1);
                assert(size == sizeof(struct Node));

                struct Node* data = reinterpret_cast<struct Node*>(m_read_buf);
                key = data->key;
                val = data->value;
                flag = data->flag;
                
                if (is_remote_color(mutable_playback_color)) {
                        switch (flag) {
                        case CAPMap::NormalNode:
                                m_local_map[key] = val;
                                break;
                        case CAPMap::HealingNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Healing node found in secondary" << std::endl;
                                apply_buffered_nodes(false);
                                m_local_map[key] = val;
                                m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                                break;
                        default:
                                assert(false);
                        }
                } else if (is_local_color(mutable_playback_color)) {
                        switch (flag) {
                        case CAPMap::PartitioningNode:
                                std::cout << std::chrono::system_clock::now().time_since_epoch().count() << " Partitioning node found in secondary" << std::endl;
                                // XXX: no break
                        case CAPMap::NormalNode:
                                buffer_nodes(data->clone());
                                break;
                        default:
                                assert(false);
                        }
                }
                previous_status = current_status;
                delete mutable_playback_color;
        }
        delete snapshot_color;
        delete playback_color;
        delete mutable_playback_color;
        return read_count;
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

struct colors* CAPMapSynchronizer::clone_local_color() {
        struct colors* local_color = static_cast<struct colors*>(malloc(sizeof(struct colors)));
        local_color->numcolors = 1;
        local_color->mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)));
        local_color->mycolors[0] = m_interesting_colors->mycolors[0];
        return local_color;
}

struct colors* CAPMapSynchronizer::clone_remote_color() {
        struct colors* remote_color = static_cast<struct colors*>(malloc(sizeof(struct colors)));
        remote_color->numcolors = 1;
        remote_color->mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)));
        remote_color->mycolors[0] = m_interesting_colors->mycolors[1];
        return remote_color;
}

struct colors* CAPMapSynchronizer::clone_all_colors() {
        return clone_color(m_interesting_colors);
}

struct colors* CAPMapSynchronizer::clone_color(struct colors* color) {
        struct colors* cloned_color = static_cast<struct colors*>(malloc(sizeof(struct colors)));
        cloned_color->numcolors = color->numcolors;
        cloned_color->mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID) * color->numcolors));
        for (size_t i = 0; i < color->numcolors; i++) {
                cloned_color->mycolors[i] = color->mycolors[i];
        }
        return cloned_color;
}

bool CAPMapSynchronizer::is_local_color(struct colors* color) {
        return color->mycolors[0] == m_interesting_colors->mycolors[0];
}

bool CAPMapSynchronizer::is_remote_color(struct colors* color) {
        return color->mycolors[0] == m_interesting_colors->mycolors[1];
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
