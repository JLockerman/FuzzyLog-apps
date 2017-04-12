#include <algorithm>
#include <capmap_synchronizer.h>
#include <cstring>
#include <cassert>
#include <thread>
#include <capmap.h>

CAPMapSynchronizer::CAPMapSynchronizer(CAPMap* map, std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors): m_map(map) {
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
        size_t num_chain_servers = log_addr->size();
        const char *chain_server_ips[num_chain_servers]; 
        for (auto i = 0; i < num_chain_servers; i++) {
                chain_server_ips[i] = log_addr->at(i).c_str();
        }
        m_fuzzylog_client = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, m_interesting_colors);

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
                synchronizer->ExecuteProtocol1();

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

void CAPMapSynchronizer::ExecuteProtocol1() {
        std::cout << "Start CAPMap synchronizer protocol 1..." << std::endl;
        uint32_t previous_flag = 0;
        CAPMap::PartitionStatus partition_status;
        struct colors* normal_playback_color = get_protocol1_playback_color();
        struct colors* partition_playback_color = clone_local_color();

        while (m_running) {
                partition_status = m_map->get_network_partition_status();
                // Wait until partition heals
                while (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                        previous_flag = sync_with_log_ver1(previous_flag, partition_playback_color);
                        partition_status = m_map->get_network_partition_status();
                }

                // If it's right after partition goes away, reorder any weakly appended nodes. Any gets would be served after this
                if (partition_status == CAPMap::PartitionStatus::HEALING) {
                        previous_flag = sync_with_log_ver1(previous_flag, normal_playback_color);
                        // Set partition status back to normal
                        m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                }

                // m_pending_queue ==> m_current_queue
                swap_queue();

                // Update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        previous_flag = sync_with_log_ver1(previous_flag, normal_playback_color);
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
        }
        delete partition_playback_color;
}

void CAPMapSynchronizer::ExecuteProtocol2Primary() {
        std::cout << "Start CAPMap synchronizer protocol 2 for primary..." << std::endl;
        uint32_t read_count = 0;
        uint32_t total_read_count = 0;

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
        uint32_t read_count = 0;
        uint32_t total_read_count = 0;

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

uint32_t CAPMapSynchronizer::sync_with_log_ver1(uint32_t previous_flag, struct colors* interesting_color) {
        uint32_t key, val, flag;
        long ts;
        size_t size;
        val = 0;
        size = 0;
        struct Node* new_node;
        struct colors* mutable_playback_color = clone_color(interesting_color);
        

        snapshot(m_fuzzylog_client);
        while ((get_next(m_fuzzylog_client, m_read_buf, &size, mutable_playback_color), 1)) {
                if (mutable_playback_color->numcolors == 0) break;
                // Read node
                assert(size == sizeof(struct Node));
                struct Node* data = reinterpret_cast<struct Node*>(m_read_buf);
                key = data->key;
                val = data->value;
                flag = data->flag;
                ts = data->ts;

                if (previous_flag != flag) {                            // Transition
                        if (flag == CAPMap::StrongNode) {                      // W->S
                                apply_buffered_nodes(true);             //   Reorder weak nodes and apply updates
                                m_local_map[key] = val;                 //   Apply update

                        } else if (flag == CAPMap::WeakNode) {                 // S->W
                                new_node = data->clone();
                                buffer_nodes(new_node);                 //   Buffer weak nodes

                        } else {
                                assert(false);
                        }
                        previous_flag = flag;

                } else {                                                // Continuous nodes
                        if (flag == CAPMap::StrongNode) {                      // S->S 
                                m_local_map[key] = val;                 //   Apply update

                        } else if (flag == CAPMap::WeakNode) {                 // W->W
                                new_node = data->clone();
                                buffer_nodes(new_node);                 //   Buffer weak nodes

                        } else {
                                assert(false);
                        }

                }
                delete mutable_playback_color;
                mutable_playback_color = clone_color(interesting_color);
        }
        delete mutable_playback_color;
        return flag;
}

uint32_t CAPMapSynchronizer::sync_with_log_ver2_primary() {
        uint32_t key, val, flag;
        size_t size = 0;
        struct colors* snapshot_color = NULL;
        struct colors* playback_color = NULL;
        struct colors* mutable_playback_color = NULL;

        snapshot_color = clone_local_color();
        playback_color = clone_all_colors();
        snapshot_colors(m_fuzzylog_client, snapshot_color);
        uint32_t read_count = 0;

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

uint32_t CAPMapSynchronizer::sync_with_log_ver2_secondary() {
        uint32_t key, val, flag;
        size_t size = 0;
        struct colors* snapshot_color = NULL;
        struct colors* playback_color = NULL;
        struct colors* mutable_playback_color = NULL;
        CAPMap::PartitionStatus previous_status, current_status;
        previous_status = CAPMap::PartitionStatus::UNINITIALIZED;
        uint32_t read_count = 0;
        
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

uint32_t CAPMapSynchronizer::get(uint32_t key) {
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

struct colors* CAPMapSynchronizer::get_protocol1_snapshot_color() {
        return m_interesting_colors;
}

struct colors* CAPMapSynchronizer::get_protocol1_playback_color() {
        return m_interesting_colors;
}

struct colors* CAPMapSynchronizer::get_protocol2_primary_snapshot_color() {
        return clone_local_color();
}

struct colors* CAPMapSynchronizer::get_protocol2_primary_playback_color() {
        return m_interesting_colors;
}

struct colors* CAPMapSynchronizer::get_protocol2_secondary_snapshot_color() {
        return clone_remote_color();
}

struct colors* CAPMapSynchronizer::get_protocol2_secondary_playback_color() {
        return m_interesting_colors;
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
        //std::cout << "Reordering " << m_buffered_nodes.size() << " buffered nodes..." << std::endl;
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
