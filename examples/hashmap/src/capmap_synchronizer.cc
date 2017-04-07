#include <algorithm>
#include <capmap_synchronizer.h>
#include <cstring>
#include <cassert>
#include <thread>
#include <capmap.h>

CAPMapSynchronizer::CAPMapSynchronizer(CAPMap* map, std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors): m_map(map) {
        uint32_t i;
        struct colors* c;
        c = (struct colors*)malloc(sizeof(struct colors));
        c->numcolors = interesting_colors.size();
        c->mycolors = (ColorID*)malloc(sizeof(ColorID) * c->numcolors);
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
        CAPMapSynchronizer *synchronizer= (CAPMapSynchronizer*)arg;
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
        struct colors* partition_playback_color = get_local_color();

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
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        delete partition_playback_color;
}

void CAPMapSynchronizer::ExecuteProtocol2Primary() {
        std::cout << "Start CAPMap synchronizer protocol 2 for primary..." << std::endl;
        uint32_t previous_flag = 0;
        struct colors* snapshot_color = get_protocol2_primary_snapshot_color();
        struct colors* playback_color = get_protocol2_primary_playback_color();

        while (m_running) {
                swap_queue();

                // Update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        previous_flag = sync_with_log_ver2(previous_flag, snapshot_color, playback_color);
                //        std::cout << "playback color numbers after sync:" << playback_color->numcolors << std::endl;
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
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        delete snapshot_color;
}

void CAPMapSynchronizer::ExecuteProtocol2Secondary() {
        std::cout << "Start CAPMap synchronizer protocol 2 for secondary..." << std::endl;
        CAPMap::PartitionStatus partition_status;
        struct colors* snapshot_color = get_protocol2_secondary_snapshot_color();
        struct colors* partition_snapshot_color = get_local_color();
        struct colors* playback_color = get_protocol2_secondary_playback_color();
        struct colors* partition_playback_color = get_local_color();

        while (m_running) {
                partition_status = m_map->get_network_partition_status();
                // Wait until partition heals
                while (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                        sync_with_log_ver2_secondary(partition_snapshot_color, partition_playback_color);
                        partition_status = m_map->get_network_partition_status();
                }

                // If it's right after partition goes away, reorder any weakly appended nodes. Any gets would be served after this
                if (partition_status == CAPMap::PartitionStatus::HEALING) {
                        // TODO: logic: playback primary color until it sees healing node 
                        sync_with_log_ver2_secondary(snapshot_color, playback_color);
                        // Set partition status back to normal
                        m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                }

                swap_queue();

                // Update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        sync_with_log_ver2_secondary(snapshot_color, playback_color);
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
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        delete snapshot_color;
        delete partition_snapshot_color;
        delete partition_playback_color;
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
                struct Node* data = (struct Node*)m_read_buf;
                key = data->key;
                val = data->value;
                flag = data->flag;
                ts = data->ts;

                if (previous_flag != flag) {                            // Transition
                        if (flag == STRONG_NODE) {                      // W->S
                                apply_buffered_nodes(true);             //   Reorder weak nodes and apply updates
                                m_local_map[key] = val;                 //   Apply update

                        } else if (flag == WEAK_NODE) {                 // S->W
                                new_node = data->clone();
                                buffer_nodes(new_node);                 //   Buffer weak nodes

                        } else {
                                assert(false);
                        }
                        previous_flag = flag;

                } else {                                                // Continuous nodes
                        if (flag == STRONG_NODE) {                      // S->S 
                                m_local_map[key] = val;                 //   Apply update

                        } else if (flag == WEAK_NODE) {                 // W->W
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

uint32_t CAPMapSynchronizer::sync_with_log_ver2(uint32_t previous_flag, struct colors* snapshot_color, struct colors* playback_color) {
        uint32_t key, val, flag;
        long ts;
        size_t size;
        val = 0;
        size = 0;
        struct colors* mutable_playback_color = clone_color(playback_color);

        assert(snapshot_color->numcolors == 1);
        assert(playback_color->numcolors == 2);

        snapshot_colors(m_fuzzylog_client, snapshot_color);
        while((get_next(m_fuzzylog_client, m_read_buf, &size, mutable_playback_color), 1)) {
                if (mutable_playback_color->numcolors == 0) break;

                assert(mutable_playback_color->numcolors == 1);
                assert(size == sizeof(struct Node));
                struct Node* data = (struct Node*)m_read_buf;
                key = data->key;
                val = data->value;
                flag = data->flag;
                ts = data->ts;
                if (flag == NORMAL_NODE) {
                        if (is_local_color(mutable_playback_color)) {
                                // update to local map
                                m_local_map[key] = val;

                        } else if (is_remote_color(mutable_playback_color)) {
                                buffer_nodes(data->clone());
                        }

                } else if (flag == HEALING_NODE) {
                        std::cout << "healing node found" << std::endl;
                        assert(m_map->get_network_partition_status() == CAPMap::PartitionStatus::NORMAL);
                        // Start healing
                        m_map->set_network_partition_status(CAPMap::PartitionStatus::HEALING);
                        apply_buffered_nodes(false);
                        m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);

                        // update to local map
                        m_local_map[key] = val;

                } else if (flag == PARTITIONING_NODE) {
                        std::cout << "partitioning node found" << std::endl;
                        buffer_nodes(data->clone());
                }
                delete mutable_playback_color;
                mutable_playback_color = clone_color(playback_color);
        }
        delete mutable_playback_color;
        return flag;
}

void CAPMapSynchronizer::sync_with_log_ver2_secondary(struct colors* snapshot_color, struct colors* playback_color) {
        uint32_t key, val, flag;
        long ts;
        size_t size;
        val = 0;
        size = 0;
        struct colors* mutable_playback_color = clone_color(playback_color);

        assert(snapshot_color->numcolors == 1);
        assert(playback_color->numcolors == 2);

        snapshot_colors(m_fuzzylog_client, snapshot_color);
        while((get_next(m_fuzzylog_client, m_read_buf, &size, mutable_playback_color), 1)) {
                if (mutable_playback_color->numcolors == 0) break;

                assert(mutable_playback_color->numcolors == 1);
                assert(size == sizeof(struct Node));
                struct Node* data = (struct Node*)m_read_buf;
                key = data->key;
                val = data->value;
                flag = data->flag;
                ts = data->ts;
                CAPMap::PartitionStatus status = m_map->get_network_partition_status();
                
                if (flag == NORMAL_NODE) {
                        if (is_local_color(mutable_playback_color)) {
                                // update to local map
                                m_local_map[key] = val;

                        } else if (is_remote_color(mutable_playback_color)) {
                                if (status == CAPMap::PartitionStatus::NORMAL) {
                                        // update to local map
                                        m_local_map[key] = val;

                                } else if (status == CAPMap::PartitionStatus::PARTITIONED
                                           || status == CAPMap::PartitionStatus::HEALING) {
                                        buffer_nodes(data->clone());
                                }
                        }

                } else if (flag == HEALING_NODE) {
                        std::cout << "healing node found" << std::endl;
                        assert(m_map->get_network_partition_status() == CAPMap::PartitionStatus::HEALING);
                        // Start healing
                        apply_buffered_nodes(false);
                        m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);

                        // update to local map
                        m_local_map[key] = val;

                } else if (flag == PARTITIONING_NODE) {
                        std::cout << "partitioning node found" << std::endl;
                        buffer_nodes(data->clone());
                }
                delete mutable_playback_color;
                mutable_playback_color = clone_color(playback_color);
        }
        delete mutable_playback_color;
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

struct colors* CAPMapSynchronizer::get_local_color() {
        struct colors* local_color = (struct colors*)malloc(sizeof(struct colors));
        local_color->numcolors = 1;
        local_color->mycolors = (ColorID*)malloc(sizeof(ColorID));
        local_color->mycolors[0] = m_interesting_colors->mycolors[0];
        return local_color;
}

struct colors* CAPMapSynchronizer::get_remote_color() {
        struct colors* remote_color = (struct colors*)malloc(sizeof(struct colors));
        remote_color->numcolors = 1;
        remote_color->mycolors = (ColorID*)malloc(sizeof(ColorID));
        remote_color->mycolors[0] = m_interesting_colors->mycolors[1];
        return remote_color;
}

struct colors* CAPMapSynchronizer::clone_color(struct colors* color) {
        struct colors* cloned_color = (struct colors*)malloc(sizeof(struct colors));
        cloned_color->numcolors = color->numcolors;
        cloned_color->mycolors = (ColorID*)malloc(sizeof(ColorID) * color->numcolors);
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
        return get_local_color();
}

struct colors* CAPMapSynchronizer::get_protocol2_primary_playback_color() {
        return m_interesting_colors;
}

struct colors* CAPMapSynchronizer::get_protocol2_secondary_snapshot_color() {
        return get_remote_color();
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
