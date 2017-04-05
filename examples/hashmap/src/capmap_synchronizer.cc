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
        m_fuzzylog_client = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, c);

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
        synchronizer->Execute();
        return NULL;
}

void CAPMapSynchronizer::Execute() {
        std::cout << "Start CAPMap synchronizer..." << std::endl;
        uint32_t previous_flag = 0;
        CAPMap::PartitionStatus partition_status;
        struct colors* local_color = get_local_color();

        while (m_running) {
                partition_status = m_map->get_network_partition_status();
                // Wait until partition heals
                while (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                        previous_flag = sync_with_log(previous_flag, local_color);
                        //std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        partition_status = m_map->get_network_partition_status();
                }

                // If it's right after partition heals, reorder any weakly appended nodes. Any gets would be served after this
                if (partition_status == CAPMap::PartitionStatus::REORDER_WEAK_NODES) {
                        //std::cout << "Switch to Reordering state..." << std::endl;
                        auto start = std::chrono::system_clock::now();
                        previous_flag = sync_with_log(previous_flag, m_interesting_colors);
                        auto end = std::chrono::system_clock::now();
                        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                        //std::cout << "Playing after partition:" << duration.count() << " msec" << std::endl; 
                         

                        //std::cout << "Switch to Normal state..." << std::endl;
                        // Set partition status back to normal
                        m_map->set_network_partition_status(CAPMap::PartitionStatus::NORMAL);
                }

                // m_pending_queue ==> m_current_queue
                swap_queue();

                // Update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        previous_flag = sync_with_log(previous_flag, m_interesting_colors);
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
        delete local_color;
}

uint32_t CAPMapSynchronizer::sync_with_log(uint32_t previous_flag, struct colors* interesting_color) {
        uint32_t key, val, flag;
        long ts;
        size_t size;
        val = 0;
        size = 0;
        struct Node* new_node;

        snapshot(m_fuzzylog_client);
        while ((get_next(m_fuzzylog_client, m_read_buf, &size, interesting_color), 1)) {
                if (interesting_color->numcolors == 0) break;
                // Read node
                assert(size == sizeof(struct Node));
                struct Node* data = (struct Node*)m_read_buf;
                key = data->key;
                val = data->value;
                flag = data->flag;
                ts = data->ts;

                if (previous_flag != flag) {                    // Transition
                        if (flag == STRONG_FLAG) {              // W->S
                                reorder_weak_nodes_if_any();    //   Reorder weak nodes (and apply updates)
                                m_local_map[key] = val;         //   Apply update

                        } else if (flag == WEAK_FLAG) {         // S->W
                                new_node = data->clone();
                                buffer_weak_nodes(new_node);    //   Buffer weak nodes

                        } else {
                                assert(false);
                        }

                        //std::cout << "[REORDERING] color:" << m_interesting_colors->mycolors[0] << ", flag:" << flag << ", ts:" << ts << std::endl;
                        previous_flag = flag;

                } else {                                        // Continuous nodes
                        if (flag == STRONG_FLAG) {              // S->S 
                                m_local_map[key] = val;         //   Apply update

                        } else if (flag == WEAK_FLAG) {         // W->W
                                new_node = data->clone();
                                buffer_weak_nodes(new_node);    //   Buffer weak nodes

                        } else {
                                assert(false);
                        }

                }
        }
        return flag;
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

bool compare_nodes(struct Node* i, struct Node* j) {
        return i->ts < j->ts;
}

void CAPMapSynchronizer::reorder_weak_nodes_if_any() {
        // TODO: sort 
        //std::cout << "Reordering " << m_weak_nodes.size() << " weak nodes..." << std::endl;
        auto start = std::chrono::system_clock::now();
        std::sort(m_weak_nodes.begin(), m_weak_nodes.end(), compare_nodes); 
        auto end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        //std::cout << "Sorting:" << duration.count() << " msec" << std::endl; 
        start = end; 

        // After sorted, apply updates
        for (auto w : m_weak_nodes) {
                m_local_map[w->key] = w->value;
                // TODO: Mark that this node is reordered into log
                // Deallocate memory
                delete w;
        }
        end = std::chrono::system_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        //std::cout << "Applying:" << duration.count() << " msec" << std::endl; 
        m_weak_nodes.clear();
}

void CAPMapSynchronizer::buffer_weak_nodes(struct Node* node) {
        m_weak_nodes.push_back(node);
}
