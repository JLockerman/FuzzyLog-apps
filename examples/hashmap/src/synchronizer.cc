#include <synchronizer.h>
#include <cstring>
#include <cassert>
#include <thread>

static char out[DELOS_MAX_DATA_SIZE];

Synchronizer::Synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors) {
        uint32_t i;
        struct colors* c;
        c = (struct colors*)malloc(sizeof(struct colors));
        c->numcolors = interesting_colors.size();
        c->mycolors = (ColorID*)malloc(sizeof(ColorID) * c->numcolors);
        for (i = 0; i < c->numcolors; i++) {
                c->mycolors[i] = interesting_colors[i];
        }
        this->m_interesting_colors = c;

        if (log_addr->size() == 1) {
                const char *server_ip = log_addr->at(0).c_str();
                m_fuzzylog_client = new_dag_handle_for_single_server(server_ip, c);
        } else {
                const char *lock_server_ip = log_addr->at(0).c_str();
                size_t num_chain_servers = log_addr->size() - 1;
                const char *chain_server_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_ips[i] = log_addr->at(i+1).c_str();
                }
                m_fuzzylog_client = new_dag_handle(lock_server_ip, num_chain_servers, chain_server_ips, c);
        }

        this->m_running = true;
}

void Synchronizer::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void Synchronizer::join() {
        m_running = false; 
        pthread_join(m_thread, NULL);
        close_dag_handle(m_fuzzylog_client);
}

void* Synchronizer::bootstrap(void *arg) {
        Synchronizer *synchronizer= (Synchronizer*)arg;
        synchronizer->Execute();
        return NULL;
}

void Synchronizer::Execute() {
        std::cout << "Start synchronizer..." << std::endl;
        uint64_t data;
        uint32_t key, val;
        size_t size;
        val = 0;
        size = 0;

        while (m_running) {
                // m_pending_queue ==> m_current_queue
                swap_queue();

                // update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        snapshot(m_fuzzylog_client);
                        while ((get_next(m_fuzzylog_client, out, &size, m_interesting_colors), 1)) {
                                if (m_interesting_colors->numcolors == 0) break;
                                data = *(uint64_t *)out;
                                key = (uint32_t)(data >> 32);
                                val = (uint32_t)(data & 0xFFFFFFFF); 

                                // update to local map
                                m_local_map[key] = val;
                        }
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
}

void Synchronizer::enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up) {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        m_pending_queue.push(std::make_pair(cv, cv_spurious_wake_up));
}

void Synchronizer::swap_queue() {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        std::swap(m_pending_queue, m_current_queue);
}

std::mutex* Synchronizer::get_local_map_lock() {
        return &m_local_map_mtx;
}

uint32_t Synchronizer::get(uint32_t key) {
        return m_local_map[key];
}
