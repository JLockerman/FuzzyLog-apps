#include <txmap_synchronizer.h>
#include <cstring>
#include <cassert>
#include <thread>
#include <atomicmap.h>

TXMapSynchronizer::TXMapSynchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication): m_replication(replication) {
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
                m_fuzzylog_client = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, c);
        }

        std::this_thread::sleep_for(std::chrono::seconds(3));
        this->m_running = true;
}

void TXMapSynchronizer::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void TXMapSynchronizer::join() {
        m_running = false; 
        pthread_join(m_thread, NULL);
        close_dag_handle(m_fuzzylog_client);
}

void* TXMapSynchronizer::bootstrap(void *arg) {
        TXMapSynchronizer *synchronizer= static_cast<TXMapSynchronizer*>(arg);
        synchronizer->Execute();
        return NULL;
}

void TXMapSynchronizer::Execute() {
        std::cout << "Start TXMap synchronizer..." << std::endl;
        size_t size = 0;

        while (m_running) {
                // m_pending_queue ==> m_current_queue
                swap_queue();

                // update local map
                {
                        std::lock_guard<std::mutex> lock(m_local_map_mtx);
                        snapshot(m_fuzzylog_client);
                        while ((get_next(m_fuzzylog_client, m_read_buf, &size, m_interesting_colors), 1)) {
                                if (m_interesting_colors->numcolors == 0) break;
                                // readset, writeset
                                txmap_set read_set, write_set;
                                // read commit record
                                deserialize_commit_record(m_read_buf, size, &read_set, &write_set);
                                // validate -> notify?
                                validate_txn(&read_set, &write_set);
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
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }
}

void TXMapSynchronizer::enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up) {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        m_pending_queue.push(std::make_pair(cv, cv_spurious_wake_up));
}

void TXMapSynchronizer::swap_queue() {
        std::lock_guard<std::mutex> lock(m_queue_mtx);
        std::swap(m_pending_queue, m_current_queue);
}

std::mutex* TXMapSynchronizer::get_local_map_lock() {
        return &m_local_map_mtx;
}

uint64_t TXMapSynchronizer::get(uint64_t key) {
        return m_local_map[key];
}

void TXMapSynchronizer::put(uint64_t key, uint64_t value) {
        m_local_map[key] = value;
}

void TXMapSynchronizer::deserialize_commit_record(char *in, size_t size, txmap_set *rset, txmap_set *wset) {
        // deserialize read set
        uint32_t offset;
        txmap_record *buf_record;
        
        txmap_set *buf_rset;
        offset = 0;
        buf_rset = reinterpret_cast<txmap_set*>(in);
        rset->num_entry = buf_rset->num_entry;
        offset += sizeof(uint32_t);
        rset->set = static_cast<txmap_record*>(malloc(sizeof(txmap_record)*rset->num_entry));
        for (size_t i = 0; i < rset->num_entry; i++) {
                buf_record = reinterpret_cast<txmap_record*>(in + offset); 
                rset->set[i] = *buf_record;
                offset += sizeof(txmap_record);
        }
        // deserialize write set 
        txmap_set *buf_wset;
        buf_wset = reinterpret_cast<txmap_set*>(in + offset);
        wset->num_entry = buf_wset->num_entry;
        offset += sizeof(uint32_t);
        wset->set = static_cast<txmap_record*>(malloc(sizeof(txmap_record)*wset->num_entry));
        for (size_t i = 0; i < wset->num_entry; i++) {
                buf_record = reinterpret_cast<txmap_record*>(in + offset); 
                wset->set[i] = *buf_record; 
                offset += sizeof(txmap_record);
        }
}

void TXMapSynchronizer::validate_txn(txmap_set *rset, txmap_set *wset) {
        assert(rset != NULL);
        assert(wset != NULL);
        uint32_t i, num_entry;
        txmap_record *record;
        bool valid = true;

        num_entry = rset->num_entry;
        for (i = 0; i < num_entry; i++) {
                record = &rset->set[i];         // FIXME: Wrong precedence?
                if (get_latest_key_version(record->key) != record->version) {
                        valid = false;
                        break;
                }
        } 
        
        if (valid) {
                update_map(wset);
        }
}

uint64_t TXMapSynchronizer::get_latest_key_version(uint64_t key) {
        return get(key);        // XXX Hack: let's store version into value
}

void TXMapSynchronizer::update_map(txmap_set *wset) {
        uint32_t i, num_entry;
        num_entry = wset->num_entry;
        for (i = 0; i < num_entry; i++) {
                txmap_record &r = wset->set[i];
                put(r.key, r.version);
        }
}
