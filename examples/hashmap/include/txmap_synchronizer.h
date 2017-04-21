#pragma once

#include <runnable.h>
#include <iostream>
#include <sstream>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <unordered_map>
#include <set>
#include <request.h>

extern "C" {
        #include "fuzzy_log.h"
}

class TXMapContext: public Context {
public:
        uint32_t                        m_request_window_size;
        std::atomic<uint64_t>           m_num_committed;
        std::atomic<uint64_t>           m_num_aborted;
        std::atomic<uint32_t>           m_num_pending_txns;
        uint32_t                        m_num_local_only_txns;
        uint32_t                        m_num_dist_txns;
        double                          m_rename_percent;
        uint64_t                        m_local_key_range_start;
        uint64_t                        m_local_key_range_end;
        uint64_t                        m_remote_key_range_start;
        uint64_t                        m_remote_key_range_end;
        // execution time
        std::chrono::system_clock::time_point   m_start_time;
        std::chrono::system_clock::time_point   m_validation_end_time;
        std::chrono::system_clock::time_point   m_execution_end_time;
        

public:
        TXMapContext(uint32_t request_window_size, double rename_percent, uint64_t lstart, uint64_t lend, uint64_t rstart, uint64_t rend): Context(), m_request_window_size(request_window_size), m_rename_percent(rename_percent), m_local_key_range_start(lstart), m_local_key_range_end(lend), m_remote_key_range_start(rstart), m_remote_key_range_end(rend) {
                this->m_num_committed = 0;
                this->m_num_aborted = 0;
                this->m_num_pending_txns = 0;
                this->m_num_local_only_txns = 0;
                this->m_num_dist_txns = 0;
        }
        ~TXMapContext() {}

        uint64_t get_num_committed() {
                return m_num_committed;
        }

        void inc_num_committed() {
                m_num_committed++;
                end_validation_measure_time();             // memorize last time when committ is decided
        }

        uint64_t get_num_aborted() {
                return m_num_aborted;
        }

        void inc_num_aborted() {
                m_num_aborted++;
                end_validation_measure_time();             // memorize last time when committ is decided
        }

        void inc_num_pending_txns() {
                m_num_pending_txns++;
        }

        void dec_num_pending_txns() {
                m_num_pending_txns--;
        }

        void inc_num_local_only_txns() {
                m_num_local_only_txns++;
        }
        
        uint32_t get_num_local_only_txns() {
                return m_num_local_only_txns;
        }

        void inc_num_dist_txns() {
                m_num_dist_txns++;
        }

        uint32_t get_num_dist_txns() {
                return m_num_dist_txns;
        }

        bool is_window_filled() {
                return m_num_pending_txns == m_request_window_size;
        }

        bool do_rename_txn() {
                double r = static_cast<double>(rand()) / (RAND_MAX) * 100.0;
                return r < m_rename_percent;
        }

        void start_measure_time() {
                m_start_time = std::chrono::system_clock::now();
        }

        void end_execution_measure_time() {
                m_execution_end_time = std::chrono::system_clock::now();
        }

        void end_validation_measure_time() {
                m_validation_end_time = std::chrono::system_clock::now();
        }

        double get_execution_time() {
                std::chrono::duration<double, std::milli> elapsed = m_execution_end_time - m_start_time;
                return elapsed.count() / 1000.0;
        }

        double get_validation_time() {
                std::chrono::duration<double, std::milli> elapsed = m_validation_end_time - m_start_time;
                return elapsed.count() / 1000.0;
        }

        double get_throughput() {
                return get_num_executed() / get_validation_time();
        }

        double get_goodput() {
                return get_num_committed() / get_validation_time();
        }
};

typedef struct txmap_record {
        uint64_t key;
        uint64_t value;
        uint64_t version;  
        std::string log() {
                std::stringstream ss;
                ss << "key:" << key << ",ver:" << version << std::endl;
                return ss.str();
        }
} txmap_record;

typedef struct txmap_set {
        uint32_t num_entry;
        txmap_record* set;
        //~txmap_set() { delete set; }
        std::string log() {
                std::stringstream ss;
                for (size_t i = 0; i < num_entry; i++)
                        ss << set[i].log();
                return ss.str();
        }
        txmap_set* clone() {
                txmap_set* new_set = static_cast<txmap_set*>(malloc(sizeof(txmap_set)));
                new_set->num_entry = this->num_entry;
                new_set->set = static_cast<txmap_record*>(malloc(sizeof(txmap_record)*this->num_entry));
                for (size_t i = 0; i < num_entry; i++) {
                        new_set->set[i] = this->set[i];
                }
                return new_set;
        }
} txmap_set;

typedef struct txmap_node {
        enum NodeType {
                COMMIT_RECORD = 1,
                DECISION_RECORD = 2,   
        };
        uint32_t node_type;
} txmap_node;

typedef struct txmap_commit_node {
        txmap_node node;
        LocationInColor commit_version;
        txmap_set read_set;
        txmap_set write_set;
        txmap_commit_node* clone() {
                txmap_commit_node* new_node = static_cast<txmap_commit_node*>(malloc(sizeof(txmap_commit_node)));
                new_node->node = this->node;
                new_node->commit_version = this->commit_version;
                new_node->read_set = *this->read_set.clone();
                new_node->write_set = *this->write_set.clone();
                return new_node;
        }
} txmap_commit_node;

typedef struct txmap_decision_node {
        enum DecisionType {
                COMMITTED = 1,
                ABORTED = 2,   
        };
        txmap_node node;
        LocationInColor commit_version;
        uint64_t key;
        uint32_t decision;
        txmap_decision_node* clone() {
                txmap_decision_node* new_node = static_cast<txmap_decision_node*>(malloc(sizeof(txmap_decision_node)));
                new_node->node = this->node;
                new_node->commit_version = this->commit_version;
                new_node->key = this->key;
                new_node->decision = this->decision;
                return new_node;
        }
} txmap_decision_node;

// Synchronizer class which eagerly synchronize with fuzzylog 
class TXMapSynchronizer : public Runnable {
private:
        std::queue<std::pair<std::condition_variable*, std::atomic_bool*>>      m_pending_queue;
        std::queue<std::pair<std::condition_variable*, std::atomic_bool*>>      m_current_queue;
        std::mutex                                                              m_queue_mtx;

        DAGHandle*                                                              m_fuzzylog_client;
        DAGHandle*                                                              m_append_client;
        struct colors*                                                          m_interesting_colors;
        struct colors*                                                          m_local_color;
        struct colors*                                                          m_remote_color;
        char                                                                    m_write_buf[DELOS_MAX_DATA_SIZE];
        
        Context*                                                                m_context;

        std::atomic_bool                                                        m_running;
        std::unordered_map<uint64_t, uint64_t>                                  m_local_map;    // Hack: let's store version into value 
        std::mutex                                                              m_local_map_mtx;

        std::set<uint64_t>                                                      m_waiting_for_decision_node;
        std::unordered_map<uint64_t, std::deque<txmap_commit_node*>*>           m_buffered_commit_nodes;
        std::unordered_map<uint64_t, std::deque<txmap_decision_node*>*>         m_buffered_decision_nodes;

        bool                                                                    m_replication;
        
public:
        TXMapSynchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, Context* context, DAGHandle* append_client, bool replication);
        ~TXMapSynchronizer() {}
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void Execute(); 

        void enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up);
        void swap_queue();
        std::mutex* get_local_map_lock();

        uint64_t get(uint64_t key);
        void put(uint64_t key, uint64_t value);

        // Txn validation
        void deserialize_commit_record(uint8_t *in, size_t size, txmap_commit_node *commit_node);
        bool is_decision_possible(txmap_commit_node *commit_node);
        void append_decision_node_to_remote(uint64_t remote_write_key, LocationInColor commit_version, bool committed);
        void serialize_decision_record(txmap_decision_node *decision_node, char* out, size_t* out_size);
        void deserialize_decision_record(uint8_t *in, size_t size, txmap_decision_node *decision_node);
        bool validate_txn(txmap_commit_node *commit_node);
        uint64_t get_latest_key_version(uint64_t key);
        void update_map(txmap_commit_node *commit_node);
        bool is_local_key(uint64_t key);
        bool is_local_only_txn(txmap_commit_node* commit_node);
        uint64_t get_remote_write_key(txmap_commit_node* commit_node);
        bool needs_buffering(txmap_commit_node *commit_node);
        void buffer_commit_node(txmap_commit_node* commit_node);
        void buffer_decision_node(txmap_decision_node* decision_node);
        void apply_buffered_nodes(txmap_decision_node *decision_node);
        void log(char *file_name, char *prefix, txmap_node *node, LocationInColor latest_key_version=0, bool decision=false);
};
