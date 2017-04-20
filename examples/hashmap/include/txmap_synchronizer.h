#pragma once

#include <runnable.h>
#include <iostream>
#include <sstream>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <unordered_map>
#include <request.h>

extern "C" {
        #include "fuzzy_log.h"
}

class TXMapContext: public Context {
public:
        uint64_t                        m_local_key_range_start;
        uint64_t                        m_local_key_range_end;
        uint64_t                        m_remote_key_range_start;
        uint64_t                        m_remote_key_range_end;
public:
        TXMapContext(uint64_t lstart, uint64_t lend, uint64_t rstart, uint64_t rend): Context(), m_local_key_range_start(lstart), m_local_key_range_end(lend), m_remote_key_range_start(rstart), m_remote_key_range_end(rend) {}
        ~TXMapContext() {}
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
        txmap_set read_set;
        txmap_set write_set;
        txmap_commit_node* clone() {
                txmap_commit_node* new_node = static_cast<txmap_commit_node*>(malloc(sizeof(txmap_commit_node)));
                new_node->node = this->node;
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
        uint32_t decision;
        txmap_decision_node* clone() {
                txmap_decision_node* new_node = static_cast<txmap_decision_node*>(malloc(sizeof(txmap_decision_node)));
                new_node->node = this->node;
                new_node->commit_version = this->commit_version;
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
        struct colors*                                                          m_interesting_colors;
        struct colors*                                                          m_local_color;
        struct colors*                                                          m_remote_color;
        char                                                                    m_write_buf[DELOS_MAX_DATA_SIZE];
        
        Context*                                                                m_context;

        std::atomic_bool                                                        m_running;
        std::unordered_map<uint64_t, uint64_t>                                  m_local_map;    // Hack: let's store version into value 
        std::mutex                                                              m_local_map_mtx;
        std::deque<txmap_commit_node*>                                          m_buffered_commit_nodes;
        std::deque<LocationInColor>                                             m_buffered_commit_versions;
        std::deque<txmap_decision_node*>                                        m_buffered_decision_nodes;

        bool                                                                    m_replication;
        
public:
        TXMapSynchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, Context* context, bool replication);
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
        void append_decision_node_to_remote(LocationInColor commit_version, bool committed);
        void serialize_decision_record(txmap_decision_node *decision_node, char* out, size_t* out_size);
        void deserialize_decision_record(uint8_t *in, size_t size, txmap_decision_node *decision_node);
        bool validate_txn(txmap_commit_node *commit_node);
        uint64_t get_latest_key_version(uint64_t key);
        void update_map(txmap_commit_node *commit_node, LocationInColor commit_version);
        bool is_local_key(uint64_t key);
        bool is_local_only_txn(txmap_commit_node* commit_node);
        void buffer_commit_node(txmap_commit_node* commit_node, LocationInColor commit_version);
        void buffer_decision_node(txmap_decision_node* decision_node);
        bool apply_buffered_nodes(txmap_decision_node *decision_node);
        void log(char *file_name, char *prefix, txmap_node *node, LocationInColor commit_version=0, LocationInColor latest_key_version=0, bool decision=false);
};
