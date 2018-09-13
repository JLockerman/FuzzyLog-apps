#pragma once

#include <runnable.h>
#include <iostream>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <unordered_map>

extern "C" {
        #include "fuzzylog_async_ext.h"
}

class CAPMap;

// Synchronizer class which eagerly synchronize with fuzzylog
class CAPMapSynchronizer : public Runnable {
private:
        std::queue<std::pair<std::condition_variable*, std::atomic_bool*>>      m_pending_queue;
        std::queue<std::pair<std::condition_variable*, std::atomic_bool*>>      m_current_queue;
        std::mutex                                                              m_queue_mtx;

        FLPtr                                                              m_fuzzylog_client;
        ColorSpec                                                          m_interesting_colors;

        std::atomic_bool                                                        m_running;
        std::unordered_map<uint64_t, uint64_t>                                  m_local_map;
        std::mutex                                                              m_local_map_mtx;

        CAPMap*                                                                 m_map;
        std::vector<struct Node*>                                               m_buffered_nodes;

        bool                                                                    m_replication;

public:
        CAPMapSynchronizer(CAPMap* map, std::vector<std::string>* log_addr, std::vector<uint64_t>& interesting_colors, bool replication);
        ~CAPMapSynchronizer() {}
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void ExecuteProtocol2Primary();
        void ExecuteProtocol2Secondary();
        uint64_t sync_with_log_ver2_primary();
        uint64_t sync_with_log_ver2_secondary();
        //void sync_with_log_ver2_secondary(struct colors* snapshot_color, struct colors* playback_color);

        void enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up);
        void swap_queue();
        std::mutex* get_local_map_lock();

        uint64_t get(uint64_t key);

        uint64_t clone_local_chain();
        uint64_t clone_remote_chain();
        ColorSpec clone_all_colors();
        ColorSpec clone_color(ColorSpec color);

        bool is_local(FuzzyLogEvent event);
        bool is_remote(FuzzyLogEvent event);

        // For weak nodes
        void apply_buffered_nodes(bool reorder);
        void buffer_nodes(struct Node* node);
};
