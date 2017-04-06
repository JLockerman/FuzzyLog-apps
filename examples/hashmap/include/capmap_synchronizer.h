#pragma once

#include <runnable.h>
#include <iostream>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <unordered_map>

extern "C" {
        #include "fuzzy_log.h"
}

class CAPMap;

// Synchronizer class which eagerly synchronize with fuzzylog 
class CAPMapSynchronizer : public Runnable {
private:
        std::queue<std::pair<std::condition_variable*, std::atomic_bool*>>      m_pending_queue;
        std::queue<std::pair<std::condition_variable*, std::atomic_bool*>>      m_current_queue;
        std::mutex                                                              m_queue_mtx;

        DAGHandle*                                                              m_fuzzylog_client;
        struct colors*                                                          m_interesting_colors;
        char                                                                    m_read_buf[DELOS_MAX_DATA_SIZE];

        std::atomic_bool                                                        m_running;
        std::unordered_map<uint32_t, uint32_t>                                  m_local_map; 
        std::mutex                                                              m_local_map_mtx;

        CAPMap*                                                                 m_map;
        std::vector<struct Node*>                                               m_buffered_nodes;

public:
        CAPMapSynchronizer(CAPMap* map, std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors);
        ~CAPMapSynchronizer() {}
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void ExecuteProtocol1(); 
        void ExecuteProtocol2Primary(); 
        void ExecuteProtocol2Secondary(); 
        uint32_t sync_with_log_ver1(uint32_t previous_flag, struct colors* interesting_color);
        uint32_t sync_with_log_ver2_primary();
        uint32_t sync_with_log_ver2_secondary();
        //void sync_with_log_ver2_secondary(struct colors* snapshot_color, struct colors* playback_color);

        void enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up);
        void swap_queue();
        std::mutex* get_local_map_lock();

        uint32_t get(uint32_t key);

        struct colors* clone_local_color();
        struct colors* clone_remote_color();
        struct colors* clone_all_colors();
        struct colors* clone_color(struct colors* color);
        struct colors* get_protocol1_snapshot_color();
        struct colors* get_protocol1_playback_color();

        struct colors* get_protocol2_primary_snapshot_color();
        struct colors* get_protocol2_primary_playback_color();
        struct colors* get_protocol2_secondary_snapshot_color();
        struct colors* get_protocol2_secondary_playback_color();

        bool is_local_color(struct colors* color);
        bool is_remote_color(struct colors* color);

        // For weak nodes
        void apply_buffered_nodes(bool reorder);
        void buffer_nodes(struct Node* node);
};
