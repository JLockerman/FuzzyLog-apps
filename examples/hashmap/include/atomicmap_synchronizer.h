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

// Synchronizer class which eagerly synchronize with fuzzylog 
class AtomicMapSynchronizer : public Runnable {
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
        
public:
        AtomicMapSynchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors);
        ~AtomicMapSynchronizer() {}
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void Execute(); 

        void enqueue_get(std::condition_variable* cv, std::atomic_bool* cv_spurious_wake_up);
        void swap_queue();
        std::mutex* get_local_map_lock();

        uint32_t get(uint32_t key);
};
