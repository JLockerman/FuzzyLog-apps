#include <txmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

TXMap::TXMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, bool replication): BaseMap(log_addr, replication) {
        std::vector<ColorID> interesting_colors;
        get_interesting_colors(workload, interesting_colors);
        init_synchronizer(log_addr, interesting_colors, replication);
}

TXMap::~TXMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
}

void TXMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        assert(workload->size() == 1);
        workload_config &w = workload->at(0);
        assert(w.first_color.numcolors == 1);
        // Only playback local color
        uint32_t i;
        for (i = 0; i < w.first_color.numcolors; i++) {
                interesting_colors.push_back(w.first_color.mycolors[i]);
        }
}

void TXMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication) {
        m_synchronizer = new TXMapSynchronizer(log_addr, interesting_colors, replication);
        m_synchronizer->run();
}

uint64_t TXMap::get(uint64_t key) {
        uint64_t val;
        std::condition_variable cv;
        std::atomic_bool cv_spurious_wake_up;
        std::mutex* mtx;

        cv_spurious_wake_up = true;
        mtx = m_synchronizer->get_local_map_lock();
        std::unique_lock<std::mutex> lock(*mtx);
        m_synchronizer->enqueue_get(&cv, &cv_spurious_wake_up);
        cv.wait(lock, [&cv_spurious_wake_up]{ return cv_spurious_wake_up != true; });

        val = m_synchronizer->get(key);
        lock.unlock();

        return val;
}


void TXMap::beginTX() {
        // 
}

bool TXMap::endTX(txmap_set* read_set, txmap_set* write_set) {
        // Append commit record
    //  size_t size = 0;
    //  get_commit_payload(read_set, write_set, m_buf, &size);
    //  async_append();

        // Play forward
        
        return true;
}
