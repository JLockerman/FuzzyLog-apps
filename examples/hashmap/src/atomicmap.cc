#include <atomicmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

AtomicMap::AtomicMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, bool replication): BaseMap(log_addr, replication), m_synchronizer(NULL) {
        std::vector<ColorID> interesting_colors;
        if (get_interesting_colors(workload, interesting_colors))
                init_synchronizer(log_addr, interesting_colors, replication);
}

AtomicMap::~AtomicMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
}

bool AtomicMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        bool get_workload_found = false;
        uint32_t i;
        for (auto w : *workload) {
                if (w.op_type == "get") {
                        for (i = 0; i < w.first_color.numcolors; i++) {
                                interesting_colors.push_back(w.first_color.mycolors[i]);
                        }
                        get_workload_found = true;
                        break;
                }
        }
        return get_workload_found;
}

void AtomicMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication) {
        m_synchronizer = new AtomicMapSynchronizer(log_addr, interesting_colors, replication);
        m_synchronizer->run();
}

void AtomicMap::multiput(uint64_t key, uint64_t value, struct colors* op_color, struct colors* dep_color) {
        assert(op_color->numcolors == 2);
        put(key, value, op_color, dep_color);
}

write_id AtomicMap::async_multiput(uint64_t key, uint64_t value, struct colors* op_color) {
        assert(op_color->numcolors == 2);
        return async_put(key, value, op_color);
}

uint64_t AtomicMap::get(uint64_t key) {
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
