#include <txmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

TXMap::TXMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, bool replication): BaseMap(log_addr, replication), m_commit_record_id(0) {
        std::vector<ColorID> interesting_colors;
        get_interesting_colors(workload, interesting_colors);
        init_synchronizer(log_addr, interesting_colors, replication);

        // init color
        init_op_color(interesting_colors);
}

TXMap::~TXMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
}

void TXMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        assert(workload->size() == 1);
        workload_config &w = workload->at(0);
        assert(w.first_color.numcolors == 2);           // Assumes two servers 
        uint32_t i;
        for (i = 0; i < w.first_color.numcolors; i++) {
                interesting_colors.push_back(w.first_color.mycolors[i]);
        }
}

void TXMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication) {
        m_synchronizer = new TXMapSynchronizer(log_addr, interesting_colors, replication);
        m_synchronizer->run();
}

void TXMap::init_op_color(std::vector<ColorID>& interesting_colors) {
        m_op_color.numcolors = interesting_colors.size();
        m_op_color.mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)*interesting_colors.size()));

        uint32_t i;
        for (i = 0; i < interesting_colors.size(); i++) {
                m_op_color.mycolors[i] = interesting_colors[i];
        }
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

void TXMap::serialize_commit_record(txmap_set *rset, txmap_set *wset, char* out, size_t* out_size) {
        uint32_t i, offset;
        txmap_set *buf_rset, *buf_wset;
        txmap_record *buf_record;

        offset = 0;
        buf_rset = reinterpret_cast<txmap_set*>(out);
        buf_rset->num_entry = rset->num_entry;
        offset += sizeof(uint32_t);
        for (i = 0; i < rset->num_entry; i++) {
                buf_record = reinterpret_cast<txmap_record*>(out + offset);
                *buf_record = rset->set[i];
                offset += sizeof(txmap_record);
        }
        buf_wset = reinterpret_cast<txmap_set*>(out + offset);  
        buf_wset->num_entry = wset->num_entry;
        offset += sizeof(uint32_t);
        for (i = 0; i < wset->num_entry; i++) {
                buf_record = reinterpret_cast<txmap_record*>(out + offset);
                *buf_record = wset->set[i];
                offset += sizeof(txmap_record);
        }
        *out_size = offset;
}

void TXMap::execute_move_txn(uint64_t from_key, uint64_t to_key) {
        uint64_t from_key_version;

        // readset
        from_key_version = get(from_key);
        txmap_record read_record;
        read_record.key = from_key; 
        read_record.value = 0;          // Value is irrelevant to the protocol 
        read_record.version = from_key_version; 
        txmap_set read_set;
        read_set.num_entry = 1;
        read_set.set = static_cast<txmap_record*>(malloc(sizeof(txmap_record)));
        read_set.set[0] = read_record;

        // writeset
        m_commit_record_id++;

        txmap_record local_write_record;
        local_write_record.key = from_key; 
        local_write_record.value = 0;          // Value is irrelevant to the protocol 
        local_write_record.version = m_commit_record_id; 

        txmap_record remote_write_record;
        remote_write_record.key = to_key; 
        remote_write_record.value = 0;          // Value is irrelevant to the protocol 
        remote_write_record.version = m_commit_record_id; 

        txmap_set write_set;
        write_set.num_entry = 2;
        write_set.set = static_cast<txmap_record*>(malloc(sizeof(txmap_record)*2));
        write_set.set[0] = local_write_record;
        write_set.set[1] = remote_write_record;

        // TODO: make payload
        size_t size = 0;
        serialize_commit_record(&read_set, &write_set, m_buf, &size);

        // Append commit record
        async_no_remote_append(m_fuzzylog_client, m_buf, size, &m_op_color, NULL, 0);
}
