#pragma once

#include <map.h>
#include <atomic>
#include <unordered_map>
#include <chrono>


using namespace std;

class Context {
private:
        std::atomic<uint64_t>                   m_num_executed;
        std::atomic<bool>                       m_finished;
        std::unordered_map<WriteId, std::chrono::system_clock::time_point, wid_hasher, wid_equality>   m_request_map;
public:
        Context() {
                this->m_num_executed = 0;
                this->m_finished = false;
        }
        bool is_finished() {
                return m_finished;
        }
        void set_finished() {
                m_finished = true;
        }
        uint64_t get_num_executed() {
                return m_num_executed;
        }
        void inc_num_executed() {
                m_num_executed++;
        }
        void mark_started(WriteId wid, std::chrono::system_clock::time_point issued_time) {
                m_request_map[wid] = issued_time;
        }
        std::chrono::system_clock::time_point mark_ended(WriteId wid) {
                auto start_time = m_request_map[wid];
                m_request_map.erase(wid);
                return start_time;
        }
};

class Txn {
public:
        std::chrono::system_clock::time_point           m_start_time;
        std::chrono::system_clock::time_point           m_end_time;
public:
        typedef enum {
                GET = 0,
                PUT = 1,
        } optype;
public:
        Txn() {}
        virtual ~Txn(){}
        virtual void Run() = 0;
        virtual WriteId AsyncRun() = 0;
        virtual WriteId AsyncRemoteRun() = 0;
        virtual WriteId AsyncStronglyConsistentRun() = 0;
        virtual WriteId AsyncWeaklyConsistentRun() = 0;
        virtual optype op_type() = 0;
};
