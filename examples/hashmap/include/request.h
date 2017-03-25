#pragma once

using namespace std;

class Context {
private:
        std::atomic<uint64_t>                   m_num_executed;
        std::atomic<bool>                       m_finished;
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
};

class Txn {
public:
        typedef enum {
                GET = 0,
                PUT = 1,
        } optype;
public:
        Txn() {}
        virtual ~Txn(){}
        virtual void Run() = 0;
        virtual void AsyncRun() = 0;
        virtual bool TryAsyncStronglyConsistentRun() = 0;
        virtual void AsyncWeaklyConsistentRun() = 0;
        virtual optype op_type() = 0; 
};
