#pragma once

#include <hashmap.h>
#include <config.h>

using namespace std;

class Txn {
public:
        Txn() {}
        virtual ~Txn(){}
        virtual void Run(HashMap *map) = 0;
        virtual void AsyncRun(HashMap *map) = 0;
};

class ycsb_insert : public Txn {
private:
        uint32_t                                m_start;
        uint32_t                                m_end;
        struct colors*                          m_color;
public:
        ycsb_insert(uint32_t start, uint32_t end, struct colors* color) {
                this->m_start = start;
                this->m_end = end;
                this->m_color = color;
        }
        ~ycsb_insert(){}
        virtual void Run(HashMap *map);
        virtual void AsyncRun(HashMap *map);
};

class workload_generator {
private:
        uint32_t                                m_range;
        vector<workload_config>*                m_workload;
public:
        workload_generator(uint32_t range, vector<workload_config>* workload) {
                this->m_range = range;
                this->m_workload = workload;
        }
        Txn** Gen();
};
