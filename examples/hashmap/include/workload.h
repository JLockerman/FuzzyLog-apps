#ifndef           WORKLOAD_H_ 
#define           WORKLOAD_H_ 

#include <hashmap.h>

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
        vector<uint32_t>*                       m_color_of_interest;
        uint32_t                                m_single_operation_count;
        uint32_t                                m_multi_operation_count;
public:
        workload_generator(uint32_t range, vector<uint32_t>* color_of_interest, uint32_t single_operation_count, uint32_t multi_operation_count) {
                this->m_range = range;
                this->m_color_of_interest = color_of_interest;
                this->m_multi_operation_count = multi_operation_count;
                this->m_single_operation_count = single_operation_count;
        }
        Txn** Gen();
};

#endif            // WORKLOAD_H_ 
