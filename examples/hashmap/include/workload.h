#ifndef           WORKLOAD_H_ 
#define           WORKLOAD_H_ 

#include <hashmap.h>

using namespace std;

class Txn {
public:
        Txn() {}
        virtual bool Run(HashMap *map) = 0;
};

class ycsb_insert : public Txn {
private:
        uint32_t start;
        uint32_t end;
        struct colors* color;
public:
        ycsb_insert(uint32_t start, uint32_t end, struct colors* color) {
                this->start = start;
                this->end = end;
                this->color = color;
        }
        virtual bool Run(HashMap *map);
};

class Table {
private:
        uint32_t record_count;
public:
        Table(uint32_t record_count) {
                this->record_count = record_count;
        }
        void populate(HashMap *map);
        uint32_t num_records() const;
};

class workload_generator {
private:
        Table *table;
        vector<uint32_t>* color_of_interest;
        uint32_t single_operation_count;
        uint32_t multi_operation_count;
public:
        workload_generator(Table *table, vector<uint32_t>* color_of_interest, uint32_t single_operation_count, uint32_t multi_operation_count) {
                this->table = table;
                this->color_of_interest = color_of_interest;
                this->multi_operation_count = multi_operation_count;
                this->single_operation_count = single_operation_count;
        }
        Txn** Gen();
        void colors_to_struct(vector<struct colors*>* out);
};

#endif            // WORKLOAD_H_ 
