#include <workload.h>
#include <cassert>
#include <random>
#include <iostream>

bool ycsb_insert::Run(HashMap *map) {
        uint32_t i, key, value;
        assert(map != NULL);
       
        for (i = 0; i < write_count; ++i) {
                // FIXME 
                // key = (start, end)
                // TODO: check multi-partition key
                //key = get_random_key();
                // value = rand
                //value = get_random_value();
                key = 0;
                value = 0;
                //std::cout << key << ", " << value << std::endl;
                map->put(key, value);
        }
        return true;
}

uint32_t ycsb_insert::num_writes() {
        return this->write_count;
}

uint32_t ycsb_insert::get_random_key() {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> uni(start, end);
        return (uint32_t)uni(rng);
}

uint32_t ycsb_insert::get_random_value() {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> uni(0x00000000, 0xffffffff);
        return (uint32_t)uni(rng);
}

void Table::populate(HashMap *map) {
        uint32_t i;
        for (i = 0; i < record_count; ++i) {
                map->put(i, 0);
        }
}

uint32_t Table::num_records() const {
        return this->record_count;
}

Txn** workload_generator::Gen() {
        uint32_t i;
        Txn **txns = (Txn**)malloc(sizeof(Txn*)*operation_count);
        for (i = 0; i < operation_count; ++i) {
                txns[i] = new ycsb_insert(0, table->num_records(), 1 /*FIXME:for multikey, >1*/);
        }
        return txns;
}
