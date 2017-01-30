#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include "hashmap.h"

extern "C" {
        #include "fuzzy_log.h"
}

void run_YCSB() {
        HashMap *map = new HashMap;
        std::string line;

        std::vector<std::pair<uint32_t, uint32_t> > dataset;

        // 1. Load
        std::cout << "Parsing dataset..." << std::endl;
        std::ifstream infile("transactions_load_fz.txt"); 
        while (std::getline(infile, line)) {
                if (line.find("INSERT FUZZYLOG ") == 0) {
                        std::istringstream iss(line);
                        std::string dummy, key, value; 
                        if (!(iss >> dummy >> dummy >> key)) break;

                        key   = key.substr(14, std::string::npos); 
                        value = line.substr(line.find("field0") + 7, 4); 
                        uint32_t k = std::stoi(key);
                        dataset.push_back(std::make_pair(k, rand()));
                }
        }
        std::cout << "Inserting dataset of size " << dataset.size() << "..." << std::endl;
        for (size_t i=0; i<dataset.size(); ++i) {
                map->put(dataset[i].first, dataset[i].second); 
                std::cout << "Inserted " << i << std::endl;
        }
        std::cout << "Done" << std::endl;
       

        // 2. Run
        std::cout << "Parsing workload..." << std::endl;
        std::ifstream infile_w("transactions_run_fz.txt"); 
        while (std::getline(infile_w, line)) {
                if (line.find("READ FUZZYLOG ") != 0 && line.find("UPDATE FUZZYLOG") != 0) continue;
                std::istringstream iss(line);
                std::string dummy, key; 
                if (!(iss >> dummy >> dummy >> key)) break;

                key   = key.substr(14, std::string::npos); 
                uint32_t k = std::stoi(key);

                if (line.find("READ FUZZYLOG ") == 0) {
                        std::cout << "Read " << key << std::endl;
                        map->get(k);
                } else if (line.find("UPDATE FUZZYLOG ") == 0) {
                        std::cout << "Update " << key << std::endl;
                        map->put(k, rand());
                }
        }
        std::cout << "Done with workload" << std::endl;
       
}

int main() {
        // FIXME: only necessary for local test
	start_fuzzy_log_server_thread("0.0.0.0:9990");
        
        // Parse workload file from YCSB
        run_YCSB();        

//      HashMap map;
//      map.put(10, 15);
//      map.put(10, 16);
//      map.put(10, 12);

//      map.put(100, 5);
//      map.remove(100);
//      map.put(100, 7);

//      
//      uint32_t val10 = map.get(10);
//      std::cout << "Output for key 10 : " << val10 << std::endl;

//      uint32_t val100 = map.get(100);
//      std::cout << "Output for key 100: " << val100 << std::endl;

        return 0;
}
