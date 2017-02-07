#include <or_set.h>
#include <chrono>
#include <worker.h>
#include <workload_generator.h>
#include <config.h>
#include <tuple>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sstream>
#include <iostream>
#include <fstream> 

void write_output(config cfg, const std::vector<uint64_t> &results)
{
	std::stringstream filename;
	std::ofstream result_file;	
	
	filename << cfg.server_id << ".txt";
	
	result_file.open(filename.str(), std::ios::app | std::ios::out);
	for (auto v : results) {
		result_file << v << "\n";
	}
	result_file.close();
}

void gen_input(uint64_t range, uint64_t num_inputs, std::vector<uint64_t> &output)
{
	workload_generator gen(range);
	uint64_t i;
	
	for (i = 0; i < num_inputs; ++i) 
		output.push_back(gen.gen_next());
}

void measure_fn(worker *w, uint64_t duration, std::vector<uint64_t> &results)
{
	uint64_t start_iters, end_iters;
	
	end_iters = w->num_iterations();
	for (auto i = 0; i < duration; ++i) {
		start_iters = end_iters; 
		std::this_thread::sleep_for(std::chrono::seconds(1));	
		end_iters = w->num_iterations();	
		results.push_back(end_iters - start_iters);
	}
}

void worker_fn(config cfg, std::atomic_bool &flag, worker **w,
	       std::condition_variable &cv) 
{
	assert(*w == NULL);
	std::vector<uint64_t> to_insert;
	
	gen_input(cfg.expt_range, cfg.expt_range, to_insert); 
	*w = new worker(cfg);
	cv.notify_one();	
	
	while (flag == true)  	
		(*w)->run_expt(to_insert);	
}

void do_experiment(config cfg, std::vector<uint64_t> &results)
{
	worker *w = NULL;
	std::atomic_bool flag;
	std::mutex m;
	std::condition_variable cv;
	
	flag = true;
	std::thread worker_thread(worker_fn, cfg, std::ref(flag), &w,  
				  std::ref(cv)); 

	std::unique_lock<std::mutex> lk(m);
	cv.wait(lk, [&w] { return w != NULL; }); 	
	lk.unlock();
	
	std::this_thread::sleep_for(std::chrono::seconds(5));
 	measure_fn(w, cfg.expt_duration, results);	
	flag = false;
	worker_thread.join();
	delete(w);
}

int main(int argc, char **argv) 
{
	std::vector<uint64_t> results;
	config_parser cfg_prser;			
	config cfg = cfg_prser.get_config(argc, argv);

	do_experiment(cfg, results);	
	write_output(cfg, results);
	std::cerr << "Done experiment!\n";
	return 0;
}