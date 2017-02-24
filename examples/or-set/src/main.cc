#include <or_set.h>
#include <or_set_tester.h>
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
	std::ofstream result_file;	
	result_file.open(std::to_string(cfg.server_id) + ".txt" , std::ios::app | std::ios::out);
	for (auto v : results) {
		result_file << v << "\n";
	}
	result_file.close();
}

void gen_input(uint64_t range, uint64_t num_inputs, std::vector<tester_request*> &output)
{
	workload_generator gen(range);
	uint64_t i;
	
	for (i = 0; i < num_inputs; ++i) {
		auto rq = static_cast<or_set_rq*>(malloc(sizeof(or_set_rq))); 
		rq->_key = gen.gen_next();
		rq->_opcode = or_set::log_opcode::ADD;
		
		auto temp = reinterpret_cast<tester_request*>(rq);
		output.push_back(temp);
	} 
}

/*
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
*/

void do_experiment(config cfg)
{
	struct colors c;
	c.numcolors = 1;
	c.mycolors = new ColorID[0];
	c.mycolors[0] = cfg.server_id;
	auto handle = new_dag_handle_for_single_server(cfg.log_addr.c_str(), &c);
	auto orset = new or_set(handle, &c, cfg.server_id, cfg.sync_duration);	

	auto tester = new or_set_tester(cfg.window_sz, orset, handle);
	std::vector<tester_request*> inputs;	
	
	gen_input(cfg.expt_range, cfg.num_rqs, inputs); 
	tester->run(inputs);		
}

int main(int argc, char **argv) 
{
	std::vector<uint64_t> results;
	config_parser cfg_prser;			
	config cfg = cfg_prser.get_config(argc, argv);

	do_experiment(cfg);	
	return 0;
}
