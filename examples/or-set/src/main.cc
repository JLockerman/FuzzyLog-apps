#include <or_set.h>
#include <chrono>
#include <worker.h>
#include <workload_generator.h>
#include <config.h>
#include <tuple>
#include <thread>
#include <mutex>
#include <condition_variable>

void gen_input(uint64_t range, uint64_t num_inputs, std::vector<uint64_t> &output)
{
	workload_generator gen(range);
	uint64_t i;
	
	for (i = 0; i < num_inputs; ++i) 
		output.push_back(gen.gen_next());
}

uint64_t measure_fn(worker *w, uint64_t duration)
{
	uint64_t start_iters, end_iters;
	
	start_iters = w->num_iterations();
	std::this_thread::sleep_for(std::chrono::seconds(duration));
	end_iters = w->num_iterations();	
	
	return end_iters - start_iters;
}

void worker_fn(config cfg, std::atomic_bool &flag, worker **w,
	       std::condition_variable &cv) 
{
	assert(*w == NULL);
	std::vector<uint64_t> to_insert;
	
	gen_input(cfg.expt_range, cfg.expt_range, to_insert); 
	*w = new worker(cfg.log_addr, cfg.server_id);
	cv.notify_one();	
	
	while (flag == true)  	
		(*w)->run_expt(to_insert);	
}

uint64_t do_experiment(config cfg)
{
	uint64_t iterations;
	worker *w = NULL;
	std::atomic_bool flag;
	std::mutex m;
	std::condition_variable cv;
	
	flag = true;
	std::thread worker_thread(worker_fn, cfg, std::ref(flag), &w,  
				  std::ref(cv)); 

	std::unique_lock<std::mutex> lk(m);
	cv.wait(lk, [w] { return w != NULL; }); 	
	lk.unlock();
	
	std::this_thread::sleep_for(std::chrono::seconds(5));
 	iterations = measure_fn(w, cfg.expt_duration);	
	flag = false;
	worker_thread.join();
	delete(w);
	
	return iterations;
}

int main(int argc, char **argv) 
{
	config_parser cfg_prser;			
	uint64_t iterations;
	config cfg = cfg_prser.get_config(argc, argv);

	iterations = do_experiment(cfg);	
	std::cerr << "Done experiment!\n";
	std::cerr << "Throughput: " << ((double)iterations) / ((double)cfg.expt_duration) << "\n";

	return 0;
}

