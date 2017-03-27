#include <tester.h>
#include <fuzzy_log.h>
#include <thread>
#include <iostream>
#include <cassert>

async_tester::async_tester(uint32_t window_sz)
{
	assert(window_sz < MAX_ASYNC_REQUESTS);
	_window_sz = window_sz;
	_num_elapsed = 0;
	_quit = true;
}

uint32_t async_tester::try_get_pending() 
{
	std::set<tester_request*> done_rqs;
	
	done_rqs.clear();
	wait_requests(done_rqs);
	
	auto end_time = std::chrono::system_clock::now();
	for (auto rq : done_rqs) 
		rq->_end_time = end_time;
	return done_rqs.size();
}

void async_tester::use_idle_cycles()
{
}

void async_tester::wait_single()
{
	auto rq = wait_single_request();
	assert(rq->_executed == false);
	rq->_executed = true;
	rq->_end_time = std::chrono::system_clock::now();
}

void async_tester::do_measurement(std::vector<double> &samples,
				  int interval, 
				  int duration)
{
	uint64_t prev, cur;
	prev = _num_elapsed;
	for (auto i = 0; i < duration; i += interval) {
		std::this_thread::sleep_for(std::chrono::seconds(interval));	
		cur = _num_elapsed;
		samples.push_back((cur - prev) / (double)interval);
		prev = cur;
	}	
}

/* Do a run */
void async_tester::do_run(const std::vector<tester_request*> &requests, std::vector<double> &samples, 
			  int interval, int duration)
{
	assert(_quit == true);
	_quit = false;
	
	/* Fork off worker thread */	
	std::thread worker(&async_tester::run, this, requests);	
	
	do_measurement(samples, interval, duration); 
	_quit = true;
	
	/* Join with worker thread */	
	worker.join();
}

void async_tester::do_run_fix_throughput(const std::vector<tester_request*> &requests, std::vector<double> &samples,
					 int interval, 	
					 int duration, 		
					 double low_throughput, 
					 double high_throughput, 
					 double spike_start, 
					 double spike_duration)
{
	assert(_quit == true);
	_quit = false;
	
	std::thread worker(&async_tester::run_throughput, this, requests, low_throughput, high_throughput, spike_start, spike_duration);
	do_measurement(samples, interval, duration);
	_quit = true;
	worker.join();
}

void async_tester::run_throughput(const std::vector<tester_request*> &requests, 
				  double low_throughput, double high_throughput, double spike_start, double spike_duration)
{
	auto low_diff = 1000.0 / low_throughput;
	auto high_diff = 1000.0 / high_throughput;		
	
	auto i = 0;
	auto start = std::chrono::system_clock::now(); 
	auto exp_start = start;
	auto phase = 0, num_pending = 0;
	
	while (_quit == false) {
		auto rq = requests[i];
		assert(rq->_executed == false);
		
		while (true) {
			auto now = std::chrono::system_clock::now();
			std::chrono::duration<double, std::milli> elapsed = now - start;
			double diff;
		
			if (phase == 0 || phase == 2) 
				diff = low_diff;
			else if (phase == 1) 
				diff = high_diff;			
			else
				assert(false);

			if (elapsed.count() > diff) {
				rq->_start_time = now;
				issue_request(rq);
				num_pending += 1;	
				start = now;
				std::chrono::duration<double, std::milli> elapsed_start = now - exp_start;
	
				if (phase == 0 && elapsed_start.count() >= spike_start*1000.0)
					phase = 1; 				
				else if (phase == 1 && elapsed_start.count() >= (spike_start + spike_duration)*1000.0)
					phase  = 2;

				break;
			} else {
				auto diff = try_get_pending();
				num_pending -= diff;
				_num_elapsed += diff;
			}
		}	
		
		i += 1;
	}
	
	assert(i < requests.size()); 
	while (num_pending != 0) {
		use_idle_cycles();
		auto diff = try_get_pending();
		num_pending -= diff;
		_num_elapsed += diff;
	}

}

void async_tester::run(const std::vector<tester_request*> &requests) 
{
	auto num_pending = 0;
	std::set<tester_request*> done_rqs;
	
	for (auto rq : requests) {
		assert(rq->_executed == false);
		if (_quit == true)
			break;
		
		while (num_pending == _window_sz) {
			use_idle_cycles();
			auto diff = try_get_pending();
			num_pending -= diff;
			_num_elapsed += diff;
		}	
		
		/*
		if (num_pending == _window_sz) {
			wait_single();	
			num_pending -= 1;
			_num_elapsed++;
		}
		*/
			
		rq->_start_time = std::chrono::system_clock::now();
		issue_request(rq);	
		num_pending += 1;
	}	
	
	/* Assume that #requests exceeds what we actually execute */
	assert(_quit == true);	

	while (num_pending != 0) {
		use_idle_cycles();
		auto diff = try_get_pending();
		num_pending -= diff;
		_num_elapsed += diff;
	}
}
