#pragma once

#include <atomic>
#include <chrono>
#include <vector>
#include <set>

#define MAX_ASYNC_REQUESTS 1024

struct tester_request {
	std::chrono::system_clock::time_point 		_start_time;
	std::chrono::system_clock::time_point 		_end_time;	
	bool 						_executed;
};

class async_tester {
private:
	uint32_t 			_window_sz;
	std::atomic<uint64_t>		_num_elapsed; 
	std::atomic<bool> 		_quit;

protected:	
	virtual void issue_request(tester_request *rq) = 0;
	virtual void wait_requests(std::set<tester_request*> &done_set) = 0; 
	virtual tester_request* wait_single_request() = 0;

	void do_measurement(std::vector<double> &samples,
	  	     	    int interval, 
			    int duration);

	void run(const std::vector<tester_request*> &requests);
	void wait_single();	
	uint32_t try_get_pending();
public:
	async_tester(uint32_t window_sz);
	void do_run(const std::vector<tester_request*> &requests, std::vector<double> &samples, int interval, int duration);
};
