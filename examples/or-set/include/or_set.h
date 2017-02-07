#pragma once 

#include <set>
#include <cstdint>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <mutex>

extern "C" {
	#include <fuzzy_log.h>
}

class or_set {
private:
	
	/* Opcodes for fuzzy log entries */
	typedef enum {
		ADD = 0,
		REMOVE = 1,
	} log_opcode;
	
	struct colors 						*_color;
	DAGHandle 						*_log_client;
	std::unordered_map<uint64_t, std::set<uint64_t> > 	_state;
 	uint64_t 						_guid_counter;
	uint8_t							_proc_id;	
	char 							_buf[DELOS_MAX_DATA_SIZE];
	std::chrono::microseconds				_sync_duration; 
	std::mutex						_instance_mutex; 
	std::thread 						_sync_thread;

	/* Opcode logic for fuzzy log entries */
	uint64_t make_opcode(log_opcode opcode);	
	log_opcode get_opcode(uint64_t code);
	uint8_t get_proc_id(uint64_t code);	
	
	/* Serialization logic for communication via the fuzzy log. */
	void serialize_add(uint64_t e, uint64_t guid, char **buf, size_t *sz);
	void serialize_remove(uint64_t e, const std::set<uint64_t> &guid_set, char **buf, size_t *sz);
	void deserialize_add(char *buf, size_t sz, uint64_t *e, uint64_t *guid);
	void deserialize_remove(char *buf, size_t sz, uint64_t *e, std::set<uint64_t> *guids);
	
	/* Guid generator for add operations. */
	uint64_t gen_guid();
	
	/* Send local add/remove info to the fuzzy log. */
	void send_add(uint64_t e, uint64_t guid);
	void send_remove(uint64_t e, const std::set<uint64_t> &guid_set);
	
	/* Receive and process remote add/remove info from the fuzzy log. */	
	void remote_remove(char *buf, size_t sz);
	void remote_add(char *buf, size_t sz);

	/* Add and remove elements from local copy of the or-set */
	void do_add(uint64_t e, uint64_t guid);
	void do_remove(uint64_t e, const std::set<uint64_t> &guid_set);

	/* Control functions for receiving remote add/removes. */
	void get_remote_updates();
	void check_remotes();

public:
	
	/* 
	 * handle is created by the caller. 
	 * 
	 * color is the set colors the local node is interested in.
	 *
	 * proc_id is a unique identifier associated with each or-set instance. 
	 *
	 * sync_duration is the interval between consecutive calls to synchronize with other
	 * instances' updates. 
	 *
	 */ 
  	or_set(DAGHandle *handle, struct colors *color, uint8_t proc_id, uint64_t sync_duration);
	
	/* Returns true if e exists in the local set */
	bool lookup(uint64_t e);

	/* Add e to the or-set */
	void add(uint64_t e);
	
	/* Remove e from the or-set */
	void remove(uint64_t e);
};
