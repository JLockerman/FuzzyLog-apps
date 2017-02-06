#ifndef 	OR_SET_H_
#define		OR_SET_H_

#include <set>
#include <cstdint>
#include <unordered_map>

extern "C" {
	#include <fuzzy_log.h>
}

class or_set {
private:
	struct colors 						*_color;
	DAGHandle 						*_log_client;
	std::unordered_map<uint64_t, std::set<uint64_t> > 	_state;
 	uint64_t 						_guid_counter;
	uint8_t							_proc_id;	

	uint64_t gen_guid();
	void send_add(uint64_t e, uint64_t guid);
	void send_remove(uint64_t e, const std::set<uint64_t> &guid_set);
	
	void serialize_add(uint64_t e, uint64_t guid, char **buf, size_t *sz);
	void serialize_remove(uint64_t e, const std::set<uint64_t> &guid_set, char **buf, size_t *sz);
	void deserialize_add(char *buf, size_t sz, uint64_t *e, uint64_t *guid);
	void deserialize_remove(char *buf, size_t *sz, uint64_t *e, std::set<uint64_t> *guids);
public:
  	or_set(DAGHandle *handle, struct colors *color, uint8_t proc_id);
	bool lookup(uint64_t e);
	void add(uint64_t e);
	void remove(uint64_t e);
};

#endif 		// OR_SET_H_
