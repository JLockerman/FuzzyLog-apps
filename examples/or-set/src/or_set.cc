#include <or_set.h>
#include <cassert>
#include <iostream>

or_set::or_set(DAGHandle *handle, struct colors *color, uint8_t proc_id)
{
	_log_client = handle;
	_color = color;
	_proc_id = proc_id;
	_guid_counter = 0;
	_state.clear();
}

uint64_t or_set::gen_guid() 
{
	uint64_t guid;
	guid = _guid_counter;
	_guid_counter += 1;
	return (guid << 8 | _proc_id);
}

void or_set::serialize_add(uint64_t e, uint64_t guid, char **buf, size_t *sz)
{
	char *rbuf;
	rbuf = (char*)malloc(sizeof(uint64_t)*3);
		
	rbuf[0] = 0;
	rbuf[1] = e;
	rbuf[2] = guid;
	
	*buf = rbuf;
	*sz = sizeof(uint64_t)*3;
	return;		
}

void or_set::send_add(uint64_t e, uint64_t guid)
{
	char *buf;
	size_t sz;
	
	serialize_add(e, guid, &buf, &sz);
	append(_log_client, buf, sz, _color, NULL);
}

void or_set::serialize_remove(uint64_t e, const std::set<uint64_t> &guid_set, char **buf, size_t *sz)
{
	assert(guid_set.size() > 0);

	uint64_t *rbuf;
	uint64_t    i;
	
	rbuf = (uint64_t*)malloc(sizeof(uint64_t)*2 + sizeof(uint64_t)*guid_set.size());
	rbuf[0] = 1;
	rbuf[1] = e;

	i = 2;
	for (uint64_t guid : guid_set) {
		rbuf[i] = guid;
		i += 1;
	}	

	*buf = (char*)rbuf;
	*sz = sizeof(uint64_t)*(2 + guid_set.size());
}

void or_set::send_remove(uint64_t e, const std::set<uint64_t> &guid_set)
{
	char *buf;
	size_t sz;
	
	serialize_remove(e, guid_set, &buf, &sz);
	append(_log_client, buf, sz, _color, NULL);
}

/* XXX Not yet implemented. */
void or_set::deserialize_remove(__attribute__((unused)) char *buf, 	
				__attribute__((unused)) size_t *sz, 
				__attribute__((unused)) uint64_t *e, 	
				__attribute__((unused)) std::set<uint64_t> *guids)
{
	assert(false);
}

/* XXX Not yet implemented. */
void or_set::deserialize_add(__attribute__((unused)) char *buf, 
			     __attribute__((unused)) size_t sz, 
			     __attribute__((unused)) uint64_t *e, 
			     __attribute__((unused)) uint64_t *guid)
{
	assert(false);	
}

bool or_set::lookup(uint64_t e)
{
	return _state.count(e) > 0;
}

void or_set::add(uint64_t e)
{
	uint64_t guid = gen_guid();
	_state[e].insert(guid);
	
	send_add(e, guid);	
}

void or_set::remove(uint64_t e)
{
	std::set<uint64_t> guid_set;
	
	guid_set = _state[e];
	send_remove(e, guid_set);
	_state.erase(e);
}
