# AtomicMap, CAPMap

### Prerequisites
Download delos repository and set `DELOS_RUST_LOC` to its root directory.

Make sure that you compiled the fuzzy log client library as following.
```sh
cd $DELOS_RUST_LOC/examples/c_linking
make clean
make
```

### Installation and run
To build,
```sh
$ make [all]
```

To run the OR-set app: 
```sh
$ ./build/or-set
```

There are two types of clients, a writer and a reader.  

To run a writer:

```sh
$ ./build/or-set --writer --head_log_addr 127.0.0.1:9990 --tail_log_addr 127.0.0.1:9991 --expt_duration 120 --expt_range 1000000 --server_id 2 --num_clients 2 --sync_duration 500 --num_rqs 3000000 --window_sz 64 --sample_interval 1 --low-throughput 10000 --high_throughput 50000 --spike_start 40 --spike_duration 20   
```

- writer: indicates that this client is a writer.
- head_log_addr: comma separated list of fuzzylog ip:port pairs corresponding to the heads of replication chain.
- tail_log_addr: comma separated list of fuzzylog ip:port pairs corresponding to the tails of each replication chain.
- exp_range: integer range [0, value] from which key is chosen
- num_rqs: number of pre-allocated request (we pre-allocate requests so that we have a window of pending requests)
- sample_interval: the time period between which number of executed requests is sampled. 
- sync_duration: the time between which an anti-entropy protocol is executed to bring writers in sync. For experiments in the paper, this is set to a number larger than experiment duration because writes are merged at a reader. 
- server_id: the identifier of this particular server. Expected to be in the range [0, num_clients-1]
- num_clients: total number of writers in this experiment. 
- window_sz: the size of the window of outstanding requests for asynchronous puts. 
- exp_duration: length of the experiment in seconds. 
- low_throughput: a comfortable rate at which requests are sent to the fuzzylog.  
- high_throughput: throughput during peak load.
- spike_start: the time, in seconds, at which a writer will start appending at high_throughput 
- spike_duration: the duration for which requests are appended at high_throughput

To run a reader: 
```sh
$ ./build/or-set --reader --head_log_addr 127.0.0.1:9990 --tail_log_addr 127.0.0.1:9991 --expt_duration 120 --expt_range 1000000 --server_id 2 --num_clients 2 --sync_duration 500 --num_rqs 3000000 --window_sz 64 --sample_interval 1 --low-throughput 10000 --high_throughput 50000 --spike_start 40 --spike_duration 20   
```

- reader: indicates that this client is a reader.
[The remaining arguments apart from writer are the same as for a writer. Set server_id equal to num_clients. ]
