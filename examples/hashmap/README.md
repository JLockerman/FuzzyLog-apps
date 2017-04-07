# AtomicMap, CAPMap

### Prerequisites
Download delos repository and set `DELOS_RUST_LOC` to its root directory.

Make sure that you compiled the fuzzy log client library as following.
```sh
cd $DELOS_RUST_LOC/examples/c_linking
make clean
make
```
CityHash:
```sh
git clone https://github.com/google/cityhash.git
cd cityhash
./configure --prefix=$DELOS_APPS_LOC/delos-apps/examples/hashmap/libs
make install
make
```

### Installation and run
To build,
```sh
$ make [all]
```

To run AtomicMap
```sh
$ ./build/atomicmap
```

For example, 
```sh
$ ./build/atomicmap --log_addr=127.0.0.1:9990 --expt_range=10000 --expt_duration=120 --client_id=1 --workload=put@15=1000,get@15=1 --async --window_size=32
```
- log_addr: fuzzylog ip:port
- expt_range: integer range [0, value] from which key is chosen
- expt_duration: experiment running time (in seconds)
- client_id: any integer value unique to each client (not used in system, but used for gathering statistics purpose)
- workload: specifying types of workload.
  - workload = operation,...
  - operation={put|get}@color=operation_count
- async: indicate if it's asynchronous operation
- window_size: specify window size for windowing in asynchronous operation

To run CAPMap,
```sh
$ ./build/capmap
```
- workload: specifying types of workload.
  - workload = operation,...
  - operation={put}@local_color-remote_color=operation_count
- protocol
  - 1: old
  - 2: new
- role: only used with protocol 2. `primary` or `secondary`
