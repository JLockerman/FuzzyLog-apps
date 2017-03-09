CFLAGS=-O2 -Wall -Wextra -std=c++11 -Wno-sign-compare
CC=g++

DELOS_LIB=$(DELOS_RUST_LOC)/examples/c_linking/target/release $(DELOS_RUST_LOC)/src/*
LIB_DIRS=./libs/lib/ $(DELOS_LIB)
LIBPATH=$(foreach d, $(LIB_DIRS), -L$d)
LIBS=-pthread -lc_link -ldl -lrt -lcityhash
INC_DIRS=include libs/include
INCLUDE=$(foreach d, $(INC_DIRS), -I$d)

build/%.o: src/%.cc 
	@mkdir -p build
	@$(CC) -c -o $@ $< $(INCLUDE) $(LIBPATH) $(CFLAGS) $(LIBS)

OBJECTS = build/workload.o build/worker.o build/synchronizer.o build/hashmap.o build/main.o

build/hashmap: $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

.PHONY: clean

clean:
	rm -rf build 
