CFLAGS=-O2 -Wall -Wextra -std=c++11 -Wno-sign-compare
CC=g++

DELOS_LIB=$(DELOS_RUST_LOC)/examples/c_linking/target/release $(DELOS_RUST_LOC)/src/*
LIB_DIRS=./libs/lib/ $(DELOS_LIB)
LIBPATH=$(foreach d, $(LIB_DIRS), -L$d)
LIBS=-pthread -lc_link -ldl -lrt -lcityhash
INC_DIRS=include libs/include $(DELOS_RUST_LOC)/examples/c_linking
INCLUDE=$(foreach d, $(INC_DIRS), -I$d)

all: build/atomicmap build/capmap build/txmap

build/%.o: src/%.cc 
	@mkdir -p build
	@$(CC) -c -o $@ $< $(INCLUDE) $(LIBPATH) $(CFLAGS) $(LIBS)

ATOMICMAP_OBJECTS = build/atomicmap_workload.o build/tester.o build/atomicmap_tester.o build/atomicmap_synchronizer.o build/map.o build/atomicmap.o build/atomicmap_test.o

build/atomicmap: $(ATOMICMAP_OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

CAPMAP_OBJECTS = build/capmap_workload.o build/tester.o build/capmap_tester.o build/capmap_synchronizer.o build/map.o build/capmap.o build/capmap_test.o

build/capmap: $(CAPMAP_OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

TXMAP_OBJECTS = build/txmap_workload.o build/tester.o build/txmap_tester.o build/txmap_synchronizer.o build/map.o build/txmap.o build/txmap_test.o

build/txmap: $(TXMAP_OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

.PHONY: clean

clean:
	rm -rf build 
