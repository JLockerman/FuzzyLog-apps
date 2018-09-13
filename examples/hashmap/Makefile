CFLAGS=-O2 -Wall -Wextra -std=c++11 -Wno-sign-compare
CC=g++

FUZZY_LIB=$(FUZZYLOG_SRC_LOC)/target/release $(FUZZYLOG_SRC_LOC)/src/*
LIB_DIRS=./libs/lib/ $(FUZZYLOG_SRC_LOC)/target/release
LIBPATH=$(foreach d, $(LIB_DIRS), -L$d)
LIBS=-pthread -lfuzzy_log -lcityhash
INC_DIRS=include libs/include $(FUZZYLOG_SRC_LOC)
INCLUDE=$(foreach d, $(INC_DIRS), -I$d)

OS_NAME := $(shell uname)

ifeq ($(OS_NAME), Linux)
	LIBS += -lm -ldl -lrt -pthread -Wl,--gc-sections
else ifeq ($(UNAME_S),Darwin)
    LIBS += -lsystem -lc -lm -pthread
endif

all: build/atomicmap build/capmap

build/%.o: src/%.cc
	@mkdir -p build
	$(CC) -c -o $@ $< $(INCLUDE) $(CFLAGS)

ATOMICMAP_OBJECTS = build/atomicmap_workload.o build/tester.o build/atomicmap_tester.o build/atomicmap_synchronizer.o build/map.o build/atomicmap.o build/atomicmap_test.o

build/atomicmap: $(ATOMICMAP_OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

CAPMAP_OBJECTS = build/capmap_workload.o build/tester.o build/capmap_tester.o build/capmap_synchronizer.o build/map.o build/capmap.o build/capmap_test.o

build/capmap: $(CAPMAP_OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

.PHONY: clean

clean:
	rm -rf build
