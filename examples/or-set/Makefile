CFLAGS=-O3 -g -Wall -Wextra -std=c++11 -Wno-sign-compare
CC=g++

FUZZY_LIB=$(FUZZYLOG_SRC_LOC)/target/release
FUZZY_INC=$(FUZZYLOG_SRC_LOC)
LIB_DIRS=./libs/lib/ $(FUZZY_LIB)
LIBPATH=$(foreach d, $(LIB_DIRS), -L$d)
LIBS=-pthread -lc_link -ldl -lrt
INC_DIRS=include $(FUZZY_APPS_LOC)/common $(FUZZY_INC)
INCLUDE=$(foreach d, $(INC_DIRS), -I$d)
SRC=src
SOURCES:=$(wildcard $(SRC)/*.cc $(SRC)/*.c)
OBJECTS=$(patsubst $(SRC)/%.cc,build/%.o,$(SOURCES))

DEPSDIR:=.deps
DEPCFLAGS=-MD -MF $(DEPSDIR)/$*.d -MP

all:build/or-set

-include $(wildcard $(DEPSDIR)/*.d)

build/or-set:$(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDE) $^ -o $@ $(LIBPATH) $(LIBS)

build/%.o: src/%.cc $(DEPSDIR)/stamp Makefile
	@mkdir -p build
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) $(INCLUDE) -c -o $@ $<

$(DEPSDIR)/stamp:
	@mkdir -p $(DEPSDIR)
	@touch $@

.PHONY: clean

clean:
	rm -rf build $(DEPSDIR)
