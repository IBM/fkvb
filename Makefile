.PHONY: all

# Build configuration parameters.
# BUILD_TYPE options: DEBUG, NORMAL, PERFORMANCE
BUILD_TYPE           ?= PERFORMANCE

BUILD_FDB	 	 = 1
BUILD_FDB_API		 = 620


SHELL = /bin/bash

INCLUDES    = -Isrc/include/

CXX        ?= g++
CXXFLAGS   += -Wall -Werror -std=c++11 $(INCLUDES)


#-Og -ggdb -g3 -fno-omit-frame-pointer
ifeq (DEBUG,$(BUILD_TYPE))
CXXFLAGS   += -O0 -ggdb -fno-omit-frame-pointer
else ifeq (NORMAL,$(BUILD_TYPE))
CXXFLAGS   += -O2 -ggdb
else ifeq (PERFORMANCE,$(BUILD_TYPE))
CXXFLAGS   += -O3 -DNDEBUG #-flto
else
$(error "Unknown BUILD_TYPE: --->$(BUILD_TYPE)<---")
endif

#CXXFLAGS  += -fconcepts
LIBS        = -lpthread -lrt


CXXFLAGS   +=-DBUILD_DUMMY
LIBFDB = lib/fdb/620/libfdb_c.so
LIBS += $(LIBFDB)

CXXFLAGS   += -DBUILD_FDB -lm -lpthread -lrt -DFDB_API=$(BUILD_FDB_API)
INCLUDES   += -Isrc/fkvb/fdb/include

LDFLAGS    += -Wl,--build-id

FKVB = src/fkvb/fkvb

LIBFDBD="lib/fdb/620"
LIBFDB=lib/fdb/620/libfdb_c.so
all: $(FKVB)

.deps/%.d: %.cc
	@mkdir -p $(dir $@)
	@echo DEPS: $<
	@set -e; $(CXX) $(CXXFLAGS) -MM -MP $< -MT $(patsubst %.cc, %.o, $<) $@ > $@ 2>/dev/null


fkvb_SRC = src/fkvb/kv-conf.cc  src/fkvb/fkvb_test_conf.cc src/fkvb/fkvbfactory.cc  src/fkvb/DummyKVOrdered.cc src/fkvb/FKVB.cc
fkvb_main_SRC = src/fkvb/fkvb_main.cc

fkvb_SRC       += src/fkvb/KVOrderedFDB.cc

fkvb_OBJ = $(patsubst %.cc, %.o, ${fkvb_SRC})
fkvb_main_OBJ = $(patsubst %.cc, %.o, ${fkvb_main_SRC})

download:
	mkdir -p $(LIBFDBD)
	wget -O $(LIBFDB) https://www.foundationdb.org/downloads/6.2.18/linux/libfdb_c_6.2.18.so

#Need -ldl
src/fkvb/fkvb: $(fkvb_main_OBJ) $(fkvb_OBJ) $(fkvb_main_SRC) $(fkvb_SRC) Makefile
ifeq (,$(wildcard $(LIBFDB)))
	make download
endif
	$(CXX) $(LDFLAGS)  $(fkvb_OBJ) $(fkvb_main_OBJ) $(LIBS) -o $@ -ldl
	mkdir -p bin
	mv src/fkvb/fkvb bin/fkvb


#clear everything, not only what you have compiled
#fixme: also clear ALL backends?
clean:
	rm  -f src/fkvb/*.o
	rm  -f $(fkvb_OBJ)
	rm  -f $(fkvb_main_OBJ)
	rm  -f test/fkvb/fkvb
