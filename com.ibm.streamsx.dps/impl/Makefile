
# In order for this to work, you must ensure that these two Linux environment
# variables are set correctly:  $STREAMS_INSTALL and $JAVA_HOME
#
# This script will attempt to build the DPS Java Lib loader library which will
# contain simply one JNI function to load many of the .so libraries inside the
# DpsHelper Java file (This is needed for using DPS inside Java operators)
#
# It will also build the main DPS library that contains the actual DPS logic to
# perform CRUD operations into 10 or so back-end K/V stores.

ARCH = $(shell uname -i)

LIB = lib

DISTRIBUTED_PROCESS_STORE_OBJS = 
DISTRIBUTED_PROCESS_STORE_OBJS += DistributedProcessStore.o
DISTRIBUTED_PROCESS_STORE_OBJS += com_ibm_streamsx_dps_impl_DpsHelper.o
DISTRIBUTED_PROCESS_STORE_OBJS += MemcachedDBLayer.o
DISTRIBUTED_PROCESS_STORE_OBJS += RedisDBLayer.o
DISTRIBUTED_PROCESS_STORE_OBJS += CassandraDBLayer.o
DISTRIBUTED_PROCESS_STORE_OBJS += CloudantDBLayer.o
DISTRIBUTED_PROCESS_STORE_OBJS += HBaseDBLayer.o
DISTRIBUTED_PROCESS_STORE_OBJS += MongoDBLayer.o
DISTRIBUTED_PROCESS_STORE_OBJS += CouchbaseDBLayer.o
# Aerospike not supported on ppc64/ppc64le
ifeq (,$(findstring ppc64,$(ARCH)))
DISTRIBUTED_PROCESS_STORE_OBJS += AerospikeDBLayer.o
endif
DISTRIBUTED_PROCESS_STORE_OBJS += RedisClusterDBLayer.o

DISTRIBUTED_PROCESS_STORE_LIB = $(LIB)/libDistributedProcessStoreLib.so

DPS_JAVA_LIB_LOADER_OBJS = 
DPS_JAVA_LIB_LOADER_OBJS += com_ibm_streamsx_dps_JavaLibLoader.o

DPS_JAVA_LIB_LOADER_LIB = $(LIB)/libDpsJavaLibLoader.so

SPL_PKGCFG = $(STREAMS_INSTALL)/bin/dst-pe-pkg-config.sh
SPL_PKG = dst-spl-pe-install
SPL_COMPILE_OPTIONS = $(shell $(SPL_PKGCFG) --cflags $(SPL_PKG))
SPL_LINK_OPTIONS = $(shell $(SPL_PKGCFG) --libs $(SPL_PKG))

CPPFLAGS = 
CPPFLAGS += -O3 -Wall -c -fmessage-length=0 -fPIC -D_REENTRANT
CPPFLAGS += -I include
CPPFLAGS += -isystem ext/include
CPPFLAGS += -isystem ext/include/libbson-1.0
CPPFLAGS += -isystem ext/include/libmongoc-1.0
CPPFLAGS += -isystem $(JAVA_HOME)/include
CPPFLAGS += $(SPL_COMPILE_OPTIONS)

LDFLAGS =
LDFLAGS += -shared
LDFLAGS += -Wl,-rpath,'$$ORIGIN'
LDFLAGS += -Wl,-rpath,'$$ORIGIN/../ext/lib'
LDFLAGS += $(SPL_LINK_OPTIONS)
#LDFLAGS += -Wl,-z,defs

#ARCH = $(shell uname -i)
#ifeq ($(ARCH),ppc64)
#CXX = /opt/at8.0/bin/g++
#endif
#ifeq ($(ARCH),ppc64le)
#CXX = /opt/at8.0/bin/g++
#endif

vpath %.cpp src

export CPLUS_INCLUDE_PATH=./include:./ext/include:$JAVA_HOME/include:$JAVA_HOME/include/linux

all: check-streams $(DISTRIBUTED_PROCESS_STORE_LIB) $(DPS_JAVA_LIB_LOADER_LIB)

check-streams:
	@echo "Checking to see if STREAMS_INSTALL is set..."
	test -d $(STREAMS_INSTALL) || false

$(DISTRIBUTED_PROCESS_STORE_LIB): $(DISTRIBUTED_PROCESS_STORE_OBJS)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^

$(DPS_JAVA_LIB_LOADER_LIB): $(DPS_JAVA_LIB_LOADER_OBJS)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^

clean:
	rm -f $(DISTRIBUTED_PROCESS_STORE_OBJS) $(DPS_JAVA_LIB_LOADER_OBJS)
	rm -f $(DISTRIBUTED_PROCESS_STORE_LIB) $(DPS_JAVA_LIB_LOADER_LIB)

