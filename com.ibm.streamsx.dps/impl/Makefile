
# In order for this to work, you must ensure that these two Linux environment
# variables are set correctly:  $STREAMS_INSTALL and $JAVA_HOME
#
# It will also build the main DPS library that contains the actual DPS logic to
# perform CRUD operations into 10 or so back-end K/V stores.

ARCH = $(shell uname -i)

LIB = lib

DISTRIBUTED_PROCESS_STORE_OBJS = 
DISTRIBUTED_PROCESS_STORE_OBJS += DistributedProcessStore.o
DISTRIBUTED_PROCESS_STORE_OBJS += com_ibm_streamsx_dps_impl_DpsHelper.o
MEMCACHED_OBJ = MemcachedDBLayer.o
REDIS_OBJ = RedisDBLayer.o
CASSANDRA_OBJ = CassandraDBLayer.o
CLOUDANT_OBJ = CloudantDBLayer.o
HBASE_OBJ = HBaseDBLayer.o
MONGO_OBJ = MongoDBLayer.o
AERO_OBJ =
AERO_LIB =
COUCH_OBJ =  CouchbaseDBLayer.o
REDIS_CLUSTER_OBJ = RedisClusterDBLayer.o
      
          
REDIS_LIB = $(LIB)/libDPSRedis.so
REDIS_CLUSTER_LIB = $(LIB)/libDPSRedisCluster.so
COUCH_LIB = $(LIB)/libDPSCouchbase.so
HBASE_LIB = $(LIB)/libDPSHBase.so
CASSANDRA_LIB = $(LIB)/libDPSCassandra.so
CLOUDANT_LIB = $(LIB)/libDPSCloudant.so
MONGO_LIB = $(LIB)/libDPSMongo.so
MEMCACHED_LIB = $(LIB)/libDPSMemcached.so

HELPER_LIBS = $(REDIS_CLUSTER_LIB)
HELPER_LIBS += $(COUCH_LIB)
HELPER_LIBS += $(REDIS_LIB)
HELPER_LIBS += $(HBASE_LIB)
HELPER_LIBS += $(CASSANDRA_LIB)
HELPER_LIBS += $(CLOUDANT_LIB)
HELPER_LIBS += $(MONGO_LIB)
HELPER_LIBS += $(MEMCACHED_LIB)

ifeq ($(ARCH),x86_64)
AERO_OBJ = AerospikeDBLayer.o
AERO_LIB = $(LIB)/libDPSAerospike.so
HELPER_LIBS += $(AERO_LIB)
endif

DISTRIBUTED_PROCESS_STORE_LIB = $(LIB)/libDistributedProcessStoreLib.so
DISTRIBUTED_PROCESS_STORE_LIB_STATIC = $(LIB)/libDistributedProcessStoreLibStatic.a

SPL_PKGCFG = $(STREAMS_INSTALL)/bin/dst-pe-pkg-config.sh
SPL_PKG = dst-spl-pe-install
SPL_COMPILE_OPTIONS = $(shell $(SPL_PKGCFG) --cflags $(SPL_PKG))
SPL_LINK_OPTIONS = $(shell $(SPL_PKGCFG) --libs $(SPL_PKG))

CPPFLAGS = 
CPPFLAGS += -O3 -Wall -c -fmessage-length=0 -fPIC -D_REENTRANT
CPPFLAGS += -I include
CPPFLAGS += -I nl/include
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

all: check-streams $(DISTRIBUTED_PROCESS_STORE_LIB_STATIC) $(DISTRIBUTED_PROCESS_STORE_LIB) $(HELPER_LIBS)

check-streams:
	@echo "Checking to see if STREAMS_INSTALL is set..."
	test -d $(STREAMS_INSTALL) || false
 
$(DISTRIBUTED_PROCESS_STORE_LIB): $(DISTRIBUTED_PROCESS_STORE_OBJS)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^

$(DISTRIBUTED_PROCESS_STORE_LIB_STATIC): $(DISTRIBUTED_PROCESS_STORE_OBJS)
	mkdir -p $(LIB)
	ar rcs $@ $^

$(AERO_LIB): $(AERO_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^

$(COUCH_LIB): $(COUCH_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^
	
$(REDIS_LIB): $(REDIS_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^
	
$(REDIS_CLUSTER_LIB): $(REDIS_CLUSTER_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^
		
$(HBASE_LIB): $(HBASE_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^

$(CASSANDRA_LIB): $(CASSANDRA_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^
	
$(CLOUDANT_LIB): $(CLOUDANT_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^
	
$(MONGO_LIB): $(MONGO_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^

$(MEMCACHED_LIB): $(MEMCACHED_OBJ)
	mkdir -p $(LIB)
	$(CXX) $(LDFLAGS) -o $@ $^
	
clean:
	rm -f $(DISTRIBUTED_PROCESS_STORE_OBJS) 
	rm -f $(DISTRIBUTED_PROCESS_STORE_LIB) 
	rm -f $(REDIS_LIB) $(REDIS_OBJ)
	rm -f $(REDIS_CLUSTER_LIB) $(REDIS_CLUSTER_OBJ)
	rm -f $(MEMCACHED_LIB) $(MEMCACHED_OBJ)
	rm -f $(CLOUDANT_LIB) $(CLOUDANT_OBJ)
	rm -f $(CASSANDRA_LIB) $(CASSANDRA_OBJ)
	rm -f $(HBASE_LIB) $(HBASE_OBJ)
	rm -f $(MONGO_LIB) $(MONGO_OBJ)
	rm -f $(AERO_LIB) $(AERO_OBJ)
	rm -f $(COUCH_LIB) $(COUCH_OBJ)
	rm -rf $(LIB)

