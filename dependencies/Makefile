###################################################################
# Copyright (C) 2015, International Business Machines Corporation.
# All Rights Reserved.
###########################################################

PACKAGES += aerospike
PACKAGES += cassandra
PACKAGES += couchbase
PACKAGES += curl
PACKAGES += hiredis
PACKAGES += hiredis-cluster
PACKAGES += json-c
PACKAGES += memcached
PACKAGES += mongo-c
#PACKAGES += sds
PACKAGES += uv

PWD := $(shell pwd)

all: ${PACKAGES:%=%.all}

clean: ${PACKAGES:%=%.clean}
	rm -rf $(shell pwd)/../com.ibm.streamsx.dps/impl/ext

%.all : out-of-date
	$(MAKE) -j1 -C $(*F) all

%.clean: out-of-date
	$(MAKE) -j1 -C $(*F) clean

.PHONY: all clean out-of-date
