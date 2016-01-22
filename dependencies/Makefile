###################################################################
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
###########################################################


#This Makefile builds the DPS toolkit.
#Pre-built versions of the dependencies (curl, redis, e.t.c) have been included in the dependencies folder for the following supported platforms:
# Linux x86_64: 
# RedHat Enterprise Linux 6.4 (or an equivalent CentOS version)
# RedHat Enterprise Linux 7.1 (or an equivalent CentOS version)
# SUSE Linux Enterprise Server (SLES)) 11
#Linux for IBM Power 8 Little Endian: 
# RedHat Enterprise Linux 7.1 )
#Linux for IBM Power 7 Big Endian: 
# RedHat Enterprise Linux 6.4 (or an equivalent CentOS version)
# To build the toolkit for any of the above platforms, run "make".  


#PACKAGES += aerospike
PACKAGES += cassandra
#PACKAGES += couchbase
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
