###################################################################
# Copyright (C) 2015, International Business Machines Corporation.
# All Rights Reserved.
###########################################################

##
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

PWD = $(shell pwd)

SUBDIRS += dependencies
SUBDIRS += com.ibm.streamsx.dps/impl

TARFILE = com.ibm.streamsx.dps-install.tar.gz
BINTARFILE = com.ibm.streamsx.dps-$(ARCH)-$(OS).tar.gz

ARCH = $(shell dependencies/platform-info.pl --arch)
OS = $(shell dependencies/platform-info.pl  --osname_rpm_format)
ATVERSION = $(shell dependencies/platform-info.pl --atver)
SPL_MAKE_TOOLKIT:=$(STREAMS_INSTALL)/bin/spl-make-toolkit

TOOLKIT_DIR = com.ibm.streamsx.dps

DOC_DIR = com.ibm.streamsx.dps/doc

PUBLISH_ROOT ?= $(HOME)/publish
PUBLISH_DIR ?= $(PUBLISH_ROOT)/$(ARCH)/$(OS)

all: check-streams gen-message check-compiler ${SUBDIRS:%=%.all}
	ant -f $(TOOLKIT_DIR)/impl/build.xml -Ddoc.dir=$(PWD)/$(DOC_DIR) all
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -i $(TOOLKIT_DIR) -m
	$(STREAMS_INSTALL)/bin/spl-make-doc --output-directory $(DOC_DIR)/spldoc -i $(TOOLKIT_DIR) --doc-title "Streams DPS Toolkit"

clean: check-streams ${SUBDIRS:%=%.clean} install-clean binpackage-clean
	ant -f $(TOOLKIT_DIR)/impl/build.xml -Ddoc.dir=$(PWD)/$(DOC_DIR) clean
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -c -i $(TOOLKIT_DIR) -m
	$(STREAMS_INSTALL)/bin/spl-make-doc -c --output-directory $(DOC_DIR)/spldoc -i $(TOOLKIT_DIR) --doc-title "Streams DPS Toolkit"
	rm -rf $(DOC_DIR)/spldoc

install:
	tar --exclude='*.o' -czvf $(TARFILE) ./$(TOOLKIT_DIR)

binpackage:
	tar --exclude='*.o' --exclude=.gitignore --exclude=.classpath \
		--exclude=Makefile --exclude=build.xml --exclude=src --exclude=java/src \
		--exclude=java/classes --exclude=.project --exclude=.settings \
		--exclude=.toolkitList -czvf $(BINTARFILE) ./$(TOOLKIT_DIR)

install-clean:
	rm -f $(TARFILE)

binpackage-clean:
	rm -f $(BINTARFILE)

gen-message:
	cd $(TOOLKIT_DIR); $(SPL_MAKE_TOOLKIT) -i . --no-mixed-mode-preprocessing

publish:
	rm -rf $(PUBLISH_DIR)
	mkdir -p $(PUBLISH_DIR)
	rsync -av --exclude='*.o' --exclude='impl/Makefile' \
		--exclude='impl/build.xml' --exclude='impl/java/classes' \
		--exclude='impl/java/src' --exclude='impl/src' \
		$(TOOLKIT_DIR) $(PUBLISH_DIR)

check-streams:
ifndef STREAMS_INSTALL
	$(error STREAMS_INSTALL must be set)
endif

check-compiler:
ifeq ($(ARCH),ppc64)
ifeq ($(strip $(ATVERSION)),)
	$(error AT Compiler must be installed and in PATH when building on ppc64 or ppc64le platforms.  Source the ppcenv.sh script in this directory to set appropriate environment.)
endif
endif
ifeq ($(ARCH),ppc64le)
ifeq ($(strip $(ATVERSION)),)
	$(error AT Compiler must be installed and in PATH when building on ppc64 or ppc64le platforms.  Source the ppcenv.sh script in this directory to set appropriate environment.)
endif
endif

%.all : out-of-date
	$(MAKE) -j1 -C $* all

%.clean: out-of-date
	$(MAKE) -j1 -C $* clean

.PHONY: all install install-clean clean publish check-streams check-compiler out-of-date gen-message
