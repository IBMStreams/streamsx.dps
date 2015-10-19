# -*- makefile -*-

PWD = $(shell pwd)

SUBDIRS += dependencies
SUBDIRS += com.ibm.streamsx.dps/impl

TARFILE = com.ibm.streamsx.dps-install.tar.gz

ARCH = $(shell dependencies/platform-info.pl --arch)
OS = $(shell dependencies/platform-info.pl  --osname_rpm_format)
ATVERSION = $(shell dependencies/platform-info.pl --atver)

TOOLKIT_DIR = com.ibm.streamsx.dps

DOC_DIR = com.ibm.streamsx.dps/doc

PUBLISH_ROOT ?= $(HOME)/publish
PUBLISH_DIR ?= $(PUBLISH_ROOT)/$(ARCH)/$(OS)

all: check-streams check-compiler ${SUBDIRS:%=%.all}
	ant -f $(TOOLKIT_DIR)/impl/build.xml -Ddoc.dir=$(PWD)/$(DOC_DIR) all
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -i $(TOOLKIT_DIR) -m
	$(STREAMS_INSTALL)/bin/spl-make-doc --output-directory $(DOC_DIR)/spldoc -i $(TOOLKIT_DIR) --doc-title "Streams DPS Toolkit"

clean: check-streams ${SUBDIRS:%=%.clean} install-clean
	ant -f $(TOOLKIT_DIR)/impl/build.xml -Ddoc.dir=$(PWD)/$(DOC_DIR) clean
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -c -i $(TOOLKIT_DIR) -m
	$(STREAMS_INSTALL)/bin/spl-make-doc -c --output-directory $(DOC_DIR)/spldoc -i $(TOOLKIT_DIR) --doc-title "Streams DPS Toolkit"
	rm -rf $(DOC_DIR)/spldoc

install:
	tar --exclude='*.o' -czvf $(TARFILE) ./$(TOOLKIT_DIR)

install-clean:
	rm -f $(TARFILE)

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

.PHONY: all install install-clean clean publish check-streams check-compiler out-of-date
