include ../make.variable.include

PKG_NAME := cpp-hiredis-cluster
VERSION := master
ARCHIVE := master.zip
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
# list of supported platforms
ARCH1 := $(PKG_NAME)-$(VERSION)-el6-ppc64-install-bin.tar.gz
ARCH2 := $(PKG_NAME)-$(VERSION)-el6-x86_64-install-bin.tar.gz
ARCH3 := $(PKG_NAME)-$(VERSION)-el7-ppc64-install-bin.tar.gz
ARCH4 := $(PKG_NAME)-$(VERSION)-el7-ppc64le-install-bin.tar.gz
ARCH5 := $(PKG_NAME)-$(VERSION)-el7-x86_64-install-bin.tar.gz
ARCH6 := $(PKG_NAME)-$(VERSION)-sles11-x86_64-install-bin.tar.gz
URL := https://github.com/shinberg/$(PKG_NAME)/archive/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

#$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
$(ARCHIVE_INSTALL):
#	$(MAKE) -C $(PKG_DIR)
#	$(MAKE) -C $(PKG_DIR) install
#	mkdir -p $(GEN_DIR)/include/hiredis-cluster
	mkdir -p $(GEN_DIR)/include
	cd $(GEN_DIR)/include && unzip -o ../../master.zip
#	cp -f $(PKG_DIR)/include/* $(GEN_DIR)/include/hiredis-cluster
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./include

#$(PKG_DIR)/Makefile: $(ARCHIVE)
#	unzip -o $(ARCHIVE)
#	cd $(PKG_DIR) && cmake .
#	cd $(PKG_DIR) && patch -p1 < ../clusterexception.h.cpp11.patch
#	cd $(PKG_DIR) && patch -p1 < ../cluster.h.cpp11.patch
#	cd $(PKG_DIR) && patch -p1 < ../container.h.cpp11.patch
#	cd $(PKG_DIR) && patch -p1 < ../hirediscommand.h.cpp11.patch
#	touch -c $@

packages:
	tar czvf master.tgz include
	cp master.tgz $(ARCH1)
	cp master.tgz $(ARCH2)
	cp master.tgz $(ARCH3)
	cp master.tgz $(ARCH4)
	cp master.tgz $(ARCH5)
	cp master.tgz $(ARCH6)
	
clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -rf $(TARGETDIR)/include/hiredis-cluster
	rm -rf master.tgz

