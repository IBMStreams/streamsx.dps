include ../make.variable.include

PKG_NAME := cpp-hiredis-cluster
VERSION := master
ARCHIVE := master.zip
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
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

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -rf $(TARGETDIR)/include/hiredis-cluster
