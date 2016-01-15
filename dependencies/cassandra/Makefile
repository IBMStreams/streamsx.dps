include ../make.variable.include

PKG_NAME := cpp-driver
VERSION := 1.0.1
ARCHIVE := $(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/datastax/$(PKG_NAME)/archive/$(VERSION).tar.gz
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

UV_DIR := $(PWD)/../uv

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

$(ARCHIVE_INSTALL): deps $(PKG_DIR)/Makefile
	$(MAKE) -C $(PKG_DIR)
	$(MAKE) -C $(PKG_DIR) install
	test -d $(GEN_DIR)/lib64 && mv -f $(GEN_DIR)/lib64 $(GEN_DIR)/lib
	mkdir -p $(GEN_DIR)/include/libcassandra-1.0
	mv -f $(GEN_DIR)/include/*.h $(GEN_DIR)/include/libcassandra-1.0
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	cd $(PKG_DIR) && cmake -DLIBUV_INCLUDE_DIR=$(UV_DIR)/depinclude -DLIBUV_ROOT_DIR=$(TARGETDIR) -DCMAKE_INSTALL_PREFIX=$(GEN_DIR)
	touch -c $@

deps:
	make -C $(UV_DIR)
	make -C $(UV_DIR) dep
#	make -C $(PWD)/boost

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libcassandra*
	rm -rf $(TARGETDIR)/include/libcassandra-1.0
