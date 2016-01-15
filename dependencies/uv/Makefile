include ../make.variable.include

PKG_NAME := libuv
VERSION := 0.10.29
ARCHIVE := v$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/joyent/$(PKG_NAME)/archive/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
	$(MAKE) -C $(PKG_DIR)
	mkdir -p $(GEN_DIR)/lib
	cp -f $(PKG_DIR)/libuv.so $(GEN_DIR)/lib/libuv.so.0.10
	cd $(GEN_DIR)/lib && ln -s libuv.so.0.10 libuv.so
	mkdir -p $(GEN_DIR)/include/adapters
	cp -f $(PKG_DIR)/include/uv.h $(GEN_DIR)/include/adapters
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	touch -c $@

dep:
	tar -xzvf $(ARCHIVE)
	ln -sf $(PKG_DIR)/include ./depinclude

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libuv*
	rm -f $(TARGETDIR)/include/adapters/uv*
	rm -f ./depinclude
