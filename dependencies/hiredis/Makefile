include ../make.variable.include

PKG_NAME := hiredis
VERSION := 1.0.0
ARCHIVE := v$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/redis/$(PKG_NAME)/archive/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
	$(MAKE) -C $(PKG_DIR) USE_SSL=1
	$(MAKE) -C $(PKG_DIR) USE_SSL=1 PREFIX=$(GEN_DIR) install
	rm -f $(GEN_DIR)/lib/*.a
	rm -rf $(GEN_DIR)/lib/pkgconfig
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	cd $(PKG_DIR) && patch -p1 -N < ../patch/sds.h.patch
	touch -c $@

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -rf $(TARGETDIR)/lib/libhiredis*
	rm -rf $(TARGETDIR)/include/hiredis
