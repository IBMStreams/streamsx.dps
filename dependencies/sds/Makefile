include ../make.variable.include

PKG_NAME := sds
VERSION := 1.0.0
ARCHIVE := $(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/antirez/$(PKG_NAME)/archive/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)/lib
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
# There is no "official" makefile for this package
	cd $(PKG_DIR) && patch -p1 -N < ../patch/sds.h.patch
	$(MAKE) -C $(PKG_DIR) -f ../Makefile.sds
	mkdir -p $(GEN_DIR)/lib
	cp -f $(PKG_DIR)/libsds.so $(GEN_DIR)/lib
	mkdir -p $(GEN_DIR)/include
	cp -f $(PKG_DIR)/sds.h $(GEN_DIR)/include
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	touch -c $@

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libsds*
	rm -f $(TARGETDIR)/include/sds*
