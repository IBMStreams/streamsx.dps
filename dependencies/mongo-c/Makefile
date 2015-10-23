include ../make.variable.include

# This package includes libbson

PKG_NAME := mongo-c-driver
VERSION := 1.0.2
ARCHIVE := $(PKG_NAME)-$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/mongodb/$(PKG_NAME)/releases/download/$(VERSION)/$(PKG_NAME)-$(VERSION).tar.gz
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
	$(MAKE) -C $(PKG_DIR) install
	cd $(GEN_DIR)/lib && ln -sf libbson-1.0.so.0.0.0 ./libbson.so
	cd $(GEN_DIR)/lib && ln -sf libmongoc-1.0.so.0.0.0 ./libmongoc.so
	rm -f $(GEN_DIR)/lib/*.la
	rm -f $(GEN_DIR)/lib/libmongoc-priv*
	rm -rf $(GEN_DIR)/lib/pkgconfig
	rm -rf $(GEN_DIR)/bin
	rm -rf $(GEN_DIR)/share
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(PKG_DIR)/Makefile.in
#	cd $(PKG_DIR) && patch -p1 < ../patch/ppc64le-configure.patch
#	cd $(PKG_DIR) && patch -p1 < ../patch/ppc64le-libtool.patch
#	cd $(PKG_DIR) && patch -p1 < ../patch/libbson-ppc64le-configure.patch
#	cd $(PKG_DIR) && patch -p1 < ../patch/libbson-ppc64le-libtool.patch
	cd $(PKG_DIR) && ./configure --prefix=$(GEN_DIR) --exec-prefix=$(GEN_DIR) \
		--enable-static=no # CPPFLAGS=$(ARCH_CFLAG)

$(PKG_DIR)/Makefile.in: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	touch -c $@

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libmongo*
	rm -f $(TARGETDIR)/lib/libbson*
	rm -rf $(TARGETDIR)/include/libmongoc-1.0
	rm -rf $(TARGETDIR)/include/libbson-1.0
