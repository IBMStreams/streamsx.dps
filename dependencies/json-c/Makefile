include ../make.variable.include

PKG_NAME := json-c
VERSION := 0.12-20140410
ARCHIVE := $(PKG_NAME)-$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/$(PKG_NAME)/$(PKG_NAME)/archive/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)-$(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

JSON_CFLAGS = -Wno-error=unused-but-set-variable
ifeq ($(OS),sles11)
JSON_CFLAGS =
endif

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
	$(MAKE) -C $(PKG_DIR)
	$(MAKE) -C $(PKG_DIR) install
	rm -f $(GEN_DIR)/lib/*.la
	rm -rf $(GEN_DIR)/lib/pkgconfig
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(PKG_DIR)/Makefile.in
	cd $(PKG_DIR) && patch -p1 < ../patch/ppc64le-configure.patch
	cd $(PKG_DIR) && patch -p1 < ../patch/ppc64le-config.patch	
	cd $(PKG_DIR) && ./configure --prefix=$(GEN_DIR) --exec-prefix=$(GEN_DIR) \
		--enable-static=no CFLAGS=$(JSON_CFLAGS) # CPPFLAGS=$(ARCH_CFLAG)

$(PKG_DIR)/Makefile.in: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	touch -c $@

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libjson-c*
	rm -rf $(TARGETDIR)/include/json-c
