include ../make.variable.include

PKG_NAME := curl
VERSION := 7.49.0
ARCHIVE := $(PKG_NAME)-$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := http://curl.haxx.se/download/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen

ALL_TARGETS = all-build
#ifeq ($(OS),el7)
#ALL_TARGETS = all-skip
#endif

all: $(ALL_TARGETS)

all-build:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

all-skip:
	@echo "Using Operating System-installed curl"

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
	$(MAKE) -C $(PKG_DIR)
	$(MAKE) -C $(PKG_DIR) install
	rm -f $(GEN_DIR)/lib/*.la
	rm -rf $(GEN_DIR)/lib/pkgconfig
	rm -rf $(GEN_DIR)/share
	rm -rf $(GEN_DIR)/bin
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(PKG_DIR)/Makefile.in
	cd $(PKG_DIR) && patch -p1 < ../patch/ppc64le-configure.patch
	cd $(PKG_DIR) && patch -p1 < ../patch/ppc64le-libtool.patch
	cd $(PKG_DIR) && ./configure --prefix=$(GEN_DIR) --exec-prefix=$(GEN_DIR) --enable-static=no # CPPFLAGS=$(ARCH_CFLAG)

$(PKG_DIR)/Makefile.in: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	touch -c $@

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -rf $(TARGETDIR)/lib/libcurl*
	rm -rf $(TARGETDIR)/include/curl
