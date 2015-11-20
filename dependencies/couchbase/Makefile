include ../make.variable.include

PKG_NAME := libcouchbase
VERSION := 2.4.5
ARCHIVE := $(PKG_NAME)-$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := http://packages.couchbase.com/clients/c/$(ARCHIVE)
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
	rm -rf $(GEN_DIR)/include/libcouchbase/plugins
	rm -rf $(GEN_DIR)/share
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	cd $(PKG_DIR) && ./configure.pl --prefix=$(GEN_DIR) --exec-prefix=$(GEN_DIR) \
		--disable-tools --disable-tests --disable-plugins
	touch -c $@

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libcouchbase*
	rm -rf $(TARGETDIR)/include/libcouchbase
