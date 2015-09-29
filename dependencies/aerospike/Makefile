include ../make.variable.include

PKG_NAME := aerospike-client-c
VERSION := 3.1.22
ARCHIVE := $(PKG_NAME)-$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
URL := https://github.com/$(PKG_NAME)/$(PKG_NAME)/archive/$(ARCHIVE)
PKG_DIR = $(PKG_NAME)
GEN_DIR := $(shell pwd)/gen

ALLTARGETS = all-supported
ifneq ($(ARCH),x86_64)
ALLTARGETS = all-unsupported
endif

all: $(ALLTARGETS)

all-supported:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)

all-unsupported:
	@echo "Unsupported Architecture $(ARCH)"

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
	$(MAKE) -C $(PKG_DIR)
	mkdir -p $(GEN_DIR)/lib
	cp -f $(PKG_DIR)/target/Linux-$(ARCH)/lib/*.so $(GEN_DIR)/lib
	mkdir -p $(GEN_DIR)/include
	cp -rf $(PKG_DIR)/target/Linux-$(ARCH)/include/aerospike $(GEN_DIR)/include
	cp -rf $(PKG_DIR)/target/Linux-$(ARCH)/include/citrusleaf $(GEN_DIR)/include
	tar -C $(GEN_DIR) -czvf $(ARCHIVE_INSTALL) ./lib ./include

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE)
	touch -c $@

archive:
# This target is to be called manually to make the source archive file
# For now, the downloaded tarfile doesn't have all of the right
# submodule things in it.  So do a git clone instead.
	git clone -b "$(VERSION)"  https://github.com/aerospike/aerospike-client-c.git
	cd $(PKG_DIR) && git submodule update --init
	find $(PKG_DIR) -name ".git*" | xargs rm -rf
	tar -czvf $(ARCHIVE) $(PKG_DIR)

clean:
	rm -rf $(PKG_DIR) $(GEN_DIR)
	rm -f $(TARGETDIR)/lib/libaerospike*
	rm -rf $(TARGETDIR)/include/aerospike
	rm -rf $(TARGETDIR)/include/citrusleaf
