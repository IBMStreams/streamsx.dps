include ../make.variable.include

PKG_NAME := redis-plus-plus
VERSION := 1.2.1
ARCHIVE := $(PKG_NAME)-$(VERSION).tar.gz
ARCHIVE_INSTALL := $(PKG_NAME)-$(VERSION)-$(OS)-$(ARCH)-install-bin.tar.gz
PKG_DIR = $(PKG_NAME)-$(VERSION)
GEN_DIR := $(shell pwd)/gen
HIREDIS_PKG := hiredis
HIREDIS_VERSION := 1.0.0
HIREDIS_PKG_DIR := $(HIREDIS_PKG)-$(HIREDIS_VERSION)
HIREDIS_SRC_DIR := $(HIREDIS_PKG)-$(HIREDIS_VERSION)-src

all:
	test -f $(ARCHIVE_INSTALL) || $(MAKE) $(ARCHIVE_INSTALL)
	@echo "Checking to see TARGETDIR is set..."
	@test -n "$(TARGETDIR)" || false
	mkdir -p $(TARGETDIR)
	tar -C $(TARGETDIR) -xzvf $(ARCHIVE_INSTALL)
	rm -f $(ARCHIVE_INSTALL)

$(ARCHIVE_INSTALL): $(PKG_DIR)/Makefile
	$(MAKE) -C $(HIREDIS_SRC_DIR) PREFIX=../$(HIREDIS_PKG_DIR) USE_SSL=1
	$(MAKE) -C $(HIREDIS_SRC_DIR) PREFIX=../$(HIREDIS_PKG_DIR) USE_SSL=1 install
	cd $(PKG_DIR) && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=../$(HIREDIS_PKG_DIR) -DCMAKE_INSTALL_PREFIX=../$(PKG_NAME) -DREDIS_PLUS_PLUS_BUILD_TEST=OFF -DREDIS_PLUS_PLUS_BUILD_STATIC=OFF -DREDIS_PLUS_PLUS_USE_TLS=ON
	cd $(PKG_DIR) && $(MAKE)
	cd $(PKG_DIR) && $(MAKE) install
	rm -rf  $(HIREDIS_SRC_DIR)
	rm -rf  $(HIREDIS_PKG_DIR)
	rm -rf  $(PKG_DIR)
	tar -C $(PKG_NAME) -czvf $(ARCHIVE_INSTALL) ./lib ./include
	rm -rf $(PKG_NAME)

$(PKG_DIR)/Makefile: $(ARCHIVE)
	tar -xvzf $(ARCHIVE) 
	tar -xvzf $(HIREDIS_PKG_DIR).tar.gz
	mv $(HIREDIS_PKG_DIR) $(HIREDIS_SRC_DIR)

clean:
	rm -rf $(PKG_DIR)
	rm -rf $(PKG_NAME)
	rm -f $(ARCHIVE_INSTALL)
	rm -rf  $(HIREDIS_SRC_DIR)
	rm -rf  $(HIREDIS_PKG_DIR)
	rm -rf $(TARGETDIR)/lib/libredis++*
	rm -rf $(TARGETDIR)/include/sw
