# requires GNU make
# riak_search's installation path is currently "riaksearch"
APP_PATH	?= $(shell echo "$(APP)" | sed 's/-//g')
PKG		 = BASHO$(APP)
# possible ARCH values are i386, sparc, all
ARCH		 = $(shell uname -p)
SOLARIS_VER	?= $(shell echo "$(KERNEL)" | sed -e 's/^5\.//')
PKGFILE		 = $(PKG)-$(REVISION)-$(RELEASE)-$(DISTRO)$(SOLARIS_VER)-$(ARCH).pkg

build: buildrel depend pkginfo prototype
	cp $(PKGERDIR)/copyright $(PKGERDIR)/preinstall $(PKGERDIR)/*.preserve .
	mkdir -p pkgbuild packages
	pkgmk -o -d pkgbuild -a $(ARCH)
	touch packages/$(PKGFILE)
	pkgtrans -s pkgbuild packages/$(PKGFILE) $(PKG) 
	rm -r pkgbuild/$(PKG)
	gzip packages/$(PKGFILE)
	chmod 0644 $(CURDIR)/packages/$(PKGFILE).gz
	@echo
	@echo Wrote: $(CURDIR)/packages/$(PKGFILE).gz
	@echo

# Build the release we need to package
buildrel:
	@# Make sure we set our EUID properly
	echo "" >> $(SEARCH_PATH)/rel/vars.config
	echo '{runner_user, "riak"}.' >> $(SEARCH_PATH)/rel/vars.config
	@# Ye Olde Bourne Shell on Solaris means we have to do it old school
	PATH=/opt/erlang/R13B04-$(DISTRO)$(SOLARIS_VER):$${PATH}; \
	export PATH; \
	echo "Using `which erl` to build"; \
	$(MAKE) -C $(SEARCH_PATH) deps rel
	chmod 0755 $(SEARCH_PATH)/rel/$(APP_PATH)/bin/* \
		$(SEARCH_PATH)/rel/$(APP_PATH)/erts-*/bin/*

depend:
	cp $(PKGERDIR)/depend .
	if [ $(DISTRO) = "OpenSolaris" ]; then \
		echo "P SUNWgawk   GNU implementation of awk" >> depend; \
	fi

pkginfo:
	sed -e 's/@@VERSION@@/$(REVISION)-$(RELEASE)/g' \
		-e 's/@@PKG@@/$(PKG)/g' \
		-e 's/@@PKGNAME@@/$(APP_PATH)/g' \
		< $(PKGERDIR)/pkginfo.tmpl > pkginfo

# NOTE!  The instances of riak below shouldn't change
prototype:
	echo "i pkginfo" > prototype
	echo "i copyright" >> prototype
	echo "i depend" >> prototype
	echo "i preinstall" >> prototype
	echo "i i.preserve" >> prototype
	echo "i r.preserve" >> prototype
	echo '' >> prototype
	pkgproto $(SEARCH_PATH)/rel/$(APP_PATH)=$(APP_PATH) >> prototype
	sed -i -e "s/ $(LOGNAME) .*$$/ riak riak/" \
	       -e 's/f none riak\/etc/e preserve riak\/etc/' prototype

$(PKGERDIR)/pkgclean:
	rm -rf copyright depend pkgbuild pkginfo prototype

