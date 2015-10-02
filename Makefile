version = 0.1

PROJECT = erocci_backend_mnesia
PROJECT_VERSION = $(shell git describe --always --tags 2> /dev/null || echo $(version))

DEPS = erocci_core
dep_erocci_core = git https://github.com/erocci/erocci_core.git master

include erlang.mk

fetch: $(ALL_DEPS_DIRS)
	for d in $(ALL_DEPS_DIRS); do \
	  $(MAKE) -C $$d $@ || true; \
	done

.PHONY: fetch
