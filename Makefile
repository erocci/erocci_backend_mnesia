version = 1.0

PROJECT = erocci_backend_mnesia
PROJECT_VERSION = PROJECT_VERSION = $(shell git describe --always --tags 2> /dev/null | sed -e 's/v\(.*\)/\1/' || echo $(version))

DEPS = erocci_core uuid
dep_erocci_core = git https://github.com/erocci/erocci_core.git next

include erlang.mk

fetch: $(ALL_DEPS_DIRS)
	for d in $(ALL_DEPS_DIRS); do \
	  $(MAKE) -C $$d $@ || true; \
	done

.PHONY: fetch
