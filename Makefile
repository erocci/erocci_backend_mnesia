PROJECT = erocci_backend_mnesia

DEPS = erocci_core uuid
dep_erocci_core = git https://github.com/erocci/erocci_core.git next

include erlang.mk

fetch: $(ALL_DEPS_DIRS)
	for d in $(ALL_DEPS_DIRS); do \
	  $(MAKE) -C $$d $@ || true; \
	done

.PHONY: fetch
