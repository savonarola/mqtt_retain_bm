REBAR ?= $(CURDIR)/rebar3
REBAR_VERSION ?= 3.19.0-emqx-6
## managed by bumpversion-*
VERSION=0.1.2

REBAR_CMD = $(REBAR)

.PHONY: release
release: compile
	$(REBAR_CMD) as mqtt_retain_bm tar -v $(VERSION)

.PHONY: all
all: compile

.PHONY: compile
compile: $(REBAR)
	$(REBAR_CMD) compile

.PHONY: ensure-rebar3
ensure-rebar3:
	$(CURDIR)/scripts/ensure-rebar3.sh $(REBAR_VERSION)

.PHONY: clean
clean: distclean

.PHONY: distclean
distclean:
	@rm -rf _build erl_crash.dump rebar3.crashdump rebar.lock mqtt_retain_bm

.PHONY: bumpversion-patch
bumpversion-patch:
	bump2version patch

.PHONY: bumpversion-minor
bumpversion-minor:
	bump2version minor

$(REBAR): ensure-rebar3