REBAR ?= $(CURDIR)/rebar3
REBAR_VERSION ?= 3.19.0-emqx-6

REBAR_CMD = $(REBAR)

.PHONY: release
release: compile
	$(REBAR_CMD) as mqtt_retain_bm tar

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

$(REBAR): ensure-rebar3