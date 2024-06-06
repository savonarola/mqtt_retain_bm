REBAR ?= $(CURDIR)/rebar3
REBAR_VERSION ?= 3.19.0-emqx-6

REBAR_CMD = BUILD_WITHOUT_QUIC=true $(REBAR)

.PHONY: all
all: compile
	$(REBAR_CMD) escriptize

.PHONY: compile
compile: $(REBAR)
	$(REBAR_CMD) compile

.PHONY: ensure-rebar3
ensure-rebar3:
	$(CURDIR)/scripts/ensure-rebar3.sh $(REBAR_VERSION)

$(REBAR): ensure-rebar3