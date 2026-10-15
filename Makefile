# Builds the implementation guide — see RUNBOOK.md. Configuration comes from the
# environment (see .env.example).

# .env fills in only what the shell environment hasn't already set, so
# `set -a; source .env` and plain `make` behave the same. Only the variables
# the build reads are exported — the rest of .env stays out of the recipes'
# environment. _genonce.sh reads ONTOSERVER_URL as the IG Publisher's -tx server.
ONTOSERVER_URL_ENV := $(ONTOSERVER_URL)
SSL_CERT_FILE_ENV := $(SSL_CERT_FILE)
-include .env
ONTOSERVER_URL := $(or $(ONTOSERVER_URL_ENV),$(ONTOSERVER_URL))
SSL_CERT_FILE := $(or $(SSL_CERT_FILE_ENV),$(SSL_CERT_FILE))
export ONTOSERVER_URL
ifneq ($(strip $(SSL_CERT_FILE)),)
export SSL_CERT_FILE
endif

.PHONY: ig

ig: ## compile FSH (sushi) and build the IG package (output/package.tgz)
	sushi .
	./_genonce.sh
