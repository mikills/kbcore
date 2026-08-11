SHELL := /bin/bash

.PHONY: test build run fmt release release-major

GOFILES := $(shell find . -name '*.go' -not -path './testdata/*')
GOLINES := go run github.com/segmentio/golines@latest
MAX_LEN ?= 120

test:
	go test ./... -v -race -count=1
	go test ./codeindex/... -count=1

build:
	GOWORK=off go build ./...
	go build -o /tmp/minnow-codeindex ./codeindex

run:
	go run .

fmt:
	$(GOLINES) --max-len=$(MAX_LEN) -w .
	gofmt -w $(GOFILES)

# Release tags and pushes a semver tag for Minnow and its nested codeindex
# module. Default bumps minor: vX.Y.0 -> vX.(Y+1).0.
# Examples:
#   make release                 # bump minor
#   make release MAJOR=1         # bump major
#   make release-major           # bump major
#   make release MINOR=3         # set minor on current major, patch=0
#   make release VERSION=v1.2.3  # explicit version
#   make release DRY_RUN=1       # print without tagging/pushing
release: test
	@set -euo pipefail; \
	latest="$$(git describe --tags --match 'v[0-9]*' --abbrev=0 2>/dev/null || true)"; \
	if [[ -z "$$latest" ]]; then latest="v0.0.0"; fi; \
	version="$(VERSION)"; \
	if [[ -z "$$version" ]]; then \
		base="$${latest#v}"; \
		IFS=. read -r major minor patch <<<"$$base"; \
		major="$${major:-0}"; minor="$${minor:-0}"; patch="$${patch:-0}"; \
		if [[ -n "$(MAJOR)" ]]; then \
			major=$$((major + 1)); minor=0; patch=0; \
		elif [[ -n "$(MINOR)" ]]; then \
			minor="$(MINOR)"; patch=0; \
		else \
			minor=$$((minor + 1)); patch=0; \
		fi; \
		version="v$${major}.$${minor}.$${patch}"; \
	fi; \
	if [[ ! "$$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then \
		echo "invalid version: $$version (expected vX.Y.Z)" >&2; exit 1; \
	fi; \
	codeindex_tag="codeindex/$$version"; \
	echo "latest tag: $$latest"; \
	echo "release tag: $$version"; \
	echo "codeindex tag: $$codeindex_tag"; \
	if [[ -n "$(DRY_RUN)" ]]; then \
		echo "dry run: git tag $$version && git tag $$codeindex_tag && git push origin $$version $$codeindex_tag"; \
		exit 0; \
	fi; \
	git tag "$$version"; \
	git tag "$$codeindex_tag"; \
	git push origin "$$version" "$$codeindex_tag"

release-major:
	$(MAKE) release MAJOR=1
