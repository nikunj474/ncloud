#!/bin/bash
# PennCloud smoke test entrypoint.
# The course container does not include Python, so keep this as a curl-only test.
set -e

cd "$(dirname "$0")"
exec bash ./smoke_test_curl.sh "$@"
