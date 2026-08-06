#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
wrapper="$script_dir/agent-wrapper-posix.sh"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/tiangong-agent-wrapper.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
target="$test_root/fake-agent"

printf '%s\n' \
  '#!/bin/sh' \
  'if [ -n "${TIANGONG_RESEARCH_AGENT_BINARY:-}" ]; then exit 65; fi' \
  'printf "%s|%s\n" "$1" "$2"' >"$target"
chmod 700 "$target"

actual=$(TIANGONG_RESEARCH_AGENT_BINARY="$target" "$wrapper" alpha "two words")
if [ "$actual" != "alpha|two words" ]; then
  printf '%s\n' "wrapper did not preserve arguments" >&2
  exit 1
fi

if TIANGONG_RESEARCH_AGENT_BINARY=relative/path "$wrapper" alpha beta >/dev/null 2>&1; then
  printf '%s\n' "wrapper accepted a relative target" >&2
  exit 1
fi

printf '%s\n' "POSIX agent wrapper test passed."
