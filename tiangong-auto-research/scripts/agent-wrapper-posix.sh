#!/bin/sh

set -eu

target=${TIANGONG_RESEARCH_AGENT_BINARY:-}
case "$target" in
  /*) ;;
  *)
    printf '%s\n' "TIANGONG_RESEARCH_AGENT_BINARY must be an absolute path." >&2
    exit 64
    ;;
esac

if [ ! -f "$target" ] || [ ! -x "$target" ]; then
  printf '%s\n' "Configured research agent binary is not an executable regular file." >&2
  exit 69
fi

unset TIANGONG_RESEARCH_AGENT_BINARY
exec "$target" "$@"
