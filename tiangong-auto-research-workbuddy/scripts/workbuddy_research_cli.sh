#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
resolver="$script_dir/../../tiangong-auto-research/scripts/research_cli.mjs"

if [ ! -f "$resolver" ] || [ -L "$resolver" ]; then
    printf '%s\n' '{"error":{"code":"AUTO_RESEARCH_CANONICAL_SKILL_REQUIRED","message":"The project-installed canonical Auto Research resolver is missing or invalid.","details":{"minimumAction":"Install the pinned tiangong-auto-research Skill beside this WorkBuddy adapter."}}}' >&2
    exit 2
fi

ambient_node=$(command -v node 2>/dev/null || true)
configured_node=${AUTO_RESEARCH_NODE:-}
owner_home=${HOME:-}

for candidate in \
    "$configured_node" \
    "$owner_home"/.nvm/versions/node/v24*/bin/node \
    "$owner_home"/.local/share/fnm/node-versions/v24*/installation/bin/node \
    "$owner_home"/.volta/bin/node \
    /usr/local/bin/node \
    /opt/homebrew/bin/node \
    "$ambient_node"; do
    [ -n "$candidate" ] || continue
    case "$candidate" in
        /*) ;;
        *) continue ;;
    esac
    [ -x "$candidate" ] || continue
    major=$(
        "$candidate" -p 'Number(process.versions.node.split(".")[0])' 2>/dev/null || true
    )
    [ "$major" = 24 ] || continue
    resolved=$(
        "$candidate" -p 'process.execPath' 2>/dev/null || true
    )
    case "$resolved" in
        /*) exec "$resolved" "$resolver" "$@" ;;
    esac
done

printf '%s\n' '{"error":{"code":"AUTO_RESEARCH_NODE_VERSION_INVALID","message":"No explicit Node.js 24 executable is available to the WorkBuddy adapter.","details":{"minimumAction":"Install Node.js 24 or set AUTO_RESEARCH_NODE to its absolute executable path; do not use WorkBuddy ambient Node 22."}}}' >&2
exit 2
