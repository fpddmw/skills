#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
image_tag="tiangong-ai-skills-clean-test:local-$$"

usage() {
    printf '%s\n' "Usage: scripts/test-clean-container.sh [--cold-build]"
}

build_mode=cached
case "$#" in
    0) ;;
    1)
        case "$1" in
            --cold-build) build_mode=cold ;;
            --help)
                usage
                exit 0
                ;;
            *)
                printf '%s\n' "Unknown clean-container mode: $1" >&2
                usage >&2
                exit 2
                ;;
        esac
        ;;
    *)
        printf '%s\n' "Only one clean-container mode may be selected." >&2
        usage >&2
        exit 2
        ;;
esac

cleanup() {
    docker image rm --force "$image_tag" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

set -- \
    --file "$repo_root/Dockerfile.clean-test" \
    --tag "$image_tag"
if [ "$build_mode" = "cold" ]; then
    set -- --no-cache "$@"
fi

printf '%s\n' "Clean-container build mode: $build_mode"
docker build "$@" "$repo_root"

docker run \
    --rm \
    --init \
    --network none \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --tmpfs /tmp:rw,exec,nosuid,nodev,uid=1000,gid=1000,mode=1777 \
    --tmpfs /home/node:rw,nosuid,nodev,uid=1000,gid=1000,mode=0700 \
    "$image_tag"
