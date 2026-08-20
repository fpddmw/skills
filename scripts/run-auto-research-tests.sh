#!/bin/sh

set -eu

[ "${CI_CLEAN_CONTAINER:-}" = "1" ]
[ -f /.dockerenv ]
[ "$(id -u)" -ne 0 ]
[ "${HOME:-}" = "/home/node" ]
[ ! -e "$HOME/.agents" ]
if command -v tiangong-ai >/dev/null 2>&1; then
    echo "global tiangong-ai must not exist in the clean test image" >&2
    exit 1
fi

sh scripts/test-clean-container-entrypoint.sh
node tiangong-auto-research/scripts/test-research-cli.mjs
node tiangong-auto-research/scripts/test-routing-contract.mjs
node tiangong-auto-research/scripts/test-research-policy-pack.mjs
node tiangong-auto-research/scripts/test-evidence-exhaustion-contract.mjs
sh tiangong-auto-research/scripts/test-agent-wrapper-posix.sh
node --test tsinghua-graduate-thesis/scripts/tests/*.test.mjs
python3 -m unittest discover -s academic-paper-download/scripts/tests -v
python3 -m unittest discover -s document-granular-decompose/scripts/tests -v
python3 -m unittest discover -s tiangong-kb-ingest/scripts/tests -v
bash tiangong-kb-sci-search/scripts/test-sci-search.sh
bash tiangong-kb-report-search/scripts/test-report-search.sh
bash tiangong-kb-patent-search/scripts/test-patent-search.sh
