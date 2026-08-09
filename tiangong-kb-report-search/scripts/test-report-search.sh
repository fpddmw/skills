#!/bin/bash

set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WRAPPER="$SCRIPT_DIR/report_search.sh"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/tiangong-kb-report-test.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT

FAKE_CLI="$TEST_ROOT/fake-cli"
AUDIT_ARGS="$TEST_ROOT/args.txt"
AUDIT_ENV="$TEST_ROOT/env.txt"
MARKER="$TEST_ROOT/invoked"
STDOUT="$TEST_ROOT/stdout.txt"
STDERR="$TEST_ROOT/stderr.txt"
SECRET='owner-only-report-key-value'

printf '%s\n' \
    '#!/bin/bash' \
    'set -euo pipefail' \
    ': > "$FAKE_MARKER"' \
    'printf "%s\n" "$@" > "$FAKE_ARGS"' \
    'printf "REPORT=%s\nTRACK=%s\n" "${TIANGONG_REPORT_APIKEY:-}" "${DO_NOT_TRACK:-}" > "$FAKE_ENV"' \
    'printf "{\"ok\":true}\n"' \
    > "$FAKE_CLI"
chmod 700 "$FAKE_CLI"

run_wrapper() {
    FAKE_ARGS="$AUDIT_ARGS" \
    FAKE_ENV="$AUDIT_ENV" \
    FAKE_MARKER="$MARKER" \
    TIANGONG_AI_CLI_BIN="$FAKE_CLI" \
    "$WRAPPER" "$@"
}

TIANGONG_REPORT_APIKEY="$SECRET" \
    run_wrapper '{"query":"isolated report","dry_run":true}' > "$STDOUT" 2> "$STDERR"
grep -Fx 'report' "$AUDIT_ARGS" >/dev/null
grep -Fx 'TRACK=1' "$AUDIT_ENV" >/dev/null
if grep -F "$SECRET" "$AUDIT_ARGS" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'report credential leaked into argv or output' >&2
    exit 1
fi

rm -f "$MARKER"
if run_wrapper "{\"query\":\"blocked\",\"report_api_key\":\"$SECRET\"}" \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'credential-like report JSON was accepted' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
if grep -F "$SECRET" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'rejected report credential was echoed' >&2
    exit 1
fi

MANAGED_WORKSPACE="$TEST_ROOT/managed"
MANAGED_NESTED="$MANAGED_WORKSPACE/nested"
mkdir -p "$MANAGED_WORKSPACE/.tiangong-research" "$MANAGED_NESTED"
printf '%s\n' '{"schemaVersion":1,"packageName":"@tiangong-ai/cli","packageVersion":"0.0.30"}' \
    > "$MANAGED_WORKSPACE/.tiangong-research/runtime-lock.json"
rm -f "$MARKER"
if (cd "$MANAGED_NESTED" && run_wrapper '{"query":"systematic report work"}') \
    > "$STDOUT" 2> "$STDERR"; then
    echo 'managed report wrapper bypassed the broker guard' >&2
    exit 1
fi
[ ! -e "$MARKER" ]
grep -F 'AUTO_RESEARCH_BROKER_REQUIRED' "$STDERR" >/dev/null
grep -F '"credentialScope":"broker"' "$STDERR" >/dev/null
grep -F '"networkAttempted":false' "$STDERR" >/dev/null

(cd "$MANAGED_NESTED" && \
    TIANGONG_REPORT_APIKEY="$SECRET" \
    run_wrapper '{"query":"isolated report","execution_mode":"standalone","dry_run":true}') \
    > "$STDOUT" 2> "$STDERR"
grep -F 'STANDALONE_MODE_SELECTED' "$STDERR" >/dev/null
grep -F '"credentialScope":"ambient-or-explicit-owner-env"' "$STDERR" >/dev/null
if grep -F "$SECRET" "$AUDIT_ARGS" "$STDOUT" "$STDERR" >/dev/null; then
    echo 'explicit report standalone mode leaked a credential' >&2
    exit 1
fi

echo 'tiangong-kb-report-search wrapper tests passed'
