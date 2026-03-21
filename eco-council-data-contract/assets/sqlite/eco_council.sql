PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS missions (
    run_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    topic TEXT NOT NULL,
    objective TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    window_end_utc TEXT NOT NULL,
    region_label TEXT NOT NULL,
    mission_json TEXT NOT NULL CHECK (json_valid(mission_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS rounds (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('planned', 'active', 'completed', 'blocked')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (run_id, round_id),
    FOREIGN KEY (run_id) REFERENCES missions(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS round_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    assigned_role TEXT NOT NULL CHECK (assigned_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    status TEXT NOT NULL CHECK (status IN ('planned', 'in_progress', 'completed', 'blocked')),
    objective TEXT NOT NULL,
    task_json TEXT NOT NULL CHECK (json_valid(task_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    claim_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('candidate', 'selected', 'dismissed', 'validated')),
    priority INTEGER NOT NULL,
    needs_physical_validation INTEGER NOT NULL CHECK (needs_physical_validation IN (0, 1)),
    summary TEXT NOT NULL,
    claim_json TEXT NOT NULL CHECK (json_valid(claim_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    source_skill TEXT NOT NULL,
    metric TEXT NOT NULL,
    aggregation TEXT NOT NULL CHECK (aggregation IN ('point', 'window-summary', 'series-summary', 'event-count')),
    unit TEXT NOT NULL,
    observation_json TEXT NOT NULL CHECK (json_valid(observation_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evidence_cards (
    evidence_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('supports', 'contradicts', 'mixed', 'insufficient')),
    confidence TEXT NOT NULL CHECK (confidence IN ('low', 'medium', 'high')),
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS expert_reports (
    report_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    agent_role TEXT NOT NULL CHECK (agent_role IN ('moderator', 'sociologist', 'environmentalist', 'historian')),
    status TEXT NOT NULL CHECK (status IN ('complete', 'needs-more-evidence', 'blocked')),
    report_json TEXT NOT NULL CHECK (json_valid(report_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS council_decisions (
    decision_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    moderator_status TEXT NOT NULL CHECK (moderator_status IN ('continue', 'complete', 'blocked')),
    evidence_sufficiency TEXT NOT NULL CHECK (evidence_sufficiency IN ('sufficient', 'partial', 'insufficient')),
    next_round_required INTEGER NOT NULL CHECK (next_round_required IN (0, 1)),
    decision_json TEXT NOT NULL CHECK (json_valid(decision_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (run_id, round_id) REFERENCES rounds(run_id, round_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_round_tasks_run_round_role
    ON round_tasks(run_id, round_id, assigned_role);

CREATE INDEX IF NOT EXISTS idx_claims_run_round_type
    ON claims(run_id, round_id, claim_type);

CREATE INDEX IF NOT EXISTS idx_observations_run_round_metric
    ON observations(run_id, round_id, metric);

CREATE INDEX IF NOT EXISTS idx_evidence_cards_run_round_claim
    ON evidence_cards(run_id, round_id, claim_id);

CREATE INDEX IF NOT EXISTS idx_reports_run_round_role
    ON expert_reports(run_id, round_id, agent_role);

CREATE INDEX IF NOT EXISTS idx_decisions_run_round
    ON council_decisions(run_id, round_id);
