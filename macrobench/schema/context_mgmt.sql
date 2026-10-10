-- S2 Agent context management: an assistant's persisted interaction history.
-- The spine appends turn/tool_call rows; compaction candidates rewrite old
-- rows and add note rows, views and indexes.

CREATE TABLE turn (
    turn_id     INT NOT NULL,
    session_id  INT NOT NULL,
    step        INT NOT NULL,
    role        VARCHAR(16) NOT NULL,
    content     VARCHAR(2000),
    token_count INT,
    created_at  TIMESTAMP,
    PRIMARY KEY (turn_id)
);

CREATE TABLE tool_call (
    call_id     INT NOT NULL,
    turn_id     INT NOT NULL,
    tool        VARCHAR(32) NOT NULL,
    args        VARCHAR(500),
    result      TEXT,
    result_size INT,
    PRIMARY KEY (call_id)
);

-- kind: 'summary' (A), 'reference' (B), 'dedup' (C), 'compaction' (sentinel)
CREATE TABLE note (
    note_id    INT NOT NULL,
    kind       VARCHAR(16) NOT NULL,
    candidate  VARCHAR(64),
    content    VARCHAR(2000),
    created_at TIMESTAMP,
    PRIMARY KEY (note_id)
);
