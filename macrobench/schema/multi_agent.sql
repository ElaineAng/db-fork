-- S3 Multi-agent collaboration: shared task management state.
-- task_id = 0 is the sentinel task whose status is changed on branches that
-- merge at different times.

CREATE TABLE task (
    task_id      INT NOT NULL,
    parent_id    INT,
    title        VARCHAR(120) NOT NULL,
    status       VARCHAR(16) NOT NULL,
    assignee     VARCHAR(32),
    version      INT NOT NULL,
    updated_step INT NOT NULL,
    PRIMARY KEY (task_id)
);

CREATE TABLE dependency (
    task_id    INT NOT NULL,
    depends_on INT NOT NULL,
    added_by   VARCHAR(32),
    PRIMARY KEY (task_id, depends_on)
);
