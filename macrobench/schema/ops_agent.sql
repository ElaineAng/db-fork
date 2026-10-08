-- S5 Operations agent: production deploy log. The bad deployment's row is
-- the head sentinel; incident_finding is created on investigation branches.

CREATE TABLE deploy_log (
    deploy_id   INT NOT NULL,
    version     VARCHAR(32) NOT NULL,
    deployed_at TIMESTAMP,
    note        VARCHAR(200),
    PRIMARY KEY (deploy_id)
);
