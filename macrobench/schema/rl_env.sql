-- S1 Agentic RL environment: the task registry on the spine. Each task
-- commit inserts its row; rollout branches add repair_audit (see
-- scenarios/s1_rl_env.py) and the item sentinels live at reserved ids.

CREATE TABLE rl_task (
    task_id      INT NOT NULL,
    fault_id     INT NOT NULL,
    fault_w_id   INT NOT NULL,
    fault_d_id   INT NOT NULL,
    status       VARCHAR(16) NOT NULL,
    created_at   TIMESTAMP,
    PRIMARY KEY (task_id)
);
