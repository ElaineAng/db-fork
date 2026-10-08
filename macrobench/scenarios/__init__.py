"""The six macrobenchmark scenarios. Importing the package registers them."""

from macrobench.scenarios.base import (  # noqa: F401
    SCENARIOS,
    InvariantFailed,
    InvariantRecorder,
    Scenario,
    ScenarioContext,
    ScenarioStopped,
    Worker,
    create_scenario,
    register,
)

from macrobench.scenarios import s1_rl_env  # noqa: F401,E402
from macrobench.scenarios import s2_context_mgmt  # noqa: F401,E402
from macrobench.scenarios import s3_multi_agent  # noqa: F401,E402
from macrobench.scenarios import s4_dev_agent  # noqa: F401,E402
from macrobench.scenarios import s5_ops_agent  # noqa: F401,E402
from macrobench.scenarios import s6_data_agent  # noqa: F401,E402
