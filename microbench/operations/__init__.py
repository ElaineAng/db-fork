"""
Operations package for extensible benchmark operations.

This package provides an extensible framework for defining and executing
database operations in benchmarks. New operation types can be added by:
1. Implementing the Operation interface from base.py
2. Registering the operation with OperationRegistry
"""

from microbench.operations.base import Operation, OperationRegistry
from microbench.operations.crud import (
    ReadOperation,
    InsertOperation,
    UpdateOperation,
    DeleteOperation,
    RangeReadOperation,
    RangeUpdateOperation,
)
from microbench.operations.branch import (
    BranchCreateOperation,
    BranchConnectOperation,
    BranchDeleteOperation,
    ConnectFirstOperation,
    ConnectMidOperation,
    ConnectLastOperation,
)
from microbench.operations.ddl import (
    AddIndexOperation,
    RemoveIndexOperation,
    VacuumOperation,
)

__all__ = [
    "Operation",
    "OperationRegistry",
    "ReadOperation",
    "InsertOperation",
    "UpdateOperation",
    "DeleteOperation",
    "RangeReadOperation",
    "RangeUpdateOperation",
    "BranchCreateOperation",
    "BranchConnectOperation",
    "BranchDeleteOperation",
    "ConnectFirstOperation",
    "ConnectMidOperation",
    "ConnectLastOperation",
    "AddIndexOperation",
    "RemoveIndexOperation",
    "VacuumOperation",
]
