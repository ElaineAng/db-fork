"""
Base classes for extensible benchmark operations.

This module defines the Operation interface that all benchmark operations
must implement, and the OperationRegistry for registering and creating operations.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, Type, Optional

from dblib import result_pb2 as rslt
from microbench import task2_pb2 as tp

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class Operation(ABC):
    """Abstract base class for all benchmark operations.

    To add a new operation type:
    1. Create a subclass implementing all abstract methods
    2. Register it with OperationRegistry.register()
    3. Add the operation type to task2.proto if needed

    Example:
        class CustomOperation(Operation):
            def execute(self, context: WorkerContext) -> None:
                # Your operation logic here
                context.db_tools.execute_sql("...", timed=True)

            def requires_setup_data(self) -> bool:
                return True  # If you need existing data

            def get_operation_type(self) -> rslt.OpType:
                return rslt.OpType.UNSPECIFIED  # Or appropriate type
    """

    @abstractmethod
    def execute(self, context: 'WorkerContext') -> None:
        """Execute the operation with timing.

        This method should perform the actual database operation and
        use context.db_tools.execute_sql() with timed=True to record
        timing information.

        Args:
            context: WorkerContext providing access to database connection,
                     random number generator, data generators, etc.

        Raises:
            Exception: Any database or operation-specific errors.
        """
        pass

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Execute the operation asynchronously.

        Default implementation runs the sync execute() in a thread pool.
        Override this method for true async support using await on
        context.db_tools.*_async() methods.

        Args:
            context: WorkerContext providing access to database connection,
                     random number generator, data generators, etc.

        Raises:
            Exception: Any database or operation-specific errors.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.execute, context)

    @abstractmethod
    def requires_setup_data(self) -> bool:
        """Whether this operation requires data from setup phase.

        Returns:
            True if the operation needs existing branches, rows, or other
            setup artifacts. False if it can run on an empty database.

        Examples:
            - READ, UPDATE operations return True (need existing rows)
            - INSERT, BRANCH_CREATE return False (can run without data)
        """
        pass

    @abstractmethod
    def get_operation_type(self) -> rslt.OpType:
        """Return the OpType enum for result collection.

        Returns:
            The rslt.OpType enum value corresponding to this operation,
            used for categorizing results in the output parquet file.
        """
        pass

    def get_operation_name(self) -> str:
        """Return a human-readable name for this operation.

        Default implementation returns the class name without "Operation" suffix.
        Override to provide a custom name.

        Returns:
            Human-readable operation name.
        """
        name = self.__class__.__name__
        if name.endswith("Operation"):
            name = name[:-9]  # Remove "Operation" suffix
        return name


class OperationRegistry:
    """Registry for all available operation types.

    This class uses the registry pattern to map operation types (from protobuf)
    to their concrete Operation implementations. New operations can be registered
    at module initialization time.

    Usage:
        # Register an operation (typically done at module level)
        OperationRegistry.register(tp.OperationType.READ, ReadOperation)

        # Create an operation instance
        operation = OperationRegistry.create(
            tp.OperationType.READ,
            table_name="users"
        )

        # Execute the operation
        operation.execute(worker_context)
    """

    _operations: Dict[tp.OperationType, Type[Operation]] = {}

    @classmethod
    def register(
        cls, op_type: tp.OperationType, op_class: Type[Operation]
    ) -> None:
        """Register an operation class for a given operation type.

        Args:
            op_type: The OperationType enum value from task2.proto
            op_class: The Operation subclass to instantiate for this type

        Example:
            OperationRegistry.register(
                tp.OperationType.READ,
                ReadOperation
            )
        """
        cls._operations[op_type] = op_class

    @classmethod
    def create(cls, op_type: tp.OperationType, **kwargs) -> Operation:
        """Factory method to create operation instances.

        Args:
            op_type: The OperationType enum value to create
            **kwargs: Arguments to pass to the operation constructor

        Returns:
            An instance of the registered Operation subclass

        Raises:
            ValueError: If the operation type is not registered

        Example:
            op = OperationRegistry.create(
                tp.OperationType.READ,
                table_name="orders",
                config=range_config
            )
        """
        op_class = cls._operations.get(op_type)
        if not op_class:
            raise ValueError(
                f"Operation type {tp.OperationType.Name(op_type)} "
                f"not registered. Available operations: "
                f"{[tp.OperationType.Name(t) for t in cls.get_all_operations()]}"
            )
        return op_class(**kwargs)

    @classmethod
    def get_all_operations(cls) -> list[tp.OperationType]:
        """Get list of all registered operation types.

        Returns:
            List of OperationType enum values that have been registered
        """
        return list(cls._operations.keys())

    @classmethod
    def is_registered(cls, op_type: tp.OperationType) -> bool:
        """Check if an operation type is registered.

        Args:
            op_type: The OperationType to check

        Returns:
            True if the operation type is registered, False otherwise
        """
        return op_type in cls._operations

    @classmethod
    def get_operation_class(cls, op_type: tp.OperationType) -> Optional[Type[Operation]]:
        """Get the operation class for a given type without instantiating.

        Args:
            op_type: The OperationType to look up

        Returns:
            The Operation subclass, or None if not registered
        """
        return cls._operations.get(op_type)
