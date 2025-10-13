import sys
from tasks import DatabaseTask
from microbench.timer import Timer

# TODO: Add automatic db setup and teardown.

if __name__ == "__main__":
    backend = sys.argv[1]
    supported_backends = ["dolt"]
    if backend not in supported_backends:
        print("Supported backend list: ", supported_backends)
        sys.exit(1)

    timer = Timer()
    task = DatabaseTask(backend, timer)
    for idx in range(10):
        task.branch(f"test_branch_{idx}")
    print("Total time:", task.report_total())
