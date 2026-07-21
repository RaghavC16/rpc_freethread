"""Run a small nogil_rpc server."""

from nogil_rpc import RpcRuntime, remote


@remote
def add(a, b):
    return a + b


def main() -> None:
    runtime = RpcRuntime(host="127.0.0.1", port=50051)
    runtime.start()
    print("listening on 127.0.0.1:50051")
    runtime.wait()


if __name__ == "__main__":
    main()
