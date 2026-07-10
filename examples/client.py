"""Call the example nogil_rpc server."""

from nogil_rpc import connect
import time

def main() -> None:
    worker = connect("127.0.0.1:50051")
    try:
        refs = []
        for i in range(10):
            refs.append(worker.add.remote(2, i))
        
        time.sleep(1)
        for i in range(9, -1, -1):
            print(refs[i].get())
        
    finally:
        worker.close()


if __name__ == "__main__":
    main()
