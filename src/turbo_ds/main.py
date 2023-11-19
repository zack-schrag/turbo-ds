import sys
import time

from turbo_ds.sqs import SqsQueue

if __name__ == "__main__":
    q = SqsQueue("demo2", create_if_not_exists=True)

    if sys.argv[1] == "p":
        q.extend([1, 2, 3, 4, 5, 100])
        # i = 0
        # while True:
        #     q.append(i * 100)
        #     i += 1
        #     time.sleep(1)

    elif sys.argv[1] == "c":
        while True:
            try:
                print(q.popleft())
            except IndexError as e:
                print(str(e))
            time.sleep(0.5)
