import sys
import time

from turbo_ds.sqs import SqsQueue

if __name__ == "__main__":
    q = SqsQueue("demo.fifo")

    if sys.argv[1] == "p":
        i = 0
        while True:
            q.append(i)
            i += 1
            time.sleep(1)

    elif sys.argv[1] == "c":
        while True:
            try:
                print(q.pop())
            except IndexError as e:
                print(str(e))
            time.sleep(2)
