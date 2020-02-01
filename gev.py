import time

import gevent

from gevent import monkey;

monkey.patch_all()

x = []
y = []


def foo():
    x = []
    for i in range(10000000):
        for j in range(1000000):
            for k in range(1000000):
                pass


def display_message():
    for i in range(10000000):
        pass
    print("displaying message")


s = time.time()
greenlet = gevent.spawn(foo)
for _ in range(5):
    display_message()
# ... perhaps interaction with the user here

# this will wait for the operation to complete (optional)
greenlet.join()
print("done"
      "")
e = time.time()
print(e - s)

s = time.time()
foo()
for _ in range(10):
    display_message()

e = time.time()
print(e - s)
