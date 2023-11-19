# turbo-ds
Familiar Python data structures backed by cloud services

# Queues
## AWS SQS
```python
my_queue = SqsQueue("my_queue", create_if_not_exists=True)
```
Send a message to the queue with any data, turbo automatically handles JSON serialization for you:
```python
my_queue.append("foo bar")
```

Consume a message:
```python
print(my_queue.popleft()) # "foo bar"
```
