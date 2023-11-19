import json
import uuid
from collections import deque
from collections.abc import Iterable
from typing import Any, Self

import boto3
from botocore.exceptions import ClientError

from turbo_ds.exceptions import QueueDoesNotExist


class SqsQueue(deque[Any]):
    def __init__(self, queue_name: str, create_if_not_exists=False) -> None:
        super().__init__()
        self.create_if_not_exists = create_if_not_exists
        self.queue_name = queue_name
        self._client = boto3.client("sqs")
        self._message_group_id = str(uuid.uuid4())

        try:
            response = self._client.get_queue_url(QueueName=queue_name)
            self._queue_url = response["QueueUrl"]
        except ClientError as error:
            if (
                error.response.get("Error", {}).get("Code")
                == "AWS.SimpleQueueService.NonExistentQueue"
            ):
                if create_if_not_exists:
                    try:
                        r = self._client.create_queue(QueueName=self.queue_name)
                        self._queue_url = r["QueueUrl"]
                    except ClientError as e:
                        self.__handle_client_error(e)
                else:
                    raise QueueDoesNotExist(f"Queue {self.queue_name} does not exist.")

        try:
            qr = self._client.get_queue_attributes(
                QueueUrl=self._queue_url, AttributeNames=["All"]
            )
            self.is_fifo = qr["Attributes"].get("FifoQueue")
        except ClientError as e:
            self.__handle_client_error(e)

    def append(self, __x: Any) -> None:
        message_body = {"type": str(type(__x)), "data": __x}

        try:
            if self.is_fifo:
                self._client.send_message(
                    QueueUrl=self._queue_url,
                    MessageBody=json.dumps(message_body),
                    MessageGroupId=self._message_group_id,
                    MessageDeduplicationId=str(uuid.uuid4()),
                )
            else:
                self._client.send_message(
                    QueueUrl=self._queue_url, MessageBody=json.dumps(message_body)
                )
        except ClientError as error:
            self.__handle_client_error(error)

    def appendleft(self, __x: Any) -> None:
        raise NotImplementedError()

    def clear(self) -> None:
        try:
            self._client.purge_queue(QueueUrl=self._queue_url)
        except ClientError as error:
            self.__handle_client_error(error)

    def copy(self) -> Self:
        raise NotImplementedError()

    def count(self, __x: Any) -> int:
        raise NotImplementedError()

    def extend(self, __iterable: Iterable[Any]) -> None:
        entries = []
        for item in __iterable:
            if self.is_fifo:
                entries.append(
                    {
                        "Id": str(uuid.uuid4()),
                        "MessageBody": json.dumps(
                            {"type": str(type(item)), "data": item}
                        ),
                        "MessageGroupId": self._message_group_id,
                        "MessageDeduplicationId": str(uuid.uuid4()),
                    }
                )
            else:
                entries.append(
                    {
                        "Id": str(uuid.uuid4()),
                        "MessageBody": json.dumps(
                            {"type": str(type(item)), "data": item}
                        ),
                    }
                )

        while entries:
            try:
                self._client.send_message_batch(
                    QueueUrl=self._queue_url, Entries=entries[:10]
                )
                entries = entries[10:]
            except ClientError as error:
                self.__handle_client_error(error)

    def extendleft(self, __iterable: Iterable[Any]) -> None:
        raise NotImplementedError()

    def index(self, __x: Any, __start: int = 0, __stop: int = ...) -> int:  # type: ignore
        raise NotImplementedError()

    def insert(self, __i: int, __x: Any) -> None:
        raise NotImplementedError()

    def pop(self) -> Any:
        raise NotImplementedError()

    def popleft(self) -> Any:
        try:
            response = self._client.receive_message(
                QueueUrl=self._queue_url, MaxNumberOfMessages=1
            )

            if not response.get("Messages"):
                raise IndexError(f"pop from an empty queue {self.queue_name}")

            message = response["Messages"][0]
            message_json = json.loads(message.get("Body", "{}"))
            self._client.delete_message(
                QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            return message_json["data"]
        except ClientError as error:
            self.__handle_client_error(error)

    def remove(self, __value: Any) -> None:
        raise NotImplementedError()

    def reverse(self) -> None:
        raise NotImplementedError()

    def rotate(self, __n: int = 1) -> None:
        raise NotImplementedError()

    def maxlen(self) -> int | None:
        return None

    def __len__(self) -> int:
        try:
            response = self._client.get_queue_attributes(
                QueueUrl=self._queue_url, AttributeNames=["ApproximateNumberOfMessages"]
            )

            return int(
                response.get("Attributes", {}).get("ApproximateNumberOfMessages", "0")
            )
        except ClientError as error:
            self.__handle_client_error(error)

        return 0

    def __contains__(self, __key: object) -> bool:
        raise NotImplementedError()

    def __handle_client_error(self, error: ClientError) -> None:
        if (
            error.response.get("Error", {}).get("Code")
            == "AWS.SimpleQueueService.NonExistentQueue"
        ):
            raise QueueDoesNotExist(f"Queue {self.queue_name} does not exist.")
        else:
            raise error
