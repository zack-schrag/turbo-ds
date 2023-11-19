import json
import uuid
from collections import deque
from typing import Any

import boto3
from botocore.exceptions import ClientError

from turbo_ds.exceptions import QueueDoesNotExist


class SqsQueue(deque[Any]):
    def __init__(self, queue_name: str) -> None:
        super().__init__()
        self.queue_name = queue_name
        self._client = boto3.client("sqs")
        self._message_group_id = str(uuid.uuid4())

        try:
            response = self._client.get_queue_url(QueueName=queue_name)
            self._queue_url = response["QueueUrl"]
        except ClientError as error:
            self.__handle_client_error(error)

    def append(self, __x: Any) -> None:
        message_body = {"type": str(type(__x)), "data": __x}

        try:
            self._client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(message_body),
                MessageGroupId=self._message_group_id,
                MessageDeduplicationId=str(uuid.uuid4()),
            )
        except ClientError as error:
            self.__handle_client_error(error)

    def pop(self) -> Any:
        try:
            response = self._client.receive_message(
                QueueUrl=self._queue_url, MaxNumberOfMessages=1
            )

            if not response.get("Messages"):
                raise IndexError()

            message = response["Messages"][0]
            message_json = json.loads(message.get("Body", "{}"))
            self._client.delete_message(
                QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            return message_json["data"]
        except ClientError as error:
            self.__handle_client_error(error)

    def __handle_client_error(self, error: ClientError) -> None:
        if (
            error.response.get("Error", {}).get("Code")
            == "AWS.SimpleQueueService.NonExistentQueue"
        ):
            raise QueueDoesNotExist(f"Queue {self.queue_name} does not exist.")
        else:
            raise error
