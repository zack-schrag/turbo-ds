from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from turbo_ds.exceptions import QueueDoesNotExist
from turbo_ds.sqs import SqsQueue


# Constructor tests
@patch("turbo_ds.sqs.boto3.client")
def test_constructor_queue_does_not_exist(mock_sqs_client):
    error = ClientError(
        error_response={"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue"}},
        operation_name="foo bar",
    )
    mock_sqs_client.return_value.get_queue_url = MagicMock(side_effect=error)

    with pytest.raises(QueueDoesNotExist) as e:
        SqsQueue("foo bar")
