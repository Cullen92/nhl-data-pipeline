import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from nhl_pipeline.ingestion.s3_utils import put_json_to_s3, s3_key_exists


class TestS3KeyExists:
    """Test suite for s3_key_exists function."""

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_key_exists_returns_true(self, mock_settings, mock_boto_client):
        """Test that s3_key_exists returns True when key exists."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.return_value = {}

        # Act
        result = s3_key_exists(bucket="test-bucket", key="test-key")

        # Assert
        assert result is True
        mock_boto_client.assert_called_once_with("s3", region_name="us-west-2")
        mock_s3.head_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_key_not_found_returns_false(self, mock_settings, mock_boto_client):
        """Test that s3_key_exists returns False for 404 error."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "404"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

        # Act
        result = s3_key_exists(bucket="test-bucket", key="test-key")

        # Assert
        assert result is False

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_no_such_key_returns_false(self, mock_settings, mock_boto_client):
        """Test that s3_key_exists returns False for NoSuchKey error."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

        # Act
        result = s3_key_exists(bucket="test-bucket", key="test-key")

        # Assert
        assert result is False

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_not_found_error_returns_false(self, mock_settings, mock_boto_client):
        """Test that s3_key_exists returns False for NotFound error."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "NotFound"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

        # Act
        result = s3_key_exists(bucket="test-bucket", key="test-key")

        # Assert
        assert result is False

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_permission_error_raises_exception(self, mock_settings, mock_boto_client):
        """Test that s3_key_exists raises PermissionError for 403 error."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "403"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

        # Act & Assert
        with pytest.raises(PermissionError) as exc_info:
            s3_key_exists(bucket="test-bucket", key="test-key")

        assert "S3 HeadObject forbidden" in str(exc_info.value)
        assert "test-bucket" in str(exc_info.value)
        assert "test-key" in str(exc_info.value)

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_other_client_error_raises(self, mock_settings, mock_boto_client):
        """Test that s3_key_exists raises ClientError for other error codes."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "500"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

        # Act & Assert
        with pytest.raises(ClientError):
            s3_key_exists(bucket="test-bucket", key="test-key")


class TestPutJsonToS3:
    """Test suite for put_json_to_s3 function."""

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_successful_upload(self, mock_settings, mock_boto_client):
        """Test successful JSON upload to S3."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        payload = {"key": "value", "number": 123}

        # Act
        result = put_json_to_s3(bucket="test-bucket", key="test-key.json", payload=payload)

        # Assert
        assert result == "s3://test-bucket/test-key.json"
        mock_boto_client.assert_called_once_with("s3", region_name="us-west-2")

        # Verify put_object was called with correct parameters
        assert mock_s3.put_object.call_count == 1
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "test-key.json"
        assert call_kwargs["ContentType"] == "application/json"

        # Verify the body is correctly serialized JSON
        expected_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        assert call_kwargs["Body"] == expected_body

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_upload_with_special_characters(self, mock_settings, mock_boto_client):
        """Test JSON upload with special characters."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        payload = {"name": "Montréal Canadiens", "emoji": "🏒", "quote": 'He said "hello"'}

        # Act
        result = put_json_to_s3(bucket="test-bucket", key="special.json", payload=payload)

        # Assert
        assert result == "s3://test-bucket/special.json"

        # Verify the body is correctly serialized with special characters
        call_kwargs = mock_s3.put_object.call_args[1]
        expected_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        assert call_kwargs["Body"] == expected_body

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_upload_with_nested_structure(self, mock_settings, mock_boto_client):
        """Test JSON upload with nested data structure."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        payload = {
            "game": {"id": 2025020001, "teams": {"home": "NYR", "away": "TOR"}},
            "stats": [{"player": "Player1", "goals": 2}, {"player": "Player2", "goals": 1}],
        }

        # Act
        result = put_json_to_s3(bucket="test-bucket", key="nested.json", payload=payload)

        # Assert
        assert result == "s3://test-bucket/nested.json"

        # Verify the body is correctly serialized
        call_kwargs = mock_s3.put_object.call_args[1]
        expected_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        assert call_kwargs["Body"] == expected_body

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_upload_empty_object(self, mock_settings, mock_boto_client):
        """Test JSON upload with empty object."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        payload = {}

        # Act
        result = put_json_to_s3(bucket="test-bucket", key="empty.json", payload=payload)

        # Assert
        assert result == "s3://test-bucket/empty.json"
        call_kwargs = mock_s3.put_object.call_args[1]
        expected_body = b"{}"
        assert call_kwargs["Body"] == expected_body

    @patch("nhl_pipeline.ingestion.s3_utils.boto3.client")
    @patch("nhl_pipeline.ingestion.s3_utils.get_settings")
    def test_upload_failure_raises_exception(self, mock_settings, mock_boto_client):
        """Test that put_json_to_s3 raises exception when upload fails."""
        # Arrange
        mock_settings.return_value.aws_region = "us-west-2"
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "NoSuchBucket"}}
        mock_s3.put_object.side_effect = ClientError(error_response, "PutObject")
        payload = {"key": "value"}

        # Act & Assert
        with pytest.raises(ClientError):
            put_json_to_s3(bucket="non-existent-bucket", key="test.json", payload=payload)
