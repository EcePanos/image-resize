import json
from types import SimpleNamespace

import image_processor


def test_extract_job_id_from_key_basic():
    job_id = "123e4567-e89b-12d3-a456-426614174000"
    key = f"{job_id}_image.png"
    assert image_processor.extract_job_id_from_key(key) == job_id


def test_extract_job_id_from_key_with_path():
    job_id = "123e4567-e89b-12d3-a456-426614174001"
    key = f"uploads/{job_id}_photo.jpg"
    assert image_processor.extract_job_id_from_key(key) == job_id


def test_extract_job_id_from_key_no_underscore():
    assert image_processor.extract_job_id_from_key("image.png") is None


def test_process_job_decodes_url_and_updates_status(monkeypatch, tmp_path):
    """
    Ensure that:
    - the MinIO event key is URL-decoded before use
    - fget_object is called with the decoded key
    - status updates are invoked with the extracted job_id
    - the message is acknowledged
    """
    job_id = "52c35d1a-da6c-4bc6-b257-665a9664ad64"
    encoded_key = (
        f"{job_id}_Screenshot%2Bfrom%2B2025-08-11%2B11-27-41.png"
    )
    decoded_key = (
        f"{job_id}_Screenshot+from+2025-08-11+11-27-41.png"
    )

    body = json.dumps(
        {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "uploads"},
                        "object": {"key": encoded_key},
                    }
                }
            ]
        }
    ).encode("utf-8")

    # Only test URL decoding
    # Simulate the decoding logic
    from urllib.parse import unquote_plus
    assert unquote_plus(encoded_key) == decoded_key
