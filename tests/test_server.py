import io
import uuid

import server


def test_presigned_upload_generates_job_and_object_name(monkeypatch):
    app = server.app
    app.testing = True
    client = app.test_client()

    fixed_uuid = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")

    def fake_uuid4():
        return fixed_uuid

    created_jobs = []

    def fake_create_job(filename, original_filename=None, job_id=None):
        created_jobs.append(
            {
                "filename": filename,
                "original_filename": original_filename,
                "job_id": job_id,
            }
        )
        return job_id

    def fake_presigned_put_object(bucket, object_name, expires):
        # Return a simple MinIO-style URL
        return f"http://minio:9000/{bucket}/{object_name}"

    monkeypatch.setattr(server, "create_job", fake_create_job)
    monkeypatch.setattr(server.minio_client, "presigned_put_object", fake_presigned_put_object)
    monkeypatch.setattr(server.uuid, "uuid4", fake_uuid4)

    resp = client.post(
        "/api/presigned-upload",
        json={"filename": "example.png", "content_type": "image/png"},
    )
    assert resp.status_code == 200
    data = resp.get_json()

    expected_job_id = str(fixed_uuid)
    expected_object_name = f"{expected_job_id}_example.png"

    assert data["job_id"] == expected_job_id
    assert data["object_name"] == expected_object_name
    assert "url" in data and expected_object_name in data["url"]

    # Ensure a job was recorded using the same identifiers
    assert created_jobs == [
        {
            "filename": expected_object_name,
            "original_filename": "example.png",
            "job_id": expected_job_id,
        }
    ]


def test_upload_image_creates_job_and_stores_in_minio(monkeypatch):
    app = server.app
    app.testing = True
    client = app.test_client()

    fixed_uuid = uuid.UUID("123e4567-e89b-12d3-a456-426614174001")

    def fake_uuid4():
        return fixed_uuid

    created_jobs = []

    def fake_create_job(filename, original_filename=None, job_id=None):
        created_jobs.append(
            {
                "filename": filename,
                "original_filename": original_filename,
                "job_id": job_id,
            }
        )
        return job_id

    stored_objects = []

    def fake_put_object(bucket, object_name, data, length, content_type):
        stored_objects.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "length": length,
                "content_type": content_type,
            }
        )

    monkeypatch.setattr(server, "create_job", fake_create_job)
    monkeypatch.setattr(server.minio_client, "put_object", fake_put_object)
    monkeypatch.setattr(server.uuid, "uuid4", fake_uuid4)

    data = {
        "image": (io.BytesIO(b"fake-image-bytes"), "photo.jpg"),
    }
    resp = client.post(
        "/api/upload",
        data=data,
        content_type="multipart/form-data",
    )

    assert resp.status_code == 202
    body = resp.get_json()

    expected_job_id = str(fixed_uuid)
    expected_object_name = f"{expected_job_id}_photo.jpg"

    assert body["job_id"] == expected_job_id
    assert body["object_name"] == expected_object_name

    # MinIO put_object should have been called with the derived key
    assert stored_objects[0]["bucket"] == server.UPLOAD_BUCKET
    assert stored_objects[0]["object_name"] == expected_object_name

    # DB job creation should reflect the same identifiers
    assert created_jobs == [
        {
            "filename": expected_object_name,
            "original_filename": "photo.jpg",
            "job_id": expected_job_id,
        }
    ]


def test_job_status_404_when_missing(monkeypatch):
    app = server.app
    app.testing = True
    client = app.test_client()

    def fake_get_job(job_id):
        return None

    monkeypatch.setattr(server, "get_job", fake_get_job)

    resp = client.get("/api/jobs/nonexistent-id")
    assert resp.status_code == 404
    assert resp.get_json()["error"] == "Job not found"

