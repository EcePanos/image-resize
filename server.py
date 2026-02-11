from datetime import timedelta
from io import BytesIO
import json
import os
import uuid

import pika
from flask import Flask, request, jsonify, Response
from minio import Minio
from minio.error import S3Error

from db import init_db, create_job, get_job


app = Flask(__name__)

# MinIO config
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
UPLOAD_BUCKET = 'uploads'
RESIZED_BUCKET = 'resized'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure buckets exist
for bucket in [UPLOAD_BUCKET, RESIZED_BUCKET]:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

# Initialise database schema
try:
    init_db()
except Exception as e:
    print(f"[WARN] Failed to initialise database: {e}")


# RabbitMQ config
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_QUEUE = 'image_jobs'


def submit_job_to_queue(job_id: str, bucket_name: str, object_name: str):
    """Send a job message to the existing RabbitMQ queue."""
    payload = {
        "job_id": job_id,
        "bucket": bucket_name,
        "object_name": object_name,
    }
    body = json.dumps(payload).encode("utf-8")
    try:
        print(f"[DEBUG] Preparing to publish job: {payload}")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
        )
        print(f"[DEBUG] Published job to queue {RABBITMQ_QUEUE}: {payload}")
        connection.close()
    except Exception as e:
        print(f"Failed to submit job to queue: {e}")

@app.route('/api/upload', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return jsonify({'error': 'No image part'}), 400
    file = request.files['image']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    object_name = file.filename
    file_stream = BytesIO(file.read())
    file_stream.seek(0)
    minio_client.put_object(
        UPLOAD_BUCKET,
        object_name,
        file_stream,
        length=file_stream.getbuffer().nbytes,
        content_type=file.mimetype
    )
    # Create DB job in 'pending' state (best-effort) and enqueue the job.
    job_id = None
    try:
        job_id = create_job(filename=object_name, original_filename=file.filename)
    except Exception as e:
        # Fall back to a generated id so the queue can still carry a job id
        job_id = str(uuid.uuid4())
        print(f"[WARN] Failed to create DB job for upload, using ad-hoc id {job_id}: {e}")

    # Enqueue job for processing via existing RabbitMQ queue (best-effort).
    try:
        submit_job_to_queue(job_id, UPLOAD_BUCKET, object_name)
    except Exception as e:
        print(f"[WARN] Failed to enqueue job {job_id} for {object_name}: {e}")
    return jsonify(
        {
            'message': 'Image received, job submitted for processing.',
            'job_id': job_id,
            'status': 'pending',
            'object_name': object_name,
        }
    ), 202


# Serve resized images from MinIO
@app.route('/api/resized/<filename>')
def resized_file(filename):
    try:
        data = minio_client.get_object(RESIZED_BUCKET, filename)
        return Response(data.read(), mimetype="image/jpeg")
    except S3Error:
        return jsonify({'error': 'Image not found'}), 404


# List resized images from MinIO
@app.route('/api/resized', methods=['GET'])
def list_resized_images():
    objects = minio_client.list_objects(RESIZED_BUCKET)
    images = [obj.object_name for obj in objects if obj.object_name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    return jsonify({'images': images})

# Generate a presigned URL for uploading to MinIO
@app.route('/api/presigned-upload', methods=['POST'])
def presigned_upload():
    data = request.get_json()
    if not data or 'filename' not in data:
        return jsonify({'error': 'Missing filename'}), 400
    filename = data['filename']
    content_type = data.get('content_type', 'application/octet-stream')

    # First, generate the presigned URL. If this fails, the client truly
    # cannot upload, so we return an error.
    try:
        url = minio_client.presigned_put_object(
            UPLOAD_BUCKET,
            filename,
            expires=timedelta(minutes=10)
        )
        # Patch the URL to use nginx /minio/ path for external access
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(url)
        # Replace scheme and netloc with nginx endpoint, and prefix path with /minio
        external_base = os.environ.get('MINIO_EXTERNAL_BASE', 'http://localhost:8080/minio')
        ext_parsed = urlparse(external_base)
        # Remove leading slash from parsed.path to avoid double slashes
        minio_path = parsed.path.lstrip('/')
        new_path = ext_parsed.path.rstrip('/') + '/' + minio_path
        new_url = urlunparse((ext_parsed.scheme, ext_parsed.netloc, new_path, parsed.params, parsed.query, parsed.fragment))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Then, best-effort create a DB job and enqueue it. If this fails we
    # still return a working upload URL so the frontend UX is not broken.
    job_id = None
    try:
        job_id = create_job(filename=filename, original_filename=filename)
    except Exception as e:
        job_id = str(uuid.uuid4())
        print(f"[WARN] Failed to create DB job for presigned upload, using ad-hoc id {job_id}: {e}")

    try:
        submit_job_to_queue(job_id, UPLOAD_BUCKET, filename)
    except Exception as e:
        print(f"[WARN] Failed to enqueue job {job_id} for presigned upload {filename}: {e}")

    response_payload = {
        'url': new_url,
        'method': 'PUT',
        'headers': {'Content-Type': content_type},
        'object_name': filename,
    }
    if job_id:
        response_payload.update(
            {
                'job_id': job_id,
                'status': 'pending',
            }
        )
    return jsonify(response_payload)


@app.route('/api/jobs/<job_id>', methods=['GET'])
def job_status(job_id):
    """Return the current status and metadata for an image job."""
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)


if __name__ == '__main__':
    app.run(debug=True)
