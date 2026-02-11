from datetime import timedelta
from io import BytesIO
import os
import uuid

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

# Allow tests (and some environments) to skip external initialisation
SKIP_EXTERNAL_INIT = os.environ.get("SKIP_EXTERNAL_INIT") == "1"

if not SKIP_EXTERNAL_INIT:
    # Ensure buckets exist
    for bucket in [UPLOAD_BUCKET, RESIZED_BUCKET]:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)

    # Initialise database schema
    try:
        init_db()
    except Exception as e:
        print(f"[WARN] Failed to initialise database: {e}")


@app.route('/api/upload', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return jsonify({'error': 'No image part'}), 400
    file = request.files['image']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    original_filename = file.filename
    # Generate a job id and embed it into the object name so the
    # image-processor (driven by MinIO events) can recover it later.
    job_id = str(uuid.uuid4())
    object_name = f"{job_id}_{original_filename}"
    file_stream = BytesIO(file.read())
    file_stream.seek(0)
    minio_client.put_object(
        UPLOAD_BUCKET,
        object_name,
        file_stream,
        length=file_stream.getbuffer().nbytes,
        content_type=file.mimetype
    )
    # Record the job as pending in the DB. The MinIO event will drive
    # the actual processing and status transitions.
    create_job(filename=object_name, original_filename=original_filename, job_id=job_id)
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
    original_filename = data['filename']
    content_type = data.get('content_type', 'application/octet-stream')

    # First, generate the presigned URL. If this fails, the client truly
    # cannot upload, so we return an error.
    try:
        # Generate a job id and embed it into the object name so we can
        # recover it from the MinIO event later.
        job_id = str(uuid.uuid4())
        object_name = f"{job_id}_{original_filename}"

        url = minio_client.presigned_put_object(
            UPLOAD_BUCKET,
            object_name,
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

    # Record the pending job linked to this object. The MinIO event
    # (consumed by the image-processor) will drive status updates.
    create_job(filename=object_name, original_filename=original_filename, job_id=job_id)

    return jsonify(
        {
            'url': new_url,
            'method': 'PUT',
            'headers': {'Content-Type': content_type},
            'job_id': job_id,
            'status': 'pending',
            'object_name': object_name,
        }
    )


@app.route('/api/jobs/<job_id>', methods=['GET'])
def job_status(job_id):
    """Return the current status and metadata for an image job."""
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)


if __name__ == '__main__':
    app.run(debug=True)
