
from flask import Flask, request, jsonify, Response
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os

import pika


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



# RabbitMQ config
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_QUEUE = 'image_jobs'

def submit_job_to_queue(object_name):
    try:
        print(f"[DEBUG] Preparing to publish job: object_name={object_name} type={type(object_name)}")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=object_name.encode(),
            properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
        )
        print(f"[DEBUG] Published job to queue {RABBITMQ_QUEUE}: {object_name}")
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
    submit_job_to_queue(object_name)
    return jsonify({'message': 'Image received, job submitted for processing.'}), 202


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


if __name__ == '__main__':
    app.run(debug=True)
