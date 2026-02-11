import json
import os
import time
import socket
from urllib.parse import unquote

import pika
from minio import Minio
from minio.error import S3Error
from PIL import Image

from db import init_db, update_job_status


RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
MINIO_ROOT_USER = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
MINIO_ROOT_PASSWORD = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')

UPLOADS_DIR = './uploads'
RESIZED_DIR = './resized'
BUCKET_NAME = 'resized'

# Connect to MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False
)


def ensure_bucket():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)


def resize_image(image_path, output_path, size=(256, 256)):
    with Image.open(image_path) as img:
        img = img.resize(size)
        img.save(output_path)


def safe_update_job_status(job_id: str, status: str, error_message: str | None = None):
    """Best-effort status update that never aborts job processing."""
    try:
        update_job_status(job_id, status, error_message=error_message)
    except Exception as e:
        print(f"[WARN] Failed to update job {job_id} to '{status}': {e}")


def extract_job_id_from_key(object_key: str) -> str | None:
    """
    Our server stores objects as '<job_id>_<original-filename>'.
    This helper extracts the job_id part for status tracking.
    """
    base = os.path.basename(object_key)
    if '_' not in base:
        return None
    job_id, _rest = base.split('_', 1)
    return job_id or None


def process_job(ch, method, properties, body):
    try:
        event = json.loads(body.decode())
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']
        # MinIO/S3 events URL-encode object keys; decode before use
        filename = unquote(raw_key)

        job_id = extract_job_id_from_key(filename)
        print(f"Processing MinIO event for {filename} in bucket {bucket_name}, job_id={job_id}")

        # Mark job as in progress (best-effort)
        if job_id:
            safe_update_job_status(job_id, "in_progress")

        input_path = os.path.join(UPLOADS_DIR, os.path.basename(filename))
        output_path = os.path.join(RESIZED_DIR, os.path.basename(filename))

        # Download the image from MinIO, with a few retries to give the
        # client time to finish the presigned upload.
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                minio_client.fget_object(bucket_name, filename, input_path)
                print(f"Successfully downloaded {filename} from MinIO on attempt {attempt}.")
                break
            except S3Error as e:
                # If the object is not yet there, wait a bit and retry.
                if e.code == "NoSuchKey" and attempt < max_attempts:
                    wait_seconds = 2 * attempt
                    print(f"{filename} not found yet (attempt {attempt}), retrying in {wait_seconds}s...")
                    time.sleep(wait_seconds)
                    continue
                # Any other error or final failed attempt: re-raise.
                raise
        # Print file size for debugging
        try:
            file_size = os.path.getsize(input_path)
            print(f"Downloaded file size: {file_size} bytes")
        except Exception as e:
            print(f"Error checking file size: {e}")

        # Resize the image
        resize_image(input_path, output_path)
        print(f"Successfully resized {filename}.")

        # Ensure the 'resized-images' bucket exists
        ensure_bucket()

        # Upload the resized image to the 'resized-images' bucket
        minio_client.fput_object(BUCKET_NAME, os.path.basename(filename), output_path)
        print(f"Successfully uploaded resized {filename} to MinIO.")

        # Mark job as completed (best-effort)
        if job_id:
            safe_update_job_status(job_id, "completed")

    except Exception as e:
        print(f"Error processing MinIO event: {e}")
        # Best-effort: try to recover job_id from the event again
        try:
            event = json.loads(body.decode())
            record = event['Records'][0]
            raw_key = record['s3']['object']['key']
            filename = unquote(raw_key)
            job_id = extract_job_id_from_key(filename)
            if job_id:
                safe_update_job_status(job_id, "error", error_message=str(e))
        except Exception as inner:
            print(f"Additionally failed to update job status: {inner}")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


def check_connection(host, port):
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"Successfully connected to {host}:{port}")
            return True
    except OSError as err:
        print(f"Failed to connect to {host}:{port}: {err}")
        return False


def main():
    # Ensure DB schema exists
    try:
        init_db()
    except Exception as e:
        print(f"[WARN] Failed to initialise database in image-processor: {e}")

    # Check connections before starting
    check_connection(RABBITMQ_HOST, 5672)
    check_connection(MINIO_ENDPOINT.split(':')[0], int(MINIO_ENDPOINT.split(':')[1]))

    time.sleep(10)
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            # Declare the queue and bind to minio-events exchange
            queue_name = 'minio_events_queue'
            channel.queue_declare(queue=queue_name, durable=True)
            channel.exchange_declare(exchange='minio-events', exchange_type='fanout', durable=True)
            channel.queue_bind(queue=queue_name, exchange='minio-events', routing_key='minio.uploaded')
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue_name, on_message_callback=process_job)
            print('Waiting for MinIO event jobs...')
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error: {e}, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}, retrying in 5s...")
            time.sleep(5)


if __name__ == '__main__':
    main()
