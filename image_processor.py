import pika
import os
import time
from minio import Minio
from PIL import Image

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

def process_job(ch, method, properties, body):
    filename = body.decode()
    print(f"Processing job for {filename}")

    # Create temporary paths for processing
    input_path = os.path.join(UPLOADS_DIR, filename)
    output_path = os.path.join(RESIZED_DIR, filename)

    try:
        # Download the image from MinIO's 'uploads' bucket
        minio_client.fget_object('uploads', filename, input_path)
        print(f"Successfully downloaded {filename} from MinIO.")

        # Resize the image
        resize_image(input_path, output_path)
        print(f"Successfully resized {filename}.")

        # Ensure the 'resized-images' bucket exists
        ensure_bucket()

        # Upload the resized image to the 'resized-images' bucket
        minio_client.fput_object(BUCKET_NAME, filename, output_path)
        print(f"Successfully uploaded resized {filename} to MinIO.")

    except Exception as e:
        print(f"Error processing {filename}: {e}")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

import socket

def check_connection(host, port):
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"Successfully connected to {host}:{port}")
            return True
    except OSError as err:
        print(f"Failed to connect to {host}:{port}: {err}")
        return False

def main():
    # Check connections before starting
    check_connection(RABBITMQ_HOST, 5672)
    check_connection(MINIO_ENDPOINT.split(':')[0], int(MINIO_ENDPOINT.split(':')[1]))

    time.sleep(10)
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='image_jobs', durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='image_jobs', on_message_callback=process_job)
            print('Waiting for image jobs...')
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error: {e}, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}, retrying in 5s...")
            time.sleep(5)

if __name__ == '__main__':
    main()
