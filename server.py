
from flask import Flask, request, jsonify, Response
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from threading import Thread
from PIL import Image


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


# Async resize function for MinIO
def resize_image_async(object_name):
    try:
        # Download original image from MinIO
        data = minio_client.get_object(UPLOAD_BUCKET, object_name)
        img = Image.open(BytesIO(data.read()))
        img.thumbnail((400, 300), Image.LANCZOS)
        img = img.convert("RGB")
        buf = BytesIO()
        img.save(buf, format="JPEG")
        buf.seek(0)
        # Save resized image to MinIO
        resized_name = f"resized_{object_name.rsplit('.', 1)[0]}.jpg"
        minio_client.put_object(
            RESIZED_BUCKET,
            resized_name,
            buf,
            length=buf.getbuffer().nbytes,
            content_type="image/jpeg"
        )
    except Exception as e:
        print(f"Resize error: {e}")

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
    Thread(target=resize_image_async, args=(object_name,)).start()
    return jsonify({'message': 'Image received, resizing in background.'}), 202


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
