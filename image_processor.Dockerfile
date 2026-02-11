# Image Processor Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY image_processor.py db.py ./
RUN pip install pillow minio pika psycopg2-binary
CMD ["python", "-u", "image_processor.py"]
