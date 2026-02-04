# Image Processor Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY image_processor.py ./
RUN pip install pillow minio pika
CMD ["python", "-u", "image_processor.py"]
