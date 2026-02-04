# Backend Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY server.py ./
COPY env/lib/python3.12/site-packages ./site-packages
RUN pip install flask pillow gunicorn minio
ENV FLASK_APP=server:app
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "server:app"]
