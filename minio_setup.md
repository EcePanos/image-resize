# MinIO Setup for Local Object Storage

- MinIO is now included in your docker-compose setup.
- Access the MinIO Console at http://localhost:9001 (user: minioadmin, password: minioadmin).
- S3 API is available at http://minio:9000 from other containers, or http://localhost:9000 from your host.
- You should create two buckets: `uploads` and `resized` for your app.

## Next Steps
- Update your Flask backend to use MinIO for storing and retrieving files instead of local disk.
- Use the `minio` Python package for integration: https://github.com/minio/minio-py
- Example connection:

```python
from minio import Minio
minio_client = Minio(
    'minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)
```
