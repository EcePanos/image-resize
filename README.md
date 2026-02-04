# image-resize
Overengineering an image-resizer service to experiment with some tools

Setup notifications in MinIO using RabbitMQ and AMQP:

```
mc alias set minio http://localhost:9000 minioadmin minioadmin
mc event add minio/uploads arn:minio:sqs::1:amqp --event put
```
