import argparse
import json
import time
from io import BytesIO
from kafka import KafkaConsumer
from minio import Minio

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "real-time"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ventes_temps_reel"


def upload_batch(client: Minio, buffer: list, batch_index: int) -> None:
    timestamp = int(time.time())
    filename = f"ventes_stream/batch_{timestamp}_{batch_index}.json"
    json_data = json.dumps(buffer, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        BUCKET_NAME,
        filename,
        BytesIO(json_data),
        length=len(json_data),
        content_type="application/json",
    )
    print(f">>> Succès : {len(buffer)} messages sauvegardés dans {filename}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Consommateur Kafka vers MinIO avec micro-batching.")
    parser.add_argument("--batch-size", type=int, default=5, help="Nombre de messages par fichier JSON.")
    parser.add_argument("--max-batches", type=int, default=3, help="Nombre de lots à sauvegarder. 0 = infini.")
    args = parser.parse_args()

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket créé : {BUCKET_NAME}")

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"minio-sink-group-{int(time.time())}",
    )

    print(f"Connecté à Kafka topic={TOPIC_NAME}. En attente de messages...")
    buffer = []
    batch_count = 0

    try:
        for message in consumer:
            data = message.value
            buffer.append(data)
            print(f"Message reçu : {data['produit']} - {data['prix']}€")

            if len(buffer) >= args.batch_size:
                batch_count += 1
                upload_batch(client, buffer, batch_count)
                buffer = []

                if args.max_batches and batch_count >= args.max_batches:
                    print("Nombre maximum de lots atteint. Fin du consommateur.")
                    break
    except KeyboardInterrupt:
        print("\nArrêt du consommateur demandé.")
        if buffer:
            batch_count += 1
            upload_batch(client, buffer, batch_count)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
