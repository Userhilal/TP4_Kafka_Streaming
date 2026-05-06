import subprocess
import time
from minio import Minio
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "real-time"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ventes_temps_reel"


def wait_for_kafka(timeout_seconds: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_seconds:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id="tp4-admin-check")
            admin.close()
            print("Kafka est prêt.")
            return
        except NoBrokersAvailable:
            print("Kafka pas encore prêt, attente 5 secondes...")
            time.sleep(5)
    raise RuntimeError("Kafka n'est pas disponible après l'attente.")


def create_minio_bucket() -> None:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket MinIO créé : {BUCKET_NAME}")
    else:
        print(f"Bucket MinIO existe déjà : {BUCKET_NAME}")


def create_kafka_topic() -> None:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id="tp4-admin")
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    try:
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic Kafka créé : {TOPIC_NAME}")
    except TopicAlreadyExistsError:
        print(f"Topic Kafka existe déjà : {TOPIC_NAME}")
    finally:
        admin.close()


def show_topics_with_docker() -> None:
    try:
        result = subprocess.run(
            [
                "docker", "exec", "kafka_broker", "kafka-topics",
                "--list", "--bootstrap-server", "localhost:9092",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        print("Topics disponibles dans Kafka :")
        print(result.stdout.strip())
    except Exception as exc:
        print(f"Impossible d'afficher les topics via Docker : {exc}")


if __name__ == "__main__":
    wait_for_kafka()
    create_minio_bucket()
    create_kafka_topic()
    show_topics_with_docker()
