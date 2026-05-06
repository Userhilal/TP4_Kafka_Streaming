# TP4 - Ingestion Streaming avec Apache Kafka

Ce projet réalise un pipeline streaming :

```text
Producteur Python -> Kafka topic ventes_temps_reel -> Consommateur micro-batch -> MinIO bucket real-time
```

## 1. Installation

```bash
python -m venv venv_tp4
source venv_tp4/bin/activate
pip install -r requirements.txt
```

## 2. Lancer Kafka et MinIO

```bash
docker compose up -d
docker ps
```

MinIO : http://localhost:9001  
Login : `minioadmin`  
Password : `minioadmin`

## 3. Créer le bucket MinIO et le topic Kafka

```bash
python scripts/00_setup_kafka_minio.py
```

Le script crée :

- bucket MinIO : `real-time`
- topic Kafka : `ventes_temps_reel`

## 4. Lancer le consommateur

Terminal 1 :

```bash
python scripts/02_consumer_to_minio.py --batch-size 5 --max-batches 3
```

## 5. Lancer le producteur

Terminal 2 :

```bash
python scripts/01_producer_ventes.py --messages 20 --interval 2
```

## 6. Vérifier dans MinIO

```bash
python scripts/03_check_minio.py
```

Dans MinIO, ouvrir :

```text
real-time/ventes_stream/
```

Vous devez voir des fichiers JSON générés automatiquement par lots de 5 messages.

## 7. Commande de vérification Kafka console

```bash
docker exec -it kafka_broker kafka-console-consumer \
  --topic ventes_temps_reel \
  --from-beginning \
  --bootstrap-server localhost:9092
```
