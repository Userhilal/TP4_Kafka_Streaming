import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ventes_temps_reel"
PRODUITS = ["Smartphone", "Tablette", "Casque", "Chargeur"]
REGIONS = ["Nord", "Sud", "Est", "Ouest"]


def build_transaction(transaction_id: int) -> dict:
    prix = round(random.uniform(10.0, 1000.0), 2)
    quantite = random.randint(1, 4)
    return {
        "transaction_id": transaction_id,
        "produit": random.choice(PRODUITS),
        "categorie": "Electronique",
        "region": random.choice(REGIONS),
        "quantite": quantite,
        "prix": prix,
        "montant_total": round(prix * quantite, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Producteur Kafka pour le TP4 streaming.")
    parser.add_argument("--messages", type=int, default=20, help="Nombre de messages à envoyer. 0 = infini.")
    parser.add_argument("--interval", type=float, default=2.0, help="Délai entre deux messages en secondes.")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Début de l'envoi des messages streaming...")
    i = 1
    try:
        while args.messages == 0 or i <= args.messages:
            data = build_transaction(i)
            producer.send(TOPIC_NAME, value=data)
            producer.flush()
            print(f"Envoyé : {data}")
            i += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Arrêt du producteur.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
