import os, json, time
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from services.inference.model.models import get_sentiment_pipeline

def load_env():
    env_path = Path(__file__).resolve().parents[1] / ".env.dev"
    if env_path.exists():
        load_dotenv(env_path)

load_env()

BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC   = os.getenv("RAW_TOPIC", "events.raw")
SCORED_TOPIC= os.getenv("SCORED_TOPIC", "events.scored")
GROUP_ID    = os.getenv("GROUP_ID", "inference-v1")

def main():
    sentiment = get_sentiment_pipeline()

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        key_deserializer=lambda b : b.decode("utf-8") if b else None,
        value_deserializer=lambda b : json.loads(b.decode("utf-8")),
    )

    produce = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f" consuming from {RAW_TOPIC}, producing to {SCORED_TOPIC}")
    for msg in consumer:
        payload = msg.value
        text = payload.get("text", "")
        result = sentiment(text)[0]

        out = {
            "id": payload.get("id"),
            "source": payload.get("source"),
            "text": text,
            "lang": payload.get("lang"),
            "created_at": payload.get("created_at"),
            "meta": {**payload.get("meta", {}), "model": "distilbert-sst2"},
            "sentiment": {"label": result["label"], "score": result["score"]},
        }

        produce.send(SCORED_TOPIC, key=out["id"], value=out)
        print(f"Sent: {out}")
        time.sleep(0.01)

if __name__ == "__main__":
    main()