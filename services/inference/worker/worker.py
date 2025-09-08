import os, json, time
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
try:
    from model.models import get_sentiment_pipeline
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from model.models import get_sentiment_pipeline

def load_env():
    env_path = Path(__file__).resolve().parents[2] / ".env.dev"
    if env_path.exists():
        load_dotenv(env_path)
    
    local_env = Path(".env.dev")
    if local_env.exists():
        load_dotenv(local_env)

load_env()

BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC   = os.getenv("RAW_TOPIC", "events.raw")
SCORED_TOPIC= os.getenv("SCORED_TOPIC", "events.scored")
GROUP_ID    = os.getenv("GROUP_ID", "inference-v1")

def main():
    print(f"[WORKER] Starting Kafka worker...")
    print(f"[WORKER] Bootstrap: {BOOTSTRAP}")
    print(f"[WORKER] Consuming from: {RAW_TOPIC}")
    print(f"[WORKER] Producing to: {SCORED_TOPIC}")
    
    # Initialize sentiment pipeline
    try:
        sentiment = get_sentiment_pipeline()
        print(f"[WORKER] Sentiment model loaded successfully")
    except Exception as e:
        print(f"[WORKER] ERROR: Failed to load sentiment model: {e}")
        return

    # Setup Kafka consumer
    try:
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            key_deserializer=lambda b : b.decode("utf-8") if b else None,
            value_deserializer=lambda b : json.loads(b.decode("utf-8")),
            consumer_timeout_ms=1000,  # Add timeout for testing
        )
        print(f"[WORKER] Kafka consumer connected")
    except Exception as e:
        print(f"[WORKER] ERROR: Failed to connect to Kafka consumer: {e}")
        return

    # Setup Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print(f"[WORKER] Kafka producer connected")
    except Exception as e:
        print(f"[WORKER] ERROR: Failed to connect to Kafka producer: {e}")
        return

    print(f"[WORKER] Starting message processing loop...")
    
    try:
        for msg in consumer:
            try:
                payload = msg.value
                text = payload.get("text", "")
                
                # Process with sentiment model
                result = sentiment(text)[0]
                
                # Create output message
                out = {
                    "id": payload.get("id"),
                    "source": payload.get("source"),
                    "text": text,
                    "lang": payload.get("lang"),
                    "created_at": payload.get("created_at"),
                    "meta": {**payload.get("meta", {}), "model": "distilbert-sst2"},
                    "sentiment": {"label": result["label"], "score": result["score"]},
                }

                # Send to scored topic
                producer.send(SCORED_TOPIC, key=out["id"], value=out)
                print(f"[WORKER] Processed message {out['id']}: {result['label']} ({result['score']:.4f})")
                
                # Small delay to avoid overwhelming
                time.sleep(0.01)
                
            except Exception as e:
                print(f"[WORKER] ERROR processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        print(f"[WORKER] Shutting down...")
    except Exception as e:
        print(f"[WORKER] ERROR in main loop: {e}")
    finally:
        consumer.close()
        producer.close()
        print(f"[WORKER] Kafka connections closed")

if __name__ == "__main__":
    main()