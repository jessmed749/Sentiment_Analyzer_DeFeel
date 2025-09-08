import os, json, time
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline

def load_env():
    # Load environment variables from container env
    pass

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
    print(f"[WORKER] Group ID: {GROUP_ID}")
    
    # Initialize sentiment pipeline directly 
    try:
        print(f"[WORKER] Loading sentiment model...")
        sentiment = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
        print(f"[WORKER] Sentiment model loaded successfully")
    except Exception as e:
        print(f"[WORKER] ERROR: Failed to load sentiment model: {e}")
        return

    # Setup Kafka consumer
    try:
        print(f"[WORKER] Connecting to Kafka consumer...")
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            key_deserializer=lambda b : b.decode("utf-8") if b else None,
            value_deserializer=lambda b : json.loads(b.decode("utf-8")),
        )
        print(f"[WORKER] Kafka consumer connected successfully")
    except Exception as e:
        print(f"[WORKER] ERROR: Failed to connect to Kafka consumer: {e}")
        print(f"[WORKER] Bootstrap servers: {BOOTSTRAP}")
        return

    # Setup Kafka producer
    try:
        print(f"[WORKER] Connecting to Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print(f"[WORKER] Kafka producer connected successfully")
    except Exception as e:
        print(f"[WORKER] ERROR: Failed to connect to Kafka producer: {e}")
        return

    print(f"[WORKER] Starting message processing loop...")
    
    try:
        for msg in consumer:
            try:
                payload = msg.value
                text = payload.get("text", "")
                
                print(f"[WORKER] Processing message {payload.get('id')}: '{text[:50]}...'")
                
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
                producer.flush()  # Ensure message is sent
                
                print(f"[WORKER] ✅ Processed message {out['id']}: {result['label']} ({result['score']:.4f})")
                
                time.sleep(0.01)
                
            except Exception as e:
                print(f"[WORKER] ERROR processing message: {e}")
                import traceback
                traceback.print_exc()
                continue
                
    except KeyboardInterrupt:
        print(f"[WORKER] Shutting down...")
    except Exception as e:
        print(f"[WORKER] ERROR in main loop: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        producer.close()
        print(f"[WORKER] Kafka connections closed")

if __name__ == "__main__":
    main()