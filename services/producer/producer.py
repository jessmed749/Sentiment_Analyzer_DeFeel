import os
import time
import json

from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables from a .env file located two directories up
def _load_env():
    env_path = Path(__file__).resolve().parents[1] / ".env.dev"
    if env_path.exists():
        load_dotenv(env_path)

# Create a Kafka producer that sends mock messages to a specified topic at a defined rate
class Producer:
    def __init__(self):
        _load_env()
        self.topic = os.getenv("RAW_TOPIC", "events.raw")
        self.rate_per_sec = int(os.getenv("RATE_PER_SEC", 5))

        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")

        # Initialize Kafka producer with JSON serialization for both keys and values
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            linger_ms=50,
        )
        print(f"[producer] Using bootstrap={bootstrap}, topic={self.topic}, rate={self.rate_per_sec}/s", flush=True)

    def run(self, n_messages: int = 10):
        try:
            for i in range(n_messages):
                message = {
                    "id": str(i),
                    "source": "mock",
                    "text": f"Sample message {i}",
                    "lang": "en",
                    "created_at": "2024-01-01T00:00:00Z",
                    "meta": {"topic": "jeans"},
                }
                self.producer.send(self.topic, key=str(i), value=message)
                print("Sent:", message, flush=True)  
                time.sleep(1 / self.rate_per_sec)
            self.producer.flush()
            print("[producer] Done.", flush=True)
        finally:
            # Close the producer to ensure all messages are sent before exiting
            self.producer.close()

if __name__ == "__main__":
    Producer().run(n_messages=10)
