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
        self.rate_per_sec = int(os.getenv("RATE_PER_SEC", 1))
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8")
        )

    # Send mock messages to Kafka topic at a specified rate
    # For example, sending 10 messages
    def run(self):
        for i in range(10):
                    message = {
                        "id": str(i),
                        "source": "mock",
                        "text": f"Sample message {i}",
                        "lang": "en",
                        "created_at": "2024-01-01T00:00:00Z",
                        "meta": {"topic" : "jeans"},
                    }
                    self.producer.send(self.topic, key=str(i), value=message)
                    print(f"Sent: {message}")
                    time.sleep(1 / self.rate_per_sec)
        # block until all messages are sent
        self.producer.flush()
