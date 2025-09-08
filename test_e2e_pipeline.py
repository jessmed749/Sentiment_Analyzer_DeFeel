#!/usr/bin/env python3
"""
End-to-End Pipeline Test Script
Tests the complete flow: Producer → Kafka → Inference Worker → Scored Topic
"""

import os
import json
import time
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load environment
load_dotenv(".env.dev")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
RAW_TOPIC = os.getenv("RAW_TOPIC", "events.raw")
SCORED_TOPIC = os.getenv("SCORED_TOPIC", "events.scored")
INFERENCE_URL = "http://localhost:8000"

def test_inference_api():
    """Test the FastAPI inference service directly"""
    print("🧪 Testing FastAPI inference service...")
    
    # Test health endpoint
    try:
        response = requests.get(f"{INFERENCE_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Health endpoint working")
            print(f"   Response: {response.json()}")
        else:
            print(f"❌ Health endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health endpoint error: {e}")
        return False
    
    # Test predict endpoint
    try:
        test_payload = {
            "id": "test-api",
            "source": "e2e-test",
            "text": "This is an amazing product!",
            "lang": "en",
            "created_at": "2024-01-01T00:00:00Z",
            "meta": {"test": True}
        }
        
        response = requests.post(
            f"{INFERENCE_URL}/predict",
            json=test_payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Predict endpoint working")
            print(f"   Text: '{result['text']}'")
            print(f"   Prediction: {result['label']} (confidence: {result['score']:.4f})")
            return True
        else:
            print(f"❌ Predict endpoint failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Predict endpoint error: {e}")
        return False

def test_kafka_connectivity():
    """Test Kafka producer and consumer connectivity"""
    print("\n🧪 Testing Kafka connectivity...")
    
    try:
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        print("✅ Kafka producer connected")
        
        # Test consumer
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            consumer_timeout_ms=1000,
        )
        print("✅ Kafka consumer connected")
        
        producer.close()
        consumer.close()
        return True
        
    except Exception as e:
        print(f"❌ Kafka connectivity error: {e}")
        return False

def test_end_to_end_pipeline():
    """Test the complete pipeline: produce → process → consume"""
    print("\n🧪 Testing end-to-end pipeline...")
    
    test_messages = [
        {
            "id": "e2e-1",
            "source": "e2e-test",
            "text": "I absolutely love this product!",
            "lang": "en",
            "created_at": "2024-01-01T00:00:00Z",
            "meta": {"test": "positive"}
        },
        {
            "id": "e2e-2", 
            "source": "e2e-test",
            "text": "This is terrible and I hate it.",
            "lang": "en",
            "created_at": "2024-01-01T00:00:00Z",
            "meta": {"test": "negative"}
        },
        {
            "id": "e2e-3",
            "source": "e2e-test", 
            "text": "The weather is okay today.",
            "lang": "en",
            "created_at": "2024-01-01T00:00:00Z",
            "meta": {"test": "neutral"}
        }
    ]
    
    try:
        # Set up producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        
        # Set up consumer for scored messages
        consumer = KafkaConsumer(
            SCORED_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="e2e-test-consumer",
            auto_offset_reset="latest",  # Only get new messages
            consumer_timeout_ms=30000,   # 30 second timeout
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        
        print(f"📤 Sending {len(test_messages)} test messages to {RAW_TOPIC}...")
        
        # Send test messages
        for msg in test_messages:
            producer.send(RAW_TOPIC, key=msg["id"], value=msg)
            print(f"   Sent: {msg['id']} - '{msg['text'][:30]}...'")
        
        producer.flush()
        producer.close()
        
        print(f"📥 Waiting for processed messages on {SCORED_TOPIC}...")
        
        # Collect processed messages
        processed_messages = []
        start_time = time.time()
        
        for message in consumer:
            processed_msg = message.value
            processed_messages.append(processed_msg)
            
            print(f"   Received: {processed_msg['id']} - {processed_msg.get('sentiment', {}).get('label', 'UNKNOWN')} "
                  f"({processed_msg.get('sentiment', {}).get('score', 0):.4f})")
            
            # Stop when we've received all test messages or timeout
            if len(processed_messages) >= len(test_messages):
                break
                
            if time.time() - start_time > 30:  # 30 second timeout
                print("⚠️  Timeout waiting for messages")
                break
        
        consumer.close()
        
        # Verify results
        if len(processed_messages) == len(test_messages):
            print(f"✅ End-to-end pipeline working! Processed {len(processed_messages)}/{len(test_messages)} messages")
            
            # Print summary
            print("\n📊 Results Summary:")
            for msg in processed_messages:
                sentiment = msg.get('sentiment', {})
                print(f"   {msg['id']}: '{msg['text'][:40]}...' → {sentiment.get('label', 'UNKNOWN')} ({sentiment.get('score', 0):.4f})")
            
            return True
        else:
            print(f"❌ Pipeline incomplete: processed {len(processed_messages)}/{len(test_messages)} messages")
            return False
            
    except Exception as e:
        print(f"❌ End-to-end test error: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting End-to-End Pipeline Tests\n")
    
    # Test 1: FastAPI service
    api_ok = test_inference_api()
    
    # Test 2: Kafka connectivity  
    kafka_ok = test_kafka_connectivity()
    
    # Test 3: End-to-end pipeline
    if api_ok and kafka_ok:
        e2e_ok = test_end_to_end_pipeline()
    else:
        print("\n⚠️  Skipping end-to-end test due to failed prerequisites")
        e2e_ok = False
    
    # Summary
    print(f"\n📋 Test Results:")
    print(f"   FastAPI Service: {'✅ PASS' if api_ok else '❌ FAIL'}")
    print(f"   Kafka Connectivity: {'✅ PASS' if kafka_ok else '❌ FAIL'}")
    print(f"   End-to-End Pipeline: {'✅ PASS' if e2e_ok else '❌ FAIL'}")
    
    if api_ok and kafka_ok and e2e_ok:
        print("\n🎉 All tests passed! Your pipeline is working end-to-end!")
        return True
    else:
        print("\n❌ Some tests failed. Check the logs above for details.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)