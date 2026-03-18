from kafka import KafkaProducer


def main():
    producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    producer.close()
    print("✅ Connected to Kafka successfully!")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"❌ Failed to connect to Kafka: {exc}")
