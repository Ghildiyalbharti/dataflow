import json
import random
from google.cloud import pubsub_v1
from datetime import datetime, timezone
import time

# Configuration
PROJECT_ID = "hoppecke-connected"
TOPIC_ID = "poc-device-data-stream"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

deviceIds = [
    "e3ca5450-3d19-11f0-a06f-3fc2bb254d43",
    "ea2bef70-3d19-11f0-9d8e-4deb9edf5046",
    "ef771180-3d19-11f0-9d8e-4deb9edf5046",
]

def generate_simple_record():
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    soc = random.randint(0, 100)
    temp = random.randint(14000, 15000)
    return timestamp, soc, temp

def publish_message(counter):
    for deviceId in deviceIds:
        timestamp, soc, temp = generate_simple_record()

        payload = {
            "deviceId": deviceId,
            "timestamp": timestamp,
            "soc": soc,
            "temp": temp
        }

        future = publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
        result = future.result(timeout=60)
        print(f" Published message for {deviceId} (Run #{counter}) → msg ID: {result}")

    print(f"Finished publishing for all devices (Run #{counter}).")

if __name__ == "__main__":
    counter = 1
    while True:
        print(f" Run #{counter}")
        publish_message(counter)
        print(" Waiting for 60 seconds before next run...\n")
        counter += 1
        time.sleep(60)
