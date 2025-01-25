# Producer Script
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob
import os
import csv
import json

# Set up Google Cloud credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud project and topic setup
project_id = "spheric-mission-448720-i7"
topic_name = "csvTopic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing records from CSV to {topic_path}.")

# Read and publish CSV records
csv_file = "Labels.csv"
with open(csv_file, mode="r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Convert the row to a JSON string (serialization)
        message = json.dumps(row).encode("utf-8")
        
        # Publish the message
        print(f"Producing record: {message}")
        future = publisher.publish(topic_path, message)
        future.result()  # Ensure the message is sent successfully

print("All records published.")