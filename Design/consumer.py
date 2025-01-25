# Consumer Script
from google.cloud import pubsub_v1
import glob
import os
import json

# Set up Google Cloud credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud project and subscription setup
project_id = "spheric-mission-448720-i7"
subscription_id = "csvTopic-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")

# Callback to process received messages
def callback(message):
    # Deserialize the message data
    message_data = json.loads(message.data.decode("utf-8"))
    
    # Print the deserialized message
    print(f"Consumed record: {message_data}")

    # Acknowledge the message
    message.ack()

with subscriber:
    # Subscribe to the topic and process messages using the callback
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()