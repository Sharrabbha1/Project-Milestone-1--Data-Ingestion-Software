
from google.cloud import pubsub_v1
import glob
import os
import json

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "spheric-mission-448720-i7"
subscription_id = "csvTopic-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")


def callback(message):
    # Deserialize the message data
    message_data = json.loads(message.data.decode("utf-8"))
    
    # it will able to print  the deserialized message
    print(f"Consumed record: {message_data}")

    # shwos and then acknowdge the messages 
    message.ack()

with subscriber:
    # Subscribe to the topic and process messages using the callback
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()