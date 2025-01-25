
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub first instal using the command by copy and paste it in the terminal windwo
import glob
import os
import csv
import json

# setting up the Google Cloud credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]


project_id = "spheric-mission-448720-i7"
topic_name = "csvTopic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing records from CSV to {topic_path}.")

# views, and checks and then publishes the CSV records
csv_file = "Labels.csv"
with open(csv_file, mode="r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        # the follwoing code will convery to json file
        message = json.dumps(row).encode("utf-8")
        
        # shows the message
        print(f"Producing record: {message}")
        future = publisher.publish(topic_path, message)
        future.result()  

print("All records published.")