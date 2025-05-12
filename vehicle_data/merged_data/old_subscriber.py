from google.cloud import pubsub_v1
import time
import threading

project_id = "dataengr-dataguru"
sub_id = "project-topic-sub"

msgs = []
lock = threading.Lock()
idle_seconds = 10 #wait time when there's no new message 

last_msg_time = time.time()

def callback(msg):
    global last_msg_time
    with lock:
        msgs.append(msg.data)
    msg.ack()
    last_msg_time = time.time()

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(project_id, sub_id)
pull = subscriber.subscribe(sub_path, callback=callback)

start = time.time()

print("Starting Listening for Messages at", sub_path, "...\n")

try:
    while True:
        time.sleep(1)
        if time.time() - last_msg_time > idle_seconds:
            pull.cancel()
            break
except KeyboardInterrupt:
    pull.cancel()

pull.result()  # Wait for it to close
end = time.time()

print("Received", len(msgs), "messages")
print("Total runtime:", round(end - start, 2), "seconds")

