# shared_state.py
import threading
import time
from collections import defaultdict

# Thread-safe dict for storing latest MQTT raw + parsed message
state_lock = threading.Lock()

shared_state = {
    "raw_packet": None,
    "parsed_df": None,
    "last_update_time": None,
}

def update_state(raw_packet=None, parsed_df=None):
    with state_lock:
        if raw_packet is not None:
            shared_state["raw_packet"] = raw_packet
        if parsed_df is not None:
            shared_state["parsed_df"] = parsed_df
        shared_state["last_update_time"] = time.time()

def get_state():
    with state_lock:
        return shared_state.copy()

def clear_state():
    with state_lock:
        shared_state["raw_packet"] = None
        shared_state["parsed_df"] = None
        shared_state["last_update_time"] = None


# -------------------------------
# Track device online/offline
# -------------------------------
activity_lock = threading.Lock()
last_activity = defaultdict(lambda: 0)

def update_activity(topic):
    with activity_lock:
        last_activity[topic] = time.time()

def is_topic_online(topic, timeout=60):
    with activity_lock:
        return (time.time() - last_activity[topic]) < timeout
