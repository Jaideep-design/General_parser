# -*- coding: utf-8 -*-
"""
Created on Sat Apr 19 12:22:11 2025

@author: Admin
"""

# shared_state.py
import threading
from collections import defaultdict
import time

latest_data_lock = threading.Lock()
latest_data = {"PV_V": 100, "PV_W": 100}

def get_latest_data():
    with latest_data_lock:
        return latest_data.copy()

def clear_latest_data():
    with latest_data_lock:
        latest_data.clear()

# Add this for tracking online status
last_mqtt_activity = defaultdict(lambda: 0)
activity_lock = threading.Lock()

def update_activity(topic):
    with activity_lock:
        last_mqtt_activity[topic] = time.time()

def is_topic_online(topic, threshold=120):  # seconds
    with activity_lock:
        last_seen = last_mqtt_activity.get(topic, 0)
        return (time.time() - last_seen) < threshold

