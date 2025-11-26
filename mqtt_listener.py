# mqtt_listener.py
import threading
import time
from paho.mqtt import client as mqtt
from shared_state import update_state, update_activity
from parser_module import parse_packet   # your parser

def start_mqtt_listener(broker, port, topic, df_dict):
    """
    Runs MQTT in a background thread.
    Does NOT use streamlit. Only updates shared_state.
    """

    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        update_activity(topic)
        client.subscribe(topic)

    def on_message(client, userdata, msg):
        raw = msg.payload.decode("utf-8", "ignore")

        # Update activity heartbeat
        update_activity(topic)

        # Parse packet
        df = parse_packet(raw, df_dict)

        # Update shared_state
        update_state(raw_packet=raw, parsed_df=df)

    client.on_connect = on_connect
    client.on_message = on_message

    # Start connection
    client.connect(broker, port, 60)

    # Loop forever in this thread
    client.loop_forever()


def run_listener_in_background(broker, port, topic, df_dict):
    thread = threading.Thread(
        target=start_mqtt_listener,
        args=(broker, port, topic, df_dict),
        daemon=True,
    )
    thread.start()
