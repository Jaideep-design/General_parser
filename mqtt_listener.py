# mqtt_listener.py
from paho.mqtt import client as mqtt
import threading
from shared_state import update_state, update_activity
from parser_module import parse_packet

def mqtt_worker(broker, port, topic, df_dict):
    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        update_activity(topic)
        client.subscribe(topic)

    def on_message(client, userdata, msg):
        raw = msg.payload.decode("utf-8", "ignore")
        update_activity(topic)

        df = parse_packet(raw, df_dict)
        update_state(raw_packet=raw, parsed_df=df)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, 60)
    client.loop_forever()

def start_mqtt_thread(broker, port, topic, df_dict):
    t = threading.Thread(
        target=mqtt_worker,
        args=(broker, port, topic, df_dict),
        daemon=True
    )
    t.start()
