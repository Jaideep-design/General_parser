# app.py
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import json

from shared_state import get_state, is_topic_online, clear_state
from mqtt_listener import start_mqtt_thread
from parser_module import parse_packet  # For testing/local parsing

# Auto refresh UI every 5 seconds
st_autorefresh(interval=5000, key="refresh_app")

st.title("📡 Live AC Parser — Excel → JSON → MQTT → Parsed Data")

# -----------------------------------------
# 1. UPLOAD EXCEL → CONVERT TO JSON
# -----------------------------------------
uploaded_excel = st.file_uploader("Upload Dictionary Excel", type=["xlsx"])

def normalize_headers(df_raw):
    header_row = None
    for i in range(len(df_raw)):
        if df_raw.iloc[i].count() >= 3:
            header_row = i
            break
    if header_row is None:
        return None
    df = df_raw.iloc[header_row+1:].copy()
    df.columns = df_raw.iloc[header_row].tolist()
    df.dropna(how="all", inplace=True)
    return df

if uploaded_excel and st.button("Convert Excel → JSON"):
    df_raw = pd.read_excel(uploaded_excel, header=None)
    df = normalize_headers(df_raw)
    if df is None:
        st.error("Failed to detect header row.")
        st.stop()

    registers = []
    for _, row in df.iterrows():
        reg = {
            "short_name": str(row["Short name"]).upper(),
            "index": int(row["Index"]),
            "size": int(row["Size [byte]"]),
            "format": str(row["Data format"]).upper(),
            "signed": str(row.get("Signed/Unsigned", "U")).upper() == "S",
            "scaling": float(row.get("Scaling factor", 1.0)),
            "offset": float(row.get("Offset", 0)),
        }
        registers.append(reg)

    st.session_state["json_dict"] = registers
    st.success("Dictionary JSON created!")
    st.json(registers[:5])

    st.download_button(
        "Download dictionary.json",
        json.dumps(registers, indent=2),
        "dictionary.json",
        "application/json"
    )

# -----------------------------------------
# 2. MQTT PARAMETERS + START BUTTON
# -----------------------------------------
st.markdown("---")
st.header("MQTT Settings")

broker = st.text_input("MQTT Broker", value="ecozen.ai")
port = st.number_input("Port", value=1883)
device_name = st.text_input("Device Name", value="EZMCSACD00001")

topic = st.text_input(
    "MQTT Topic",
    value=f"/AC/2/{device_name}/Datalog"
)

# Start listener
if st.button("Start MQTT Listener"):
    if "json_dict" not in st.session_state:
        st.error("Please upload and convert Excel dictionary first!")
        st.stop()

    df_dict = pd.DataFrame(st.session_state["json_dict"])
    clear_state()
    start_mqtt_thread(broker, int(port), topic, df_dict)

    st.success("🎉 MQTT listener started in background!")

# -----------------------------------------
# 3. LIVE DATA FROM shared_state
# -----------------------------------------
st.markdown("---")
st.header("📡 Live Data Stream")

state = get_state()

# Online/offline badge
online = is_topic_online(topic)
if online:
    st.success("🟢 Topic Online")
else:
    st.error("🔴 Topic Offline")

# Raw packet
if state["raw_packet"]:
    st.subheader("Raw Packet")
    st.code(state["raw_packet"])

# Parsed dataframe
if state["parsed_df"] is not None:
    st.subheader("Parsed Output")
    st.dataframe(state["parsed_df"])

# Timestamp
if state["last_update_time"]:
    import time
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(state["last_update_time"]))
    st.write(f"⏱ Last update: {ts}")
else:
    st.info("Waiting for MQTT data...")
