# app.py
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import json
import jsonschema
from jsonschema import validate

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

SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "short_name": {"type": "string"},
            "index": {"type": "integer", "minimum": 0},
            "size": {"type": "integer", "minimum": 1},
            "format": {"type": "string", "enum": ["ASCII", "DEC", "HEX", "BIN"]},
            "signed": {"type": "boolean"},
            "scaling": {"type": "number"},
            "offset": {"type": "number"},
        },
        "required": ["short_name", "index", "size", "format", "signed", "scaling", "offset"],
    }
}


def normalize_excel_headers(uploaded_file):
    """Detect header row (first with ≥3 non-null cells)."""
    df_raw = pd.read_excel(uploaded_file, header=None)
    header_row = None

    for i in range(len(df_raw)):
        if df_raw.iloc[i].count() >= 3:
            header_row = i
            break

    if header_row is None:
        raise ValueError("Header row not detected in Excel.")

    header = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = header
    df.dropna(how="all", inplace=True)

    return df


def validate_register(reg):
    """Validate each register using the JSON SCHEMA."""
    try:
        validate(instance=reg, schema=SCHEMA["items"])
        return True, None
    except jsonschema.exceptions.ValidationError as err:
        return False, err.message


def excel_to_json(uploaded_file):
    """Convert Excel rows → list of register dicts."""
    df = normalize_excel_headers(uploaded_file)

    required = ["Short name", "Index", "Size [byte]", "Data format"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}\nAvailable columns: {list(df.columns)}")
            
    if uploaded_excel and st.button("Convert Excel → JSON"):
    df_raw = pd.read_excel(uploaded_excel, header=None)
    df = normalize_headers(df_raw)
    if df is None:
        st.error("Failed to detect header row.")
        st.stop()
        
    registers = []

    for _, row in df.iterrows():
        # Skip totally empty or incomplete rows
        if pd.isna(row["Short name"]) or pd.isna(row["Index"]):
            continue

        # Format normalization
        fmt = str(row["Data format"]).strip().upper()
        if fmt == "BINARY":
            fmt = "BIN"
        if fmt not in ["ASCII", "DEC", "HEX", "BIN"]:
            fmt = "DEC"  # Default fallback

        # Offset
        offset_val = row["Offset"] if ("Offset" in df.columns and pd.notnull(row["Offset"])) else 0

        reg = {
            "short_name": str(row["Short name"]).strip().upper(),
            "index": int(row["Index"]),
            "size": int(row["Size [byte]"]),
            "format": fmt,
            "signed": str(row.get("Signed/Unsigned", "U")).strip().upper() == "S",
            "scaling": float(row.get("Scaling factor", 1.0)),
            "offset": float(offset_val),
        }

        ok, err = validate_register(reg)
        if not ok:
            raise ValueError(f"Validation Failed at index {reg['index']}: {err}")

        registers.append(reg)

    return registers

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
