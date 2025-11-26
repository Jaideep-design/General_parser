import streamlit as st
import pandas as pd
import json
import jsonschema
from jsonschema import validate
from paho.mqtt import client as mqtt
import threading

# ------------------------------------------------------
# THREAD-SAFE LOG + DATA BUFFERS
# ------------------------------------------------------
log_buffer = []
buffer_lock = threading.Lock()

# Initialize session_state keys
if "run_mqtt" not in st.session_state:
    st.session_state["run_mqtt"] = False

if "parse_enabled" not in st.session_state:
    st.session_state["parse_enabled"] = True

if "last_raw" not in st.session_state:
    st.session_state["last_raw"] = None

if "last_df" not in st.session_state:
    st.session_state["last_df"] = None


# ------------------------------------------------------
# PART 1 — EXCEL → JSON CONVERTER
# ------------------------------------------------------

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
            "offset": {"type": "number"}
        },
        "required": ["short_name", "index", "size", "format", "signed", "scaling", "offset"]
    }
}

def normalize_excel_headers(uploaded_file):
    df_raw = pd.read_excel(uploaded_file, header=None)
    header_row = None

    for i in range(len(df_raw)):
        if df_raw.iloc[i].count() >= 3:
            header_row = i
            break

    if header_row is None:
        raise ValueError("Header row not detected")

    header = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = header
    df.dropna(how="all", inplace=True)

    return df

def validate_register(reg):
    try:
        validate(instance=reg, schema=SCHEMA["items"])
        return True, None
    except jsonschema.exceptions.ValidationError as err:
        return False, err.message

def excel_to_json(uploaded_file):
    df = normalize_excel_headers(uploaded_file)

    required = ["Short name", "Index", "Size [byte]", "Data format"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    registers = []

    for _, row in df.iterrows():
        if pd.isna(row["Short name"]) or pd.isna(row["Index"]):
            continue

        fmt = str(row["Data format"]).strip().upper()
        if fmt == "BINARY":
            fmt = "BIN"

        offset_val = row["Offset"] if "Offset" in df.columns and pd.notnull(row["Offset"]) else 0

        reg = {
            "short_name": str(row["Short name"]).strip().upper(),
            "index": int(row["Index"]),
            "size": int(row["Size [byte]"]),
            "format": fmt,
            "signed": str(row.get("Signed/Unsigned", "U")).strip().upper() == "S",
            "scaling": float(row.get("Scaling factor", 1.0)),
            "offset": float(offset_val)
        }

        ok, err = validate_register(reg)
        if not ok:
            raise ValueError(f"Validation Failed: {err}")

        registers.append(reg)

    return registers


# ------------------------------------------------------
# PART 2 — PARSER LOGIC (Single Dictionary)
# ------------------------------------------------------

def process_all_registers(df_dict, raw_packet):
    rows = []
    for _, row in df_dict.iterrows():
        idx = row["index"]
        size = row["size"]
        segment = raw_packet[idx: idx + size]

        rows.append({
            "Short name": row["short_name"],
            "Raw": segment,
            "format": row["format"],
            "scaling": row["scaling"],
            "offset": row["offset"]
        })
    return pd.DataFrame(rows)

def apply_dataformat_conversion(df):
    df["Value"] = df["Raw"]
    return df

def parse_packet(raw_packet, df_dict):
    df_out = process_all_registers(df_dict, raw_packet)
    df_final = apply_dataformat_conversion(df_out)
    return df_final


# ------------------------------------------------------
# PART 3 — MQTT LISTENER (Thread-Safe)
# ------------------------------------------------------

def mqtt_listener(broker, port, topic, df_dict):

    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        with buffer_lock:
            log_buffer.append(f"Connected (code {rc})")
        client.subscribe(topic)

    def on_message(client, userdata, msg):
        raw = msg.payload.decode("utf-8", "ignore")

        # Save raw packet
        with buffer_lock:
            st.session_state["last_raw"] = raw

        # Parse only if enabled
        if st.session_state["parse_enabled"]:
            df = parse_packet(raw, df_dict)
            with buffer_lock:
                st.session_state["last_df"] = df
        else:
            with buffer_lock:
                st.session_state["last_df"] = None

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, 60)

    # Run while listening is enabled
    while st.session_state["run_mqtt"]:
        client.loop(timeout=1)

    client.disconnect()


# ------------------------------------------------------
# STREAMLIT UI
# ------------------------------------------------------

st.title("📡 AC Dictionary → JSON → Live MQTT Parser")

# INPUTS
device_name = st.text_input("Device Name", value="EZMCSACD00001")
mqtt_topic = st.text_input("MQTT Subscriber Topic", value=f"/AC/2/{device_name}/Datalog")
uploaded_excel = st.file_uploader("Upload Dictionary Excel", type=["xlsx"])

# Convert Excel → JSON
if uploaded_excel and st.button("Convert Excel → JSON"):
    try:
        registers = excel_to_json(uploaded_excel)
        st.session_state["parser_json"] = registers
        st.success("Dictionary JSON generated!")
        st.json(registers[:5])

        st.download_button(
            "Download dictionary.json",
            json.dumps(registers, indent=2),
            "dictionary.json",
            "application/json"
        )
    except Exception as e:
        st.error(str(e))

# MQTT Params
st.header("Live MQTT Parser")
broker = st.text_input("MQTT Broker", value="ecozen.ai")
port = st.number_input("MQTT Port", value=1883)

# UI elements for output
st.subheader("Latest RAW Packet")
raw_output_box = st.empty()

st.subheader("Latest Parsed DataFrame")
parsed_output_box = st.empty()

# BUTTONS
col1, col2, col3, col4 = st.columns(4)

# START
if col1.button("▶ Start Listening"):

    if "parser_json" not in st.session_state:
        st.error("Convert an Excel dictionary first!")
    else:
        st.session_state["run_mqtt"] = True
        df_dict = pd.DataFrame(st.session_state["parser_json"])

        threading.Thread(
            target=mqtt_listener,
            args=(broker, port, mqtt_topic, df_dict),
            daemon=True
        ).start()

        st.success("MQTT listening started!")

# PAUSE
if col2.button("⏸ Pause Parsing"):
    st.session_state["parse_enabled"] = False
    st.info("Parsing paused (still receiving packets).")

# RESUME
if col3.button("▶ Resume Parsing"):
    st.session_state["parse_enabled"] = True
    st.success("Parsing resumed.")

# STOP
if col4.button("⏹ Stop Listener"):
    st.session_state["run_mqtt"] = False
    st.session_state["parse_enabled"] = True
    st.warning("Stopping listener...")

# AUTO REFRESH UI
with buffer_lock:
    if st.session_state["last_raw"] is not None:
        raw_output_box.code(st.session_state["last_raw"])

    if st.session_state["last_df"] is not None:
        parsed_output_box.dataframe(st.session_state["last_df"])
