import streamlit as st
import pandas as pd
import json
import jsonschema
from jsonschema import validate
from paho.mqtt import client as mqtt
import threading


log_buffer = []
buffer_lock = threading.Lock()
# %%
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
# PART 2 — PARSER LOGIC (uses ONLY uploaded dictionary)
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
    df["Value"] = df["Raw"]  # Expand logic later if needed
    return df


def parse_packet(raw_packet, df_dict):
    df_out = process_all_registers(df_dict, raw_packet)
    df_final = apply_dataformat_conversion(df_out)
    return df_final


# ------------------------------------------------------
# PART 3 — THREAD-SAFE MQTT LISTENER
# ------------------------------------------------------

stop_flag = False

def mqtt_listener(broker, port, topic, df_dict):
    global stop_flag, log_buffer

    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        with buffer_lock:
            log_buffer.append(f"Connected (code {rc})")
        client.subscribe(topic)

    def on_message(client, userdata, msg):
        raw = msg.payload.decode("utf-8", "ignore")
        df = parse_packet(raw, df_dict)

        with buffer_lock:
            log_buffer.append(
                f"### Topic: {msg.topic}\n```\n{df.to_string()}\n```"
            )

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, 60)

    while not stop_flag:
        client.loop(timeout=1)

# %%
# ------------------------------------------------------
# STREAMLIT UI
# ------------------------------------------------------

st.title("📡 AC Dictionary → JSON → Live MQTT Parser")

# Initialize log state
if "mqtt_log" not in st.session_state:
    st.session_state["mqtt_log"] = []


# --------------------------------------------
# INPUT 1: Device name
# --------------------------------------------
device_name = st.text_input("Device Name", value="EZMCSACD00001")

# --------------------------------------------
# INPUT 2: MQTT subscriber topic
# --------------------------------------------
mqtt_topic = st.text_input("Subscriber Topic", value=f"/AC/2/{device_name}/Datalog")

# --------------------------------------------
# INPUT 3: Upload Dictionary Excel
# --------------------------------------------
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
        st.error(f"Error: {e}")


# ------------------------------------------------------
# MQTT PARAMETERS
# ------------------------------------------------------
st.header("Live MQTT Parser")

broker = st.text_input("MQTT Broker", value="ecozen.ai")
port = st.number_input("MQTT Port", value=1883)

output_box = st.empty()

# ------------------------------------------------------
# Start / Stop buttons
# ------------------------------------------------------
col1, col2 = st.columns(2)

if col1.button("▶ Start MQTT Listener"):

    if "parser_json" not in st.session_state:
        st.error("Please convert an Excel dictionary first!")
    else:
        df_dict = pd.DataFrame(st.session_state["parser_json"])

        threading.Thread(
            target=mqtt_listener,
            args=(broker, port, mqtt_topic, df_dict),
            daemon=True
        ).start()

        st.success("Listener started!")


if col2.button("⏹ Stop MQTT Listener"):
    stop_flag = True
    st.warning("Listener stopping...")


# ------------------------------------------------------
# AUTO-UPDATING UI BLOCK
# ------------------------------------------------------
with buffer_lock:
    logs = "\n\n".join(log_buffer[-30:])

output_box.markdown(logs)
