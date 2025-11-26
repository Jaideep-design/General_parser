import streamlit as st
import pandas as pd
import json
import os
import jsonschema
from jsonschema import validate
import threading
from paho.mqtt import client as mqtt

# Import your parser functions
from General_parser_MQTT import (
    process_all_registers,
    apply_dataformat_conversion
)

# ----------------------------
# JSON Schema for validation
# ----------------------------
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


# ---------------------------------------
# Utility: Detect header properly from Excel
# ---------------------------------------
def normalize_excel_headers(uploaded_file):
    df_raw = pd.read_excel(uploaded_file, header=None)
    
    header_row_index = None
    for i in range(len(df_raw)):
        if df_raw.iloc[i].count() >= 3:
            header_row_index = i
            break

    if header_row_index is None:
        raise ValueError("Unable to detect header row")

    header = df_raw.iloc[header_row_index].tolist()
    df_clean = df_raw.iloc[header_row_index + 1:].copy()
    df_clean.columns = header
    df_clean.dropna(how="all", inplace=True)

    return df_clean


def validate_register(reg):
    try:
        validate(instance=reg, schema=SCHEMA["items"])
        return True, None
    except jsonschema.exceptions.ValidationError as err:
        return False, err.message


# ---------------------------------------
# Convert Excel → JSON
# ---------------------------------------
def excel_to_json(uploaded_file):
    df = normalize_excel_headers(uploaded_file)

    required_cols = ["Short name", "Index", "Size [byte]", "Data format"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}'")

    registers = []
    for _, row in df.iterrows():
        if pd.isna(row["Short name"]) or pd.isna(row["Index"]):
            continue

        fmt = str(row["Data format"]).strip().upper()
        if fmt == "BINARY": fmt = "BIN"

        offset_val = (
            row["Offset"]
            if "Offset" in df.columns and pd.notnull(row["Offset"])
            else 0
        )

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
            raise ValueError(f"Validation failed: {err}")

        registers.append(reg)

    return registers


# ============================================================
# ====================== MQTT PARSER ==========================
# ============================================================
mqtt_client = None
mqtt_thread = None
stop_mqtt_flag = False

parsed_output_placeholder = None


def parse_packet(raw_packet, dict_list):
    dict_name, df_dict_selected = guess_dictionary(raw_packet, dict_list)
    df_out = process_all_registers(df_dict_selected, raw_packet)
    df_final = apply_dataformat_conversion(df_out, df_dict_selected)
    return dict_name, df_final


def start_mqtt_listener(broker, port, sub_topic, dict_list):
    global mqtt_client, stop_mqtt_flag, parsed_output_placeholder

    stop_mqtt_flag = False
    mqtt_client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        parsed_output_placeholder.write(f"Connected with code {rc}")
        client.subscribe(sub_topic)

    def on_message(client, userdata, msg):
        raw_payload = msg.payload.decode("utf-8", errors="ignore")
        dict_name, df_final = parse_packet(raw_payload, dict_list)
        parsed_output_placeholder.write(
            f"**Dictionary Used:** {dict_name}\n\n"
            f"**Raw:** {raw_payload[:100]}...\n\n"
            f"```{df_final.to_string()}```"
        )

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(broker, port, 60)

    while not stop_mqtt_flag:
        mqtt_client.loop(timeout=1.0)


# ============================================================
# ======================= STREAMLIT UI ========================
# ============================================================
st.title("📡 Parser + Dictionary Builder UI")

st.sidebar.header("⚙ Configuration")

# --------------------------
# Upload Excel file
# --------------------------
st.header("1️⃣ Upload Dictionary Excel → Convert to JSON")

uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

if uploaded_file:
    if st.button("Convert to JSON"):
        try:
            json_data = excel_to_json(uploaded_file)
            st.success("Conversion successful!")
            
            # Show preview
            st.json(json_data[:5])

            # Provide download
            json_str = json.dumps(json_data, indent=2)
            st.download_button(
                label="Download JSON",
                data=json_str,
                file_name="dictionary.json",
                mime="application/json"
            )

            st.session_state["converted_json"] = json_data

        except Exception as e:
            st.error(f"Error: {e}")


# --------------------------
# MQTT Setup
# --------------------------
st.header("2️⃣ MQTT Listener Setup")

selected_topic = st.text_input("Enter selected_topic", "EZMCSACD00001")
mqtt_topic = f"/AC/2/{selected_topic}/Datalog"
mqtt_topic_input = st.text_input("MQTT Subscriber Topic", mqtt_topic)

broker = st.text_input("MQTT Broker", "ecozen.ai")
port = st.number_input("MQTT Port", value=1883)

# Load JSON dictionaries
st.subheader("Upload JSON Dictionaries (Required for Parsing)")

json_ecofrost = st.file_uploader("Ecofrost JSON", type=["json"])
json_deye = st.file_uploader("Deye JSON", type=["json"])
json_sunnal = st.file_uploader("Sunnal JSON", type=["json"])

dict_list = []
if json_ecofrost:
    dict_list.append(("ecofrost", pd.DataFrame(json.load(json_ecofrost))))
if json_deye:
    dict_list.append(("deye", pd.DataFrame(json.load(json_deye))))
if json_sunnal:
    dict_list.append(("sunnal", pd.DataFrame(json.load(json_sunnal))))


# MQTT output box
parsed_output_placeholder = st.empty()


# --------------------------
# START / STOP MQTT
# --------------------------
col1, col2 = st.columns(2)

if col1.button("▶ Start MQTT Listener"):
    if not dict_list:
        st.error("Please upload at least one JSON dictionary.")
    else:
        stop_mqtt_flag = False
        mqtt_thread = threading.Thread(
            target=start_mqtt_listener,
            args=(broker, port, mqtt_topic_input, dict_list),
            daemon=True
        )
        mqtt_thread.start()
        st.success("MQTT Listener started.")

if col2.button("⏹ Stop Listener"):
    stop_mqtt_flag = True
    st.warning("Stopping MQTT listener...")
