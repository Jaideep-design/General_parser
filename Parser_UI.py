import streamlit as st
import pandas as pd
import json
import jsonschema
from jsonschema import validate
from paho.mqtt import client as mqtt
import threading

# ======================================================
# GLOBAL THREAD-SAFE STATE (NO streamlit HERE)
# ======================================================

log_buffer = []
buffer_lock = threading.Lock()

# Control flags for the MQTT thread
run_mqtt_flag = False
parse_enabled_flag = True

# Latest raw message and parsed dataframe
global_last_raw = None
global_last_df = None

flag_lock = threading.Lock()


# ======================================================
# PART 1 — EXCEL → JSON CONVERTER
# ======================================================

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
    """Detect header row (first with >=3 non-null cells) and return a cleaned DataFrame."""
    df_raw = pd.read_excel(uploaded_file, header=None)
    header_row = None

    for i in range(len(df_raw)):
        if df_raw.iloc[i].count() >= 3:
            header_row = i
            break

    if header_row is None:
        raise ValueError("Header row not detected")

    header = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1 :].copy()
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
    """Convert the uploaded Excel dictionary into a list of JSON-register dicts."""
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

        offset_val = 0
        if "Offset" in df.columns and pd.notnull(row["Offset"]):
            offset_val = row["Offset"]

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
            raise ValueError(f"Validation Failed: {err}")

        registers.append(reg)

    return registers


# ======================================================
# PART 2 — PARSER LOGIC (Single Dictionary)
# ======================================================

def process_all_registers(df_dict: pd.DataFrame, raw_packet: str) -> pd.DataFrame:
    """Extract raw substrings from the packet using index/size from the dictionary."""
    rows = []
    for _, row in df_dict.iterrows():
        idx = row["index"]
        size = row["size"]
        segment = raw_packet[idx : idx + size]  # raw_packet is assumed string/hex string

        rows.append(
            {
                "Short name": row["short_name"],
                "Raw": segment,
                "format": row["format"],
                "scaling": row["scaling"],
                "offset": row["offset"],
            }
        )
    return pd.DataFrame(rows)


def apply_dataformat_conversion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dummy conversion right now: Value = Raw.
    You can expand this to handle DEC/HEX/BIN/ASCII and scaling/offset as needed.
    """
    df = df.copy()
    df["Value"] = df["Raw"]
    return df


def parse_packet(raw_packet: str, df_dict: pd.DataFrame) -> pd.DataFrame:
    df_out = process_all_registers(df_dict, raw_packet)
    df_final = apply_dataformat_conversion(df_out)
    return df_final


# ======================================================
# PART 3 — MQTT LISTENER (Thread, NO streamlit inside)
# ======================================================

def mqtt_listener(broker: str, port: int, topic: str, df_dict: pd.DataFrame):
    """
    Background MQTT listener.
    Uses only global flags and buffers; does NOT touch st.session_state or UI directly.
    """
    global run_mqtt_flag, parse_enabled_flag
    global global_last_raw, global_last_df

    client = mqtt.Client()  # Warning about callback API v1 is harmless, can be ignored.

    def on_connect(client, userdata, flags, rc):
        # Log connection
        with buffer_lock:
            log_buffer.append(f"Connected to {broker}:{port} with code {rc}")
        client.subscribe(topic)

    def on_message(client, userdata, msg):
        nonlocal df_dict

        raw = msg.payload.decode("utf-8", "ignore")

        # Store raw packet
        with buffer_lock:
            global_last_raw = raw

        # Read parse flag safely
        with flag_lock:
            parse_now = parse_enabled_flag

        if parse_now:
            df = parse_packet(raw, df_dict)
            with buffer_lock:
                global_last_df = df
        else:
            with buffer_lock:
                global_last_df = None

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port, 60)

    # Main loop: run while run_mqtt_flag is True
    while True:
        with flag_lock:
            if not run_mqtt_flag:
                break
        client.loop(timeout=1)

    client.disconnect()
    with buffer_lock:
        log_buffer.append("MQTT listener stopped")


# ======================================================
# STREAMLIT UI (ONLY here we use st/session_state)
# ======================================================

st.title("📡 AC Dictionary → JSON → Live MQTT Parser")

# Session-state only for storing dictionary JSON
if "parser_json" not in st.session_state:
    st.session_state["parser_json"] = None

# -------------------------------
# INPUTS
# -------------------------------
device_name = st.text_input("Device Name", value="EZMCSACD00001")
mqtt_topic = st.text_input("MQTT Subscriber Topic", value=f"/AC/2/{device_name}/Datalog")

uploaded_excel = st.file_uploader("Upload Dictionary Excel", type=["xlsx"])

# Convert Excel → JSON
if uploaded_excel and st.button("Convert Excel → JSON"):
    try:
        registers = excel_to_json(uploaded_excel)
        st.session_state["parser_json"] = registers

        st.success("✅ Dictionary JSON generated from Excel")
        st.json(registers[:5])

        st.download_button(
            "Download dictionary.json",
            json.dumps(registers, indent=2),
            "dictionary.json",
            "application/json",
        )
    except Exception as e:
        st.error(f"Error during conversion: {e}")

st.markdown("---")
st.header("Live MQTT Parser")

broker = st.text_input("MQTT Broker", value="ecozen.ai")
port = st.number_input("MQTT Port", value=1883)

# Output containers
st.subheader("Latest RAW Packet")
raw_output_box = st.empty()

st.subheader("Latest Parsed DataFrame")
parsed_output_box = st.empty()

st.subheader("Logs")
logs_box = st.empty()

# -------------------------------
# CONTROL BUTTONS
# -------------------------------
col1, col2, col3, col4 = st.columns(4)

# START
if col1.button("▶ Start Listening"):
    if not st.session_state["parser_json"]:
        st.error("Please convert an Excel dictionary first!")
    else:
        df_dict = pd.DataFrame(st.session_state["parser_json"])

        with flag_lock:
            run_mqtt_flag = True
            parse_enabled_flag = True

        # Start background thread
        t = threading.Thread(
            target=mqtt_listener,
            args=(broker, int(port), mqtt_topic, df_dict),
            daemon=True,
        )
        t.start()

        st.success(f"Started listening on: {mqtt_topic}")

# PAUSE PARSING (still receives packets)
if col2.button("⏸ Pause Parsing"):
    with flag_lock:
        parse_enabled_flag = False
    st.info("Parsing paused. MQTT client still receiving packets.")

# RESUME PARSING
if col3.button("▶ Resume Parsing"):
    with flag_lock:
        parse_enabled_flag = True
    st.success("Parsing resumed.")

# STOP
if col4.button("⏹ Stop Listener"):
    with flag_lock:
        run_mqtt_flag = False
        parse_enabled_flag = True
    st.warning("Requested listener to stop.")


# -------------------------------
# DISPLAY LATEST DATA
# -------------------------------
with buffer_lock:
    # Logs
    if log_buffer:
        logs_text = "\n".join(log_buffer[-30:])
        logs_box.code(logs_text)

    # Latest raw packet
    if global_last_raw is not None:
        raw_output_box.code(global_last_raw)

    # Latest parsed dataframe
    if global_last_df is not None:
        parsed_output_box.dataframe(global_last_df)
