# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 16:16:55 2025

@author: Admin
"""

import pandas as pd
from datetime import datetime
import json

def load_json_dict(path):
    with open(path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def convert_json_dict_to_parser_df(json_path):
    import json
    import pandas as pd

    with open(json_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Rename JSON keys → Parser expected column names
    df = df.rename(columns={
        "short_name": "Short name",
        "index": "Index",
        "size": "Size [byte]",
        "format": "Data format",
        "signed": "Signed/Unsigned",
        "scaling": "Scaling factor",
        "offset": "Offset"
    })

    # Convert boolean signed → "S"/"U"
    df["Signed/Unsigned"] = df["Signed/Unsigned"].apply(lambda x: "S" if x else "U")

    return df

df_dict_ecofrost = load_json_dict("ecofrost.json")
df_dict_deye     = load_json_dict("deye.json")
df_dict_sunnal   = load_json_dict("sunnal.json")

# # ECOFROST Dictionary
# URL_DICT_ECOFROST = r"G:\.shortcut-targets-by-id\1Rv5M8FXZMFkA686BaMBpTjhmSXwrJND-\IoT Shared Folder\Users\Jaideep\Backlogs and tools\ST_main\Ecozen - Ecofrost Data Dictionary.xlsx"

# df_dict_ecofrost = pd.read_excel(
#     URL_DICT_ECOFROST,
#     sheet_name="Sheet1"
# )

# # Deye Dictionary
# URL_DICT_DEYE = r"G:\.shortcut-targets-by-id\1Rv5M8FXZMFkA686BaMBpTjhmSXwrJND-\IoT Shared Folder\Users\Jaideep\Backlogs and tools\ST_main\Solar_AC_data_dictionary_version_1_deye_updated.xlsx"

# df_dict_deye = pd.read_excel(
#     URL_DICT_DEYE
# )

# # Sunnal Dictionary
# URL_DICT_SUNNAL = r"G:\.shortcut-targets-by-id\1Rv5M8FXZMFkA686BaMBpTjhmSXwrJND-\IoT Shared Folder\Users\Jaideep\Backlogs and tools\ST_main\Solar_AC_data_dictionary_version_3.xlsx"

# df_dict_sunnal = pd.read_excel(
#     URL_DICT_SUNNAL
# )
# %%
# ---------------------- EXTRACT SEGMENT ----------------------

def extract_segment(raw_str, row):
    start = int(row["Index"])
    size = int(row["Size [byte]"])
    return raw_str[start:start + size]

# ---------------------- PARSE SEGMENT ----------------------

def parse_value(segment, row):
    fmt = str(row["Data format"]).strip().upper()
    signed = str(row["Signed/Unsigned"]).lower() == "s"
    scaling = row["Scaling factor"] if pd.notnull(row["Scaling factor"]) else 1

    if fmt == "ASCII":
        return segment.strip()

    elif fmt == "BIN":
        return segment.strip()

    elif fmt == "DEC":
        try:
            val = int(segment.strip())
            return val * scaling
        except:
            return segment

    else:  # default fallback
        return segment

# ---------------------- CUSTOM LOGIC ----------------------

def apply_custom_logic(short_name, parsed_val):
    if short_name == "RES2":
        return str(parsed_val)[-3:]

    elif short_name == "W_STAT":
        WSTAT_MAP = {
            0: "Power On", 1: "Test", 2: "Stand By", 3: "Battery Mode",
            4: "Line Mode", 5: "Bypass", 6: "Fault Mode", 7: "ShutDown"
        }
        return WSTAT_MAP.get(parsed_val, parsed_val)

    elif short_name in ["INT TIME", "INT_TIME"]:
        try:
            return datetime.strptime(parsed_val, "%d%m%y%H%M%S")
        except:
            return parsed_val

    return parsed_val


# ---------------------- PROCESS ONE ROW ----------------------

def process_register_row(row, raw_str):
    seg = extract_segment(raw_str, row)
    val = parse_value(seg, row)
    val = apply_custom_logic(row["Short name"], val)
    return row["Short name"], val


# ---------------------- FULL PACKET PARSER ----------------------

def process_all_registers(df_dict, raw_str):
    result = {}

    for _, row in df_dict.iterrows():
        key = row["Short name"]
        try:
            _, val = process_register_row(row, raw_str)
            result[key] = val
        except Exception:
            result[key] = None

    # derived fields
    if "BATT_V" in result and "BATT_I" in result:
        try:
            result["BATT_W"] = result["BATT_V"] * result["BATT_I"]
        except:
            pass

    return pd.DataFrame([result])

def apply_dataformat_conversion(df_out, df_dict):
    """
    df_out  -> DataFrame containing extracted HEX or raw values
    df_dict -> dictionary containing Data format rules
    
    Returns -> df_final with fully converted values
    """
    df_final = df_out.copy()

    # For each short name defined in the dictionary
    for _, row in df_dict.iterrows():

        key = row["Short name"]

        # if df_out does not contain this key, skip
        if key not in df_final.columns:
            continue

        val = df_final.at[0, key]     # raw extracted value
        if pd.isna(val):
            continue

        fmt = str(row["Data format"]).strip().upper()
        signed = str(row.get("Signed/Unsigned", "U")).strip().upper() == "S"
        scaling = row["Scaling factor"] if pd.notnull(row["Scaling factor"]) else 1

        # ------------------------------------------------
        # 1) ASCII
        # ------------------------------------------------
        if fmt == "ASCII":
            # value is already ASCII
            df_final.at[0, key] = str(val).strip()
            continue

        # ------------------------------------------------
        # 2) BIN → value may be hex or already binary
        # ------------------------------------------------
        if fmt in ["BIN", "BINARY"]:
            try:
                hex_clean = str(val).replace(" ", "")
                b = bytes.fromhex(hex_clean)
                df_final.at[0, key] = ''.join(f"{x:08b}" for x in b)
            except:
                df_final.at[0, key] = val  # fallback
            continue

        # ------------------------------------------------
        # 3) DEC → hex to decimal
        # ------------------------------------------------
        if fmt == "DEC":
            try:
                hex_clean = str(val).replace(" ", "")
                b = bytes.fromhex(hex_clean)
                num = int.from_bytes(b, byteorder="big", signed=signed)
                df_final.at[0, key] = num * scaling
            except:
                df_final.at[0, key] = val
            continue

        # ------------------------------------------------
        # 4) DEFAULT → leave as is
        # ------------------------------------------------
        df_final.at[0, key] = val

    return df_final

# %%
# Dictionary validation

def validate_dict_by_length(raw_hex, df_dict, dict_name="(unknown)"):
    """
    Validate that the hex string is long enough for the given dictionary.
    Uses HEX-CHARACTER indexing directly (no space removal).
    
    RETURNS:
        (fits_flag: bool, max_required_index: int)
    """

    hex_len = len(raw_hex)   # raw_hex contains spaces and they are counted

    print(f"\n===== LENGTH VALIDATION → {dict_name} =====")
    print(f"Packet hex length:     {hex_len}")

    max_required = 0

    for _, row in df_dict.iterrows():

        # Skip invalid rows
        if pd.isna(row.get("Index")) or pd.isna(row.get("Size [byte]")):
            continue

        try:
            idx = int(row["Index"])             # hex-character index
            size = int(row["Size [byte]"])      # hex-character count
        except:
            continue

        end = idx + size

        if end > max_required:
            max_required = end

        # If dictionary references past the packet length → fail immediately
        if end > hex_len:
            print(f"❌ FAIL at Short name: {row.get('Short name')}")
            print(f"   Index={idx}, Size={size}, End={end}, PacketLen={hex_len}")
            print("   → This dictionary CANNOT match this packet.")
            return False, max_required

    print(f"Dictionary requires upto hex index: {max_required}")

    if max_required <= hex_len:
        print("✔ PASS → Packet is long enough for this dictionary.")
        return True, max_required
    else:
        print("❌ FAIL → Packet is too short for this dictionary.")
        return False, max_required


def score_datatype_match(raw_hex, df_dict, sample_limit=12):
    sample_hex = raw_hex  # do NOT strip spaces
    total_score = 0
    checks = 0

    for _, row in df_dict.iterrows():
        if checks >= sample_limit:
            break

        if pd.isna(row["Index"]) or pd.isna(row["Size [byte]"]) or pd.isna(row["Data format"]):
            continue

        idx = int(row["Index"])
        size = int(row["Size [byte]"])
        fmt = str(row["Data format"]).strip().upper()
        signed_flag = str(row.get("Signed/Unsigned", "U")).strip().upper()
        key = str(row.get("Short name", "")).upper()

        hex_seg_raw = sample_hex[idx : idx + size]
        hex_seg = hex_seg_raw.replace(" ", "")

        # must be even-length for bytes
        if len(hex_seg) % 2 != 0:
            total_score -= 5
            continue

        try:
            b = bytes.fromhex(hex_seg)
        except:
            total_score -= 5
            continue

        # ---- FORMAT VALIDATION ----
        if fmt == "ASCII":
            if all(32 <= c <= 126 for c in b):
                total_score += 3
            else:
                total_score -= 3

        elif fmt == "DEC":
            try:
                unsigned_val = int.from_bytes(b, "big", signed=False)
                signed_val   = int.from_bytes(b, "big", signed=True)
                total_score += 2
            except:
                total_score -= 4
                continue

            # SIGNED/UNSIGNED VALIDATION
            if signed_flag == "S":
                if not (-(2**(8*len(b)-1)) <= signed_val <= (2**(8*len(b)-1)-1)):
                    total_score -= 3
                else:
                    total_score += 1
            else:
                if unsigned_val < 0:
                    total_score -= 3
                else:
                    total_score += 1

            # GENERIC HEURISTICS
            if "V" in key:
                if 0 <= unsigned_val <= 1000:
                    total_score += 2
                else:
                    total_score -= 2
            elif "CUR" in key or "I" == key:
                if 0 <= unsigned_val <= 500:
                    total_score += 2
                else:
                    total_score -= 2
            elif "W" in key or "PWR" in key:
                if 0 <= unsigned_val <= 10_000_000:
                    total_score += 2
                else:
                    total_score -= 2
            elif "TEMP" in key or "T" == key:
                if -100 <= signed_val <= 200:
                    total_score += 2
                else:
                    total_score -= 2
            else:
                # generic fallback
                if len(b) <= 2 and unsigned_val > 20000:
                    total_score -= 2
                if len(b) <= 4 and unsigned_val > 1e9:
                    total_score -= 2

        elif fmt == "BINARY":
            bitstr = ''.join(f"{x:08b}" for x in b)
            if len(b) > 4:
                total_score -= 2
            elif bitstr.count("0") in (0, len(bitstr)):
                total_score -= 3
            else:
                total_score += 2

        checks += 1

    return total_score

def guess_dictionary(raw_hex, dict_list):
    """
    Select the best dictionary based on:
        1. Exact length match (strongest)
        2. Closest fit (len_required <= packet_len)
        3. Datatype validation (only if needed)

    dict_list = [(name, df_dict), ...]
    """

    packet_len = len(raw_hex)

    # STEP 1 — Collect length info
    results = []  # [(name, df, fits, required_len), ...]

    for name, df_dict in dict_list:
        fits, req_len = validate_dict_by_length(raw_hex, df_dict, name)
        results.append((name, df_dict, fits, req_len))

    # Filter those that fit
    fits_list = [(n, d, r) for (n, d, f, r) in results if f]
    if not fits_list:
        raise ValueError("❌ No dictionaries fit the packet length.")

    # STEP 2 — Exact length match wins immediately
    exact_matches = [(n, d, r) for (n, d, r) in fits_list if r == packet_len]
    if len(exact_matches) == 1:
        print(f"✔ Selected → {exact_matches[0][0]} (Exact length match)")
        return exact_matches[0][0], exact_matches[0][1]

    if len(exact_matches) > 1:
        print("⚠ Multiple exact matches → proceed to datatype validation.")
        # Fall through to datatype validation

    # STEP 3 — Choose closest match (largest required_len)
    if len(exact_matches) == 0:
        # All fits but none exact
        max_req = max(r for (_, _, r) in fits_list)
        closest = [(n, d, r) for (n, d, r) in fits_list if r == max_req]

        if len(closest) == 1:
            print(f"✔ Selected → {closest[0][0]} (Closest length match)")
            return closest[0][0], closest[0][1]

        print("⚠ Multiple candidates equally close → proceed to datatype validation.")
        # Fall through to datatype validation
        candidates_for_type_check = closest
    else:
        candidates_for_type_check = exact_matches

    # STEP 4 — Datatype validation for ambiguous choices
    print("\n===== DATATYPE SCORING =====")

    scores = []
    for name, df_dict, req in candidates_for_type_check:
        score = score_datatype_match(raw_hex, df_dict)
        scores.append((score, name, df_dict))
    
    scores.sort(reverse=True)  # highest score first
    best_score, best_name, best_dict = scores[0]
    
    print(f"Scores: {scores}")
    print(f"✔ Selected → {best_name} (highest datatype score)")
    
    return best_name, best_dict
# %%

# raw_packet = "NEWECOFROST10000       Jio             JIOCIOT10001400 0.00000 0.000008991864050706043301500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003737373737373838383838383.0.01R03A03M08_OCPE012201A131             0047880000000000000000000000000000000000000000000000000000ff01012f003903045ad2048713                                ECOFROST1.E.41\xff\x02U\xff\xff\xff0xFFFFFFFF0012092500035200009f0c00000000b1004b1166:23:02:41000000000000000096261100000020000000000000a10355031901000000000803000000000000000008030500000000000000a8091800040000000000f800180300000000000074ff00000000b6c900010405010202110203000001621f11010000100000000002020200000000000000020000020000000000000011005a1e0300ff88138813fa00740905000f002800080000000000290002000000000101010101010100000101010101000000000000000000000000000000000000000000000000000000000000000000"
# raw_packet = "0100000000000000000000000000000000000000000000000000000000000000000000000000000801000000000000000000000000008c000000000000083508350850053401ba02120b13fff2fb9a06ad06ad04e20aff000e06ad0000000000000000fc34f28013931393000100100000010118015d00056a000500000000000000000c070525121328200000000f0e74        IOT.COM0000204    AIRTELR02A07M08_OCP8991900992480943928F866082076503862020500020000000000000004"
# raw_packet = "010003000000000a7f019001040064000a0000089801f40042004d0001000000dc1d57000100011068106800e6001200f000e601f4001200000000159f00010002000000010000000a00dc000a00e6010e0124010e00c80124003c0078001e000b00270000000000050001000000000000fffffff60000000000000000000000000000000000000636fffffbf00000012e000000000000000000000c20112512292225000050830c80        IOT.COM0000210    AIRTELR02A07M08_OCP8991900992665204997F860738070457357020100020000000000000001"
# raw_packet = "01000200000004ff6b00000000328e000100000000056500000000000000040000328e00000000080100000000000000000000000000460a7c0004000000000000000000000000000000006b00000d08fc091200000000006400000000006c006c006c04e20ab40064006c006b000000000000fff8ffe1138813880000001000000000000000000000000000000000000000000c201125122730190000529f0bb8        IOT.COM0000104    AIRTELR02A07M08_OCP8991900992613444414F860738070197342020100020000000000000001"
# dicts = [
#     ("sunnal", convert_json_dict_to_parser_df("sunnal.json")),
#     ("deye", convert_json_dict_to_parser_df("deye.json")),
#     ("ecofrost", convert_json_dict_to_parser_df("ecofrost.json"))
# ]


# name, df_dict_selected = guess_dictionary(raw_packet, dicts)

# df = df_dict_selected
# print("Name",name)
# df_out = process_all_registers(df, raw_packet)
# df_final = apply_dataformat_conversion(df_out, df)
