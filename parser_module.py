# parser_module.py
import pandas as pd

def process_all_registers(df_dict, raw_packet):
    rows = []
    for _, row in df_dict.iterrows():
        idx = row["index"]
        size = row["size"]
        segment = raw_packet[idx : idx + size]

        rows.append({
            "Short name": row["short_name"],
            "Raw": segment,
            "format": row["format"],
            "scaling": row["scaling"],
            "offset": row["offset"]
        })
    return pd.DataFrame(rows)

def apply_conversion(df):
    df = df.copy()
    df["Value"] = df["Raw"]
    return df

def parse_packet(raw_packet, df_dict):
    df1 = process_all_registers(df_dict, raw_packet)
    df2 = apply_conversion(df1)
    return df2
