# src/ingestion.py

import os
import sqlite3
from datetime import datetime

import pandas as pd
import requests

API_URL = "https://www.datos.gov.co/resource/4rxi-8m8d.json"
DB_PATH = "src/db/ingestion.db"
XLSX_PATH = "src/xlsx/ingestion.xlsx"
AUDIT_PATH = "src/static/auditoria/ingestion.txt"
LIMIT = 50000
SAMPLE = 1000


def ensure_dirs():
    for path in [DB_PATH, XLSX_PATH, AUDIT_PATH]:
        os.makedirs(os.path.dirname(path), exist_ok=True)


def fetch_all_records():
    records = []
    offset = 0

    while True:
        params = {"$limit": LIMIT, "$offset": offset, "$order": "fecha_hecho"}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()

        batch = response.json()
        if not batch:
            break

        records.extend(batch)
        offset += len(batch)
        print(f"Registros descargados: {offset}")

        if len(batch) < LIMIT:
            break

    print(f"Total extraido: {len(records)}")
    return records


def save_to_db(records):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS hechos")
    cur.execute("""
        CREATE TABLE hechos (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_hecho   TEXT,
            cod_depto     TEXT,
            departamento  TEXT,
            cod_muni      TEXT,
            municipio     TEXT,
            cantidad      REAL
        )
    """)

    rows = [
        (
            r.get("fecha_hecho"),
            r.get("cod_depto"),
            r.get("departamento"),
            r.get("cod_muni"),
            r.get("municipio"),
            r.get("cantidad"),
        )
        for r in records
    ]

    cur.executemany(
        """
        INSERT INTO hechos (fecha_hecho, cod_depto, departamento, cod_muni, municipio, cantidad)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        rows,
    )

    conn.commit()
    count = cur.execute("SELECT COUNT(*) FROM hechos").fetchone()[0]
    conn.close()

    print(f"Registros en base de datos: {count}")
    return count


def generate_excel(records):
    df = pd.DataFrame(records).head(SAMPLE)
    df.to_excel(XLSX_PATH, index=False)
    print(f"Archivo Excel generado con {len(df)} filas")


def generate_audit(api_count, db_count):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    diff = api_count - db_count

    lines = [
        "=" * 60,
        f"AUDITORIA DE INGESTA - {now}",
        "=" * 60,
        f"API URL        : {API_URL}",
        f"Registros API  : {api_count}",
        f"Registros BD   : {db_count}",
        f"Diferencia     : {diff}",
        "-" * 60,
        f"Estado         : {'OK - Integridad confirmada' if diff == 0 else 'ALERTA - Diferencia detectada'}",
        "=" * 60,
    ]

    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("\n".join(lines))


if __name__ == "__main__":
    ensure_dirs()
    records = fetch_all_records()
    db_count = save_to_db(records)
    generate_excel(records)
    generate_audit(len(records), db_count)
    print("Ingesta completada")
