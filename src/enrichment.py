# src/enrichment.py

import json
import os
import sqlite3
import sys
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import xml.etree.ElementTree as ET

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Rutas ────────────────────────────────────────────────────────────────────
# Raíz del proyecto = carpeta padre de src/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLEANED_XLSX = os.path.join(ROOT, "src", "xlsx", "cleaned_data.xlsx")
ENRICHED_XLSX = os.path.join(ROOT, "src", "xlsx", "enriched_data.xlsx")
AUDIT_PATH = os.path.join(ROOT, "src", "static", "auditoria", "enrichment_report.txt")

SRC = os.path.join(ROOT, "src", "sources")
CSV_PATH = f"{SRC}/poblacion.csv"
JSON_PATH = f"{SRC}/categorias.json"
TXT_PATH = f"{SRC}/regiones.txt"
XML_PATH = f"{SRC}/pib.xml"
HTML_PATH = f"{SRC}/desempleo.html"

SAMPLE = 1000


def ensure_dirs():
    for path in [ENRICHED_XLSX, AUDIT_PATH]:
        os.makedirs(os.path.dirname(path), exist_ok=True)


# ── Carga del dataset base ────────────────────────────────────────────────────
def load_base() -> pd.DataFrame:
    df = pd.read_excel(CLEANED_XLSX)
    print(f"[BASE] Registros cargados: {len(df)}")
    return df


# ── Lectura de fuentes adicionales ───────────────────────────────────────────
def read_csv() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH)
    df = df[["cod_depto", "poblacion_2023"]]
    print(f"[CSV]  poblacion.csv          → {len(df)} registros")
    return df


def read_json() -> pd.DataFrame:
    with open(JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)[["cod_depto", "categoria_departamento", "num_municipios"]]
    print(f"[JSON] categorias.json        → {len(df)} registros")
    return df


def read_txt() -> pd.DataFrame:
    df = pd.read_csv(TXT_PATH, sep="|")
    df = df[["cod_depto", "region"]]
    print(f"[TXT]  regiones.txt           → {len(df)} registros")
    return df


def read_xml() -> pd.DataFrame:
    tree = ET.parse(XML_PATH)
    root = tree.getroot()
    rows = []
    for dep in root.findall("departamento"):
        rows.append(
            {
                "cod_depto": int(dep.find("cod_depto").text),
                "pib_percapita_cop": int(dep.find("pib_percapita_cop").text),
                "pib_total_millones_cop": int(dep.find("pib_total_millones_cop").text),
            }
        )
    df = pd.DataFrame(rows)
    print(f"[XML]  pib.xml                → {len(df)} registros")
    return df


def read_html() -> pd.DataFrame:
    tables = pd.read_html(HTML_PATH)
    df = tables[0][["cod_depto", "tasa_desempleo_pct", "poblacion_ocupada"]]
    print(f"[HTML] desempleo.html         → {len(df)} registros")
    return df


# ── Integración con PySpark ───────────────────────────────────────────────────
def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("enriquecimiento-hurtos")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def enrich(spark, base, csv_df, json_df, txt_df, xml_df, html_df):
    # Convertir todo a Spark
    sdf = spark.createDataFrame(base)
    sdf_csv = spark.createDataFrame(csv_df)
    sdf_json = spark.createDataFrame(json_df)
    sdf_txt = spark.createDataFrame(txt_df)
    sdf_xml = spark.createDataFrame(xml_df)
    sdf_html = spark.createDataFrame(html_df)

    # Asegurar tipo de clave en todas las fuentes
    for s in [sdf_csv, sdf_json, sdf_txt, sdf_xml, sdf_html]:
        s = s.withColumn("cod_depto", F.col("cod_depto").cast("int"))

    sdf = sdf.withColumn("cod_depto", F.col("cod_depto").cast("int"))

    # Joins sucesivos (left join para conservar todos los registros base)
    sdf = sdf.join(sdf_csv, on="cod_depto", how="left")
    sdf = sdf.join(sdf_json, on="cod_depto", how="left")
    sdf = sdf.join(sdf_txt, on="cod_depto", how="left")
    sdf = sdf.join(sdf_xml, on="cod_depto", how="left")
    sdf = sdf.join(sdf_html, on="cod_depto", how="left")

    # Columna derivada: tasa de hurtos por cada 100.000 habitantes
    sdf = sdf.withColumn(
        "hurtos_por_100k",
        F.round((F.col("cantidad") / F.col("poblacion_2023")) * 100000, 4),
    )

    return sdf


# ── Exportar resultados ───────────────────────────────────────────────────────
def export_excel(sdf):
    df = sdf.limit(SAMPLE).toPandas()
    df["fecha_hecho"] = df["fecha_hecho"].astype(str)
    df.to_excel(ENRICHED_XLSX, index=False)
    print(f"[OUT]  enriched_data.xlsx generado con {len(df)} filas → {ENRICHED_XLSX}")
    return df


# ── Reporte de auditoría ──────────────────────────────────────────────────────
def generate_audit(base_count, enriched_count, sdf, sources_info):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Coincidencias por fuente (registros sin nulo en columna clave de cada fuente)
    matches = {
        "poblacion.csv   (poblacion_2023)": sdf.filter(
            F.col("poblacion_2023").isNotNull()
        ).count(),
        "categorias.json (categoria_departamento)": sdf.filter(
            F.col("categoria_departamento").isNotNull()
        ).count(),
        "regiones.txt    (region)": sdf.filter(F.col("region").isNotNull()).count(),
        "pib.xml         (pib_percapita_cop)": sdf.filter(
            F.col("pib_percapita_cop").isNotNull()
        ).count(),
        "desempleo.html  (tasa_desempleo_pct)": sdf.filter(
            F.col("tasa_desempleo_pct").isNotNull()
        ).count(),
    }

    columnas_nuevas = [
        "poblacion_2023",
        "categoria_departamento",
        "num_municipios",
        "region",
        "pib_percapita_cop",
        "pib_total_millones_cop",
        "tasa_desempleo_pct",
        "poblacion_ocupada",
        "hurtos_por_100k",
    ]

    lines = [
        "=" * 65,
        f"REPORTE DE ENRIQUECIMIENTO DE DATOS - {now}",
        "=" * 65,
        "",
        "--- FUENTES ADICIONALES INTEGRADAS ---",
    ]
    for nombre, registros in sources_info.items():
        lines.append(f"  {nombre:<35}: {registros} registros")

    lines += [
        "",
        "--- REGISTROS COINCIDENTES POR FUENTE (left join sobre cod_depto) ---",
    ]
    for fuente, count in matches.items():
        lines.append(f"  {fuente}: {count} / {enriched_count}")

    lines += [
        "",
        "--- COLUMNAS NUEVAS INCORPORADAS ---",
    ]
    for col in columnas_nuevas:
        lines.append(f"  + {col}")

    lines += [
        "",
        "--- TRANSFORMACIONES APLICADAS ---",
        "  [1] cod_depto casteado a IntegerType en todas las fuentes",
        "  [2] Left join sucesivo por cod_depto (5 fuentes)",
        "  [3] Columna derivada: hurtos_por_100k = (cantidad / poblacion_2023) * 100000",
        "",
        "--- RESUMEN ---",
        f"  Registros dataset base     : {base_count}",
        f"  Registros dataset enriquec.: {enriched_count}",
        f"  Columnas base              : 7",
        f"  Columnas tras enriquec.    : {7 + len(columnas_nuevas)}",
        f"  Muestra exportada (xlsx)   : {SAMPLE} registros",
        "",
        "  Estado: ENRIQUECIMIENTO COMPLETADO EXITOSAMENTE",
        "=" * 65,
    ]

    content = "\n".join(lines)
    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write(content)
    print(content)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ensure_dirs()

    print("\n=== Cargando dataset base ===")
    base = load_base()
    base_count = len(base)

    print("\n=== Leyendo fuentes adicionales ===")
    csv_df = read_csv()
    json_df = read_json()
    txt_df = read_txt()
    xml_df = read_xml()
    html_df = read_html()

    sources_info = {
        "poblacion.csv  (CSV)": len(csv_df),
        "categorias.json (JSON)": len(json_df),
        "regiones.txt   (TXT)": len(txt_df),
        "pib.xml        (XML)": len(xml_df),
        "desempleo.html (HTML)": len(html_df),
    }

    print("\n=== Iniciando sesión PySpark ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== Ejecutando integración (joins) ===")
    sdf_enriched = enrich(spark, base, csv_df, json_df, txt_df, xml_df, html_df)
    enriched_count = sdf_enriched.count()
    print(f"[JOIN] Registros tras enriquecimiento: {enriched_count}")

    print("\n=== Exportando Excel ===")
    export_excel(sdf_enriched)

    print("\n=== Generando reporte de auditoría ===")
    generate_audit(base_count, enriched_count, sdf_enriched, sources_info)

    spark.stop()
    print("\nEnriquecimiento completado.")
