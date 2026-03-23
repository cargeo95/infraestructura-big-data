# src/cleaning.py

import os
import sqlite3
import sys
from datetime import datetime

# PySpark necesita saber el ejecutable Python exacto (critico en Windows)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

DB_PATH = "src/db/ingestion.db"
XLSX_PATH = "src/xlsx/cleaned_data.xlsx"
AUDIT_PATH = "src/static/auditoria/cleaning_report.txt"
SAMPLE = 1000


def ensure_dirs():
    for path in [XLSX_PATH, AUDIT_PATH]:
        os.makedirs(os.path.dirname(path), exist_ok=True)


def load_from_db() -> pd.DataFrame:
    """Carga los datos desde SQLite simulando extraccion desde entorno cloud."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM hechos", conn)
    conn.close()
    print(f"Datos cargados desde base de datos: {len(df)} registros")
    return df


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("limpieza-hurtos")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def explore(df) -> dict:
    """Analisis exploratorio: nulos, duplicados y estadisticas iniciales."""
    initial_count = df.count()

    # Conteo de nulos en una sola pasada (solo NULL; strings vacios se manejan en limpieza)
    null_row = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0]
    null_counts = null_row.asDict()

    # Duplicados excluyendo la columna id (autoincrement)
    subset = [c for c in df.columns if c != "id"]
    dup_count = initial_count - df.dropDuplicates(subset).count()

    return {
        "initial_count": initial_count,
        "null_counts": null_counts,
        "dup_count": dup_count,
    }


def clean(df, stats: dict):
    """Aplica todas las operaciones de limpieza y retorna el DataFrame limpio y el log."""
    log = []
    subset = [c for c in df.columns if c != "id"]

    # -------------------------------------------------------------------
    # 1. Eliminacion de duplicados
    # -------------------------------------------------------------------
    df = df.dropDuplicates(subset)
    after_dedup = df.count()
    removed_dups = stats["initial_count"] - after_dedup
    log.append(f"[1] Eliminacion de duplicados  : {removed_dups} registros eliminados")

    # -------------------------------------------------------------------
    # 2. Correccion de tipos de datos
    # -------------------------------------------------------------------
    # fecha_hecho: TEXT -> TimestampType (formato: "2010-01-01T00:00:00.000")
    df = df.withColumn("fecha_hecho", F.to_timestamp(F.col("fecha_hecho")))
    # cantidad: REAL -> IntegerType (es un conteo de eventos)
    df = df.withColumn("cantidad", F.col("cantidad").cast(IntegerType()))
    # cod_depto / cod_muni: garantizar string y eliminar espacios
    df = df.withColumn("cod_depto", F.trim(F.col("cod_depto").cast("string")))
    df = df.withColumn("cod_muni",  F.trim(F.col("cod_muni").cast("string")))
    log.append("[2] Correccion de tipos        : fecha_hecho->timestamp | cantidad->integer | cod_depto/cod_muni->string trimmed")

    # -------------------------------------------------------------------
    # 3. Manejo de valores nulos
    # -------------------------------------------------------------------
    # 3a. Eliminar filas sin fecha (dato critico no imputable)
    before = df.count()
    df = df.filter(F.col("fecha_hecho").isNotNull())
    removed_dates = before - df.count()
    log.append(f"[3a] Nulos en fecha_hecho      : {removed_dates} registros eliminados")

    # 3b. Imputar nulos en campos categoricos
    df = df.fillna({
        "departamento": "DESCONOCIDO",
        "municipio":    "DESCONOCIDO",
        "cod_depto":    "00",
        "cod_muni":     "00000",
    })
    log.append("[3b] Imputacion categoricos    : departamento/municipio -> 'DESCONOCIDO' | cod_depto -> '00' | cod_muni -> '00000'")

    # 3c. Imputar nulos en cantidad con 0
    df = df.fillna({"cantidad": 0})
    log.append("[3c] Imputacion cantidad       : nulos -> 0")

    # -------------------------------------------------------------------
    # 4. Normalizacion de texto
    # -------------------------------------------------------------------
    df = df.withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
    df = df.withColumn("municipio",    F.upper(F.trim(F.col("municipio"))))
    log.append("[4]  Normalizacion texto       : departamento y municipio en mayusculas sin espacios")

    final_count = df.count()
    return df, final_count, log


def generate_excel(df_spark):
    df_sample = df_spark.limit(SAMPLE).toPandas()
    df_sample["fecha_hecho"] = df_sample["fecha_hecho"].astype(str)
    df_sample.to_excel(XLSX_PATH, index=False)
    print(f"Excel generado: {XLSX_PATH} ({len(df_sample)} filas)")


def generate_audit(stats: dict, final_count: int, op_log: list):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    initial = stats["initial_count"]
    null_counts = stats["null_counts"]
    dup_count = stats["dup_count"]

    lines = [
        "=" * 60,
        f"REPORTE DE LIMPIEZA DE DATOS - {now}",
        "=" * 60,
        f"Fuente           : src/db/ingestion.db (tabla: hechos)",
        "",
        "--- ESTADO ANTES DE LA LIMPIEZA ---",
        f"Registros totales    : {initial}",
        f"Duplicados detectados: {dup_count}",
        "Valores nulos por columna:",
    ]
    for col, count in null_counts.items():
        lines.append(f"  {col:<15}: {count}")

    lines += [
        "",
        "--- OPERACIONES REALIZADAS ---",
    ]
    lines.extend(op_log)

    lines += [
        "",
        "--- ESTADO DESPUES DE LA LIMPIEZA ---",
        f"Registros finales    : {final_count}",
        f"Registros eliminados : {initial - final_count}",
        "",
        f"Estado: LIMPIEZA COMPLETADA EXITOSAMENTE",
        "=" * 60,
    ]

    content = "\n".join(lines)
    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write(content)
    print(content)


if __name__ == "__main__":
    ensure_dirs()

    print("Cargando datos desde base de datos (entorno cloud simulado)...")
    df_pandas = load_from_db()

    print("Iniciando sesion PySpark...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.createDataFrame(df_pandas)

    print("Realizando analisis exploratorio...")
    stats = explore(df)
    print(f"  Registros iniciales : {stats['initial_count']}")
    print(f"  Duplicados          : {stats['dup_count']}")

    print("Aplicando limpieza y transformaciones...")
    df_clean, final_count, op_log = clean(df, stats)

    print("Exportando muestra a Excel...")
    generate_excel(df_clean)

    print("Generando reporte de auditoria...")
    generate_audit(stats, final_count, op_log)

    spark.stop()
    print("Limpieza completada.")
