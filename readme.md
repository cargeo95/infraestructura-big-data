# Infraestructura Big Data - Ingesta, Limpieza y Enriquecimiento de Datos

**Estudiante:** Carlos Arturo Gómez Jiménez  
**Cédula:** 1020808609  
**Repositorio:** https://github.com/cargeo95/infraestructura-big-data

---

## Descripcion

Proyecto de pipeline de datos desde la API publica del gobierno colombiano
(datos.gov.co), específicamente el dataset de hurtos con aproximadamente 630.000
registros. El pipeline cubre tres etapas: ingesta, limpieza y enriquecimiento.
Los datos se almacenan en SQLite y se generan muestras en Excel junto con
archivos de auditoria que validan la integridad de cada proceso.

---

## Estructura del Proyecto

```
infraestructura-big-data/
├── setup.py
├── README.md
├── .github/
│   └── workflows/
│       └── bigdata.yml
└── src/
    ├── ingestion.py
    ├── cleaning.py
    ├── enrichment.py
    ├── sources/
    │   ├── poblacion.csv
    │   ├── categorias.json
    │   ├── regiones.txt
    │   ├── pib.xml
    │   └── desempleo.html
    ├── static/
    │   └── auditoria/
    │       ├── ingestion.txt
    │       ├── cleaning_report.txt
    │       └── enrichment_report.txt
    ├── db/
    │   └── ingestion.db
    └── xlsx/
        ├── ingestion.xlsx
        ├── cleaned_data.xlsx
        └── enriched_data.xlsx
```

---

## Requisitos

- Python 3.11 o superior
- Java 11 (requerido por PySpark)
- pip

---

## Instalacion

Clonar el repositorio:

```bash
git clone https://github.com/cargeo95/infraestructura-big-data.git
cd infraestructura-big-data
```

Instalar dependencias:

```bash
pip install requests pandas openpyxl pyspark lxml html5lib beautifulsoup4
```

---

## Ejecucion local

Las tres etapas deben ejecutarse en orden:

```bash
python src/ingestion.py
python src/cleaning.py
python src/enrichment.py
```

### Archivos generados por etapa

**Ingesta:**
- `src/db/ingestion.db` — base de datos SQLite con todos los registros
- `src/xlsx/ingestion.xlsx` — muestra de 1000 registros en Excel
- `src/static/auditoria/ingestion.txt` — reporte de auditoria

**Limpieza:**
- `src/xlsx/cleaned_data.xlsx` — muestra limpia de 1000 registros
- `src/static/auditoria/cleaning_report.txt` — reporte de limpieza

**Enriquecimiento:**
- `src/xlsx/enriched_data.xlsx` — muestra enriquecida de 1000 registros
- `src/static/auditoria/enrichment_report.txt` — reporte de enriquecimiento

---

## Enriquecimiento de Datos (Actividad 3)

El script `src/enrichment.py` integra el dataset limpio con cinco fuentes
adicionales en distintos formatos, cruzadas por `cod_depto`:

| Archivo | Formato | Informacion aportada |
|---|---|---|
| `sources/poblacion.csv` | CSV | Poblacion 2023 por departamento (DANE) |
| `sources/categorias.json` | JSON | Categoria y numero de municipios (DNP) |
| `sources/regiones.txt` | TXT | Region geografica (Andina, Caribe, etc.) |
| `sources/pib.xml` | XML | PIB per capita y total por departamento (DANE 2022) |
| `sources/desempleo.html` | HTML | Tasa de desempleo y poblacion ocupada (DANE 2023) |

### Logica de integracion

- Clave de union: `cod_depto` (codigo DANE del departamento)
- Tipo de join: LEFT JOIN para conservar todos los registros del dataset base
- Columna derivada: `hurtos_por_100k` = (cantidad / poblacion_2023) × 100.000
- Herramienta: PySpark con joins sucesivos sobre Spark DataFrames

---

## Automatizacion con GitHub Actions

El archivo `.github/workflows/bigdata.yml` define el workflow `ingesta-big-data`
que se ejecuta automaticamente con cada push a `main`, o de forma manual desde
la pestana Actions del repositorio.

### Pasos del workflow

1. Clona el repositorio en Ubuntu
2. Configura Java 11 y Python 3.11
3. Instala las dependencias
4. Ejecuta `src/ingestion.py`
5. Ejecuta `src/cleaning.py`
6. Ejecuta `src/enrichment.py`
7. Sube todos los artefactos generados

### Verificacion

1. Ir a la pestana **Actions** del repositorio
2. Seleccionar el workflow `ingesta-big-data`
3. Al finalizar, descargar los artefactos desde la seccion **Artifacts**