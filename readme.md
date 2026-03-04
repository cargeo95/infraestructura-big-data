# Infraestructura Big Data - Ingesta de Datos

**Estudiante:** Carlos Arturo Gómez Jiménez  
**Cédula:** 1020808609  
**Repositorio:** https://github.com/cargeo95/infraestructura-big-data

---

## Descripcion

Proyecto de ingesta de datos desde la API publica del gobierno colombiano
(datos.gov.co), específicamente el dataset de hurtos con aproximadamente 630.000
registros. Los datos se almacenan en una base de datos SQLite, se genera una
muestra en Excel y un archivo de auditoria que valida la integridad del proceso.

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
    ├── static/
    │   └── auditoria/
    │       └── ingestion.txt
    ├── db/
    │   └── ingestion.db
    └── xlsx/
        └── ingestion.xlsx
```

---

## Requisitos

- Python 3.11 o superior
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
pip install requests pandas openpyxl
```

---

## Ejecucion local

```bash
python src/ingestion.py
```

Al finalizar se generan tres archivos:

- `src/db/ingestion.db` — base de datos SQLite con todos los registros
- `src/xlsx/ingestion.xlsx` — muestra de 1000 registros en Excel
- `src/static/auditoria/ingestion.txt` — reporte de auditoria

---

## Automatizacion con GitHub Actions

El archivo `.github/workflows/bigdata.yml` define un workflow llamado
`ingesta-big-data` que se ejecuta automaticamente con cada push a la rama
`main`, o de forma manual desde la pestana Actions del repositorio.

El workflow realiza los siguientes pasos:

1. Clona el repositorio en una maquina virtual Ubuntu
2. Instala Python 3.11 y las dependencias necesarias
3. Ejecuta `src/ingestion.py`
4. Sube los archivos generados como artefactos descargables

Para verificar la ejecucion:

1. Ir a la pestana **Actions** del repositorio
2. Seleccionar el workflow `ingesta-big-data`
3. Al finalizar, descargar los artefactos desde la seccion **Artifacts**

```

```
