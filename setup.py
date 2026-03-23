# setup.py

from setuptools import find_packages, setup

setup(
    name="infraestructura-big-data",
    version="1.0.0",
    description="Ingesta de datos desde API pública del gobierno colombiano hacia SQLite",
    author="cargeo95",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "openpyxl",
        "pyspark",
    ],
)
