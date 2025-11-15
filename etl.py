import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from omegaconf import OmegaConf
import os
from datetime import datetime

# Definición de schema
schema = StructType([
    StructField("pais", StringType(), True),
    StructField("fecha_proceso", StringType(), True),
    StructField("transporte", IntegerType(), True),
    StructField("ruta", IntegerType(), True),
    StructField("tipo_entrega", StringType(), True),
    StructField("material", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("cantidad", DoubleType(), True),
    StructField("unidad", StringType(), True),
])

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
logfile = f"logs/pipeline_{ts}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logfile)
    ]
)
log = logging.getLogger("pipeline")

def main():
    # Inicializacion
    log.info("Iniciando ejecucion de ETL")

    spark = (
        SparkSession.builder
        .appName("pipeline")
        .getOrCreate()
    )
    log.info("Spark inicializado")

    config_path="config.yaml"
    cfg = OmegaConf.load(config_path)
    log.info("Configuracion cargada")

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(cfg.input.raw_path)
    )
    log.info(f"Registros cargados {df.count()}")

    # Casteo de fecha
    df = df.withColumn(
        "fecha_proceso",
        F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    )

    # Normalizacion de columnas de texto
    cols_texto = ["pais", "tipo_entrega", "unidad", "material"]
    for col in cols_texto:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    # Aplicacion de parametros de ejecucion

    log.info("Aplicando parametros de fechas y pais")
    log.info(f"- Rango de fechas: {cfg.params.start_date} a {cfg.params.end_date}")
    log.info(f"- Pais: {cfg.params.country}")

    df = df.filter(
        (F.col("fecha_proceso") >= cfg.params.start_date) &
        (F.col("fecha_proceso") <= cfg.params.end_date)
    )

    df = df.filter(F.col("pais") == cfg.params.country)

    log.info(f"Registros despues de filtrado por fechas y pais: {df.count()}")

    # Exclusion de registros con valores nulos
    cols = [
        "pais", "fecha_proceso", "transporte", "ruta",
        "tipo_entrega", "material", "precio", "cantidad", "unidad"
    ]

    for col in cols:
        df = df.filter(F.col(col).isNotNull())

    log.info("Validacion de registros con valores nulos")

    # Aplicacion de reglas de negocio
    tipo_entrega_validos = ["ZPRE", "ZVE1", "Z04", "Z05"]
    df = df.filter(F.col("tipo_entrega").isin(tipo_entrega_validos))
    df = df.filter(F.col("precio") > 0)
    df = df.filter(F.col("cantidad") > 0)

    log.info(f"Registros despues de aplicacion de reglas de negocio: {df.count()}") 

    log.info("Ejecucion de ETL finalizada exitosamente")

if __name__ == "__main__":
    main()
