import logging
from pyspark.sql import SparkSession, functions as F
from omegaconf import OmegaConf
import os
from datetime import datetime

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
        .csv(cfg.input.raw_path)
    )
    log.info(f"Registros cargados {df.count()}")

    df = df.withColumn(
        "fecha_proceso",
        F.to_date(F.col("fecha_proceso").cast("string"), "yyyyMMdd")
    )

    log.info("Aplicando filtros de fechas y pais")
    log.info(f"- Rango de fechas: {cfg.params.start_date} a {cfg.params.end_date}")
    log.info(f"- Pais: {cfg.params.country}")

    df = df.filter(
        (F.col("fecha_proceso") >= cfg.params.start_date) &
        (F.col("fecha_proceso") <= cfg.params.end_date)
    )

    df = df.filter(F.col("pais") == cfg.params.country)

    log.info(f"Registros despues de filtrado por fechas y pais: {df.count()}")

    log.info("Ejecucion de ETL finalizada exitosamente")

if __name__ == "__main__":
    main()
