import logging
from pyspark.sql import SparkSession
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

    log.info("Ejecucion de ETL finalizada exitosamente")


if __name__ == "__main__":
    main()
