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

    # Renombre de columnas
    cols_dict = {
        "pais": "cod_pais",
        "transporte": "id_transporte",
        "ruta": "id_ruta",
        "tipo_entrega": "cod_tipo_entrega",
        "material": "cod_material",
        "unidad": "cod_unidad"
    }

    for old, new in cols_dict.items():
        df = df.withColumnRenamed(old, new)

    # Casteo de fecha
    df = df.withColumn(
        "fecha_proceso",
        F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    )

    # Normalizacion de columnas de texto
    cols_texto = ["cod_pais", "cod_tipo_entrega", "cod_material", "cod_unidad"]
    for col in cols_texto:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    # Aplicacion de parametros de ejecucion

    log.info("Aplicando parametros de fechas y pais")
    log.info(f"- Rango de fechas: {cfg.params.start_date} a {cfg.params.end_date}")
    log.info(f"- Pais: {cfg.params.country}")

    start_date = cfg.params.start_date
    end_date = cfg.params.end_date
    country = cfg.params.country

    df = df.filter(
        (F.col("fecha_proceso") >= start_date) &
        (F.col("fecha_proceso") <= end_date)
    )

    df = df.filter(F.col("cod_pais") == country)

    log.info(f"Registros despues de filtrado por fechas y pais: {df.count()}")

    # Exclusion de registros con valores nulos
    cols = [
        "cod_pais", "fecha_proceso", "id_transporte", "id_ruta",
        "cod_tipo_entrega", "cod_material", "precio", "cantidad", "cod_unidad"
    ]

    for col in cols:
        df = df.filter(F.col(col).isNotNull())

    log.info(f"Registros despues de exclusion de nulos: {df.count()}")

    # Aplicacion de reglas de negocio
    tipo_entrega_validos = ["ZPRE", "ZVE1", "Z04", "Z05"]
    df = df.filter(F.col("cod_tipo_entrega").isin(tipo_entrega_validos))
    df = df.filter(F.col("precio") > 0)
    df = df.filter(F.col("cantidad") > 0)
    df = df.withColumn("unidades",
                       F.when(F.col("cod_unidad") == "CS", F.col("cantidad") * 20)
                       .otherwise(F.col("cantidad"))
    )
    df = (df
            .withColumn("flag_zpre", F.when(F.col("cod_tipo_entrega") == "ZPRE", 1).otherwise(0))
            .withColumn("flag_zve1", F.when(F.col("cod_tipo_entrega") == "ZVE1", 1).otherwise(0))
            .withColumn("flag_z05", F.when(F.col("cod_tipo_entrega") == "Z05", 1).otherwise(0))
            .withColumn("flag_z04", F.when(F.col("cod_tipo_entrega") == "Z04", 1).otherwise(0))
    )

    log.info(f"Registros despues de aplicacion de reglas de negocio: {df.count()}")

    # EXTRA: columnas adicionales

    # descripcion de pais
    df = df.withColumn("pais_desc",
        F.when(F.col("cod_pais") == "GT", "GUATEMALA")
        .when(F.col("cod_pais") == "HN", "HONDURAS")
        .when(F.col("cod_pais") == "EC", "ECUADOR")
        .when(F.col("cod_pais") == "SV", "EL SALVADOR")
        .when(F.col("cod_pais") == "PE", "PERU")
        .when(F.col("cod_pais") == "JM", "JAMAICA")
    )

    # descripcion de unidad
    df = df.withColumn("unidad_desc",
        F.when(F.col("cod_unidad") == "CS", "CAJA")
        .when(F.col("cod_unidad") == "ST", "UNIDAD")
    )

    # descripcion de tipo de entrega
    df = df.withColumn(
        "tipo_entrega_desc",
        F.when(F.col("cod_tipo_entrega").isin(["ZPRE", "ZVE1"]), "RUTINA")
        .when(F.col("cod_tipo_entrega").isin(["Z04", "Z05"]), "BONIFICACION")
    )
    # valores de fecha
    df = df.withColumn("anio", F.year("fecha_proceso"))
    df = df.withColumn("mes", F.month("fecha_proceso"))
    df = df.withColumn("semana_anio", F.weekofyear("fecha_proceso"))
    df = df.withColumn("dia_semana", F.date_format("fecha_proceso", "E"))
    df = df.withColumn(
        "dia_semana",
        F.when(F.date_format("fecha_proceso", "E") == "Mon", "LUNES")
        .when(F.date_format("fecha_proceso", "E") == "Tue", "MARTES")
        .when(F.date_format("fecha_proceso", "E") == "Wed", "MIERCOLES")
        .when(F.date_format("fecha_proceso", "E") == "Thu", "JUEVES")
        .when(F.date_format("fecha_proceso", "E") == "Fri", "VIERNES")
        .when(F.date_format("fecha_proceso", "E") == "Sat", "SABADO")
        .when(F.date_format("fecha_proceso", "E") == "Sun", "DOMINGO")
    )

    # rangos de unidades segun mediana
    th_unidades = cfg.thresholds.unidades[country]["q50"]
    df = df.withColumn(
        "rango_unidades",
        F.when(F.col("unidades") <= F.lit(th_unidades), "BAJO")
        .otherwise("ALTO")
    )

    # rangos de precios segun mediana
    th_precio = cfg.thresholds.precio[country]["q50"]
    df = df.withColumn(
        "rango_precio",
        F.when(F.col("precio") <= F.lit(th_precio), "BAJO")
        .otherwise("ALTO")
    )

    df = df.withColumn("fecha_ejecucion", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Orden de columnas
    ordered_cols = [
        "fecha_proceso",
        "fecha_ejecucion",

        "cod_pais",
        "id_transporte",
        "id_ruta",

        "cod_material",
        "cod_unidad",
        "unidad_desc",

        "cantidad",
        "unidades",
        "rango_unidades",

        "precio",
        "rango_precio",

        "cod_tipo_entrega",
        "tipo_entrega_desc",
        "flag_zpre",
        "flag_zve1",
        "flag_z04",
        "flag_z05",

        "anio",
        "mes",
        "semana_anio",
        "dia_semana",

        "pais_desc"
    ]

    df = df.select(*ordered_cols)

    fechas = (
        df.select("fecha_proceso")
        .distinct()
        .orderBy("fecha_proceso")
        .collect()
    )
    log.info("Exportando particiones:")
    for row in fechas:
        log.info(f" - {row['fecha_proceso']}")

    output_path = cfg.output.base_path

    log.info(f"Guardando datos procesados en: {output_path}")
    (df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("fecha_proceso")
        .parquet(output_path)
    )

    log.info("Ejecucion de ETL finalizada exitosamente")

if __name__ == "__main__":
    main()
