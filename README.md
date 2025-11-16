# Caso de Ingeniería de Datos

Proyecto de ETL en PySpark para procesar un archivo de entregas de producto.  
El flujo lee un CSV, normaliza columnas, aplica reglas de negocio, filtra por país y rango de fechas (configurados en `config.yaml`), genera columnas adicionales y guarda el resultado en formato Parquet particionado por `fecha_proceso`.  

## Estructura
- `etl.py`: script principal
- `config.yaml`: parámetros del proceso
- `data/raw/data_entrega_productos.csv`: archivo CSV crudo
- `exploracion_datos.ipynb`: análisis exploratorio preliminar

## Ejecución

Ejecutar mediante el siguiente comando:

```
python etl.py
```

Los resultados se almancena en el `output_path` definido a continuación

```
data/processed/
```

## Flujo del ETL

```mermaid
flowchart TD
    A[Lectura CSV<br>con esquema definido] 
      --> B[Renombre de columnas<br>cod_*, id_*]
    B --> C[Conversión de fecha<br>to_date(fecha_proceso)]
    C --> D[Normalización de textos<br>trim + upper en campos clave]
    D --> E[Filtros configurables<br>- país<br>- rango de fechas]
    E --> F[Eliminación de nulos<br>columnas críticas que no aceptan NULL]
    F --> G[Validaciones<br>- tipo_entrega válido<br>- precio > 0<br>- cantidad > 0]
    G --> H[Conversión de unidades<br>CS → cantidad*20]
    H --> I[Creación de indicadores<br>flags zpre, zve1, z04, z05]
    I --> J[Columnas descriptivas<br>pais_desc, unidad_desc, tipo_entrega_desc]
    J --> K[Campos derivados de fecha<br>anio, mes, semana, día_semana]
    K --> L[Rangos basados en thresholds<br>rango_precio, rango_unidades]
    L --> M[Marca temporal<br>fecha_ejecucion]
    M --> N[Reordenamiento final de columnas]
    N --> O[Exportación Parquet<br>particionado por fecha_proceso]
```

El proceso inicia con la lectura del archivo CSV original.  
Luego se normalizan las columnas, se convierte la fecha de proceso y se aplican filtros por rango de fechas y país (según YAML).  
Se eliminan valores nulos o anómalos y se aplican reglas de negocio para validar tipos de entrega y unificar unidades.
Después se generan columnas adicionales (descripciones, rangos y campos derivados) y se ordena el esquema final.  
Finalmente, el resultado se escribe en formato Parquet particionado por `fecha_proceso`.

## Dependencias

El proyecto fue desarrollado con las siguientes versiones de Python y librerías:

- Python 3.10+
- pyspark 3.5.1
- omegaconf 2.3.0
- pandas 2.3.3
- pyarrow 22.0.0

