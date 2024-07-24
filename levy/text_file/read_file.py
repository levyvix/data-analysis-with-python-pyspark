from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Iniciar Spark Session
spark: SparkSession = SparkSession.builder.appName("Read file").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Ler arquivo CSV
results = (
    spark.read.text("../data/gutenberg_books/*.txt")
    .select(f.split(f.col("value"), " ").alias("line"))
    .select(f.explode(f.col("line")).alias("word"))
    .select(f.lower(f.col("word")).alias("words_lower"))
    .select(f.regexp_extract(f.col("words_lower"), "[a-z]+", 0).alias("word"))
    .filter(f.col("word") != "")
    .groupby(f.col("word"))
    .agg(f.count(f.col("word")).alias("count"))
    .orderBy(f.col("count").desc())
)


# Salvar resultados em um arquivo CSV
results.coalesce(1).write.csv("../data/pride-and-prejudice.csv", mode="overwrite")
