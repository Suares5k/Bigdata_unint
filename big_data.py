
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, col

# Configurar o Spark para usar o HDFS corretamente
spark = SparkSession.builder \
    .appName("SomatorioIDs") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020") \
    .getOrCreate()

# Caminho do arquivo no HDFS
caminho_csv = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/imdb-reviews-pt-br.csv"

# Carregar o dataset no Spark DataFrame
imdbDf = spark.read.csv(caminho_csv,
                        header=True,       # Primeira linha como cabeçalho
                        quote="\"",        # Aspas para valores textuais
                        escape="\"",       # Escape de caracteres especiais
                        encoding="UTF-8")  # Codificação UTF-8

# Verificar se os dados foram carregados corretamente
print("Estrutura do DataFrame:")
imdbDf.printSchema()
print("Visualizando os dados:")
imdbDf.show(5)

# Converter a coluna "id" para inteiro, caso necessário
imdbDf = imdbDf.withColumn("id", col("id").cast("int"))

# Filtrar para pegar apenas os filmes com sentimento negativo
neg_filmes = imdbDf.filter(imdbDf.sentiment == "neg")

# Verificar o número de filmes negativos
num_filmes_neg = neg_filmes.count()
print("Total de filmes negativos encontrados: {}".format(num_filmes_neg))

# Validar os IDs mínimos e máximos dos filmes negativos
id_stats = neg_filmes.select(min("id").alias("min_id"), max("id").alias("max_id"))
id_stats.show()

# Somar os valores da coluna "id" para os filmes negativos
soma_ids = neg_filmes.agg({"id": "sum"}).collect()

# Exibir a soma dos IDs dos filmes negativos
print("A soma de todos os campos 'id' dos filmes negativos é: {}".format(soma_ids[0]['sum(id)']))

