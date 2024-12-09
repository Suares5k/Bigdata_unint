from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, size, split



# Configurar o Spark
conf = SparkConf().setAppName("ContarPalavras").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Caminho do arquivo no HDFS
caminho_csv = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/imdb-reviews-pt-br.csv"

# Carregar o dataset no Spark DataFrame
imdbDf = sqlContext.read.format("csv") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("encoding", "UTF-8") \
    .load(caminho_csv)

# Converter as colunas de texto para string, caso necessário
imdbDf = imdbDf.withColumn("text_en", col("text_en").cast("string"))
imdbDf = imdbDf.withColumn("text_pt", col("text_pt").cast("string"))

# Filtrar para pegar apenas os textos com sentimento negativo
neg_filmes = imdbDf.filter(imdbDf.sentiment == "neg")

# Contar o número de palavras em "text_pt" e "text_en" usando "\s+" como regex
neg_filmes = neg_filmes.withColumn("word_count_pt", size(split(col("text_pt"), r"\s+")))
neg_filmes = neg_filmes.withColumn("word_count_en", size(split(col("text_en"), r"\s+")))

# Somar o total de palavras para português e inglês
total_palavras = neg_filmes.groupBy().sum("word_count_pt", "word_count_en").collect()

# Extrair os valores das somas
soma_palavras_pt = total_palavras[0]["sum(word_count_pt)"]
soma_palavras_en = total_palavras[0]["sum(word_count_en)"]

# Calcular a diferença
diferenca_palavras = soma_palavras_pt - soma_palavras_en

# Exibir os resultados
print("Total de palavras nos textos em português (negativos): {}".format(soma_palavras_pt))
print("Total de palavras nos textos em inglês (negativos): {}".format(soma_palavras_en))
print("Diferença total de palavras (português - inglês): {}".format(diferenca_palavras))
