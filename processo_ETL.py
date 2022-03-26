''' DESCRIÇÃO DO PROJETO
realizar um processo de ETL – Extract, Transform, Load usando uma base de dados csv - Semente de Abóbora
PROCEDIMENTOS
1.	Foi escolhido dataset Semente de Abóbora em csv, pelo site https://www.kaggle.com/datasets.
2.	O dataset deverá ser carregado em um script em Python usando PySpark.
3.	O dataset deve ser tratado usando PySpark e demais tecnologias aprendidas durante o curso.
4.	Os dados tratados devem ser carregados para uma base de dados MongoDB.

'''

from pymongo import MongoClient
from pyspark.sql.session import SparkSession
from pyspark.sql.types import(IntegerType, StringType, FloatType, StructType, StructField)
import pyspark.sql.functions as F
from pymongo import collection
import pymongo
import findspark # FINDSPARK - SERVE PARA LOCALIZAR O SERVIDOR DO PYSPARK NA MAQUINA E INICIAR NO PYTHON
findspark.init()
'''Conectando ao Spark'''

spark = SparkSession.builder.appName('firstSeesion')\
   .config('spark.master', 'local')\
   .config("spark.executor.memory", "2gb") \
   .config('spark.shuffle.sql.partitions', 2)\
   .getOrCreate() 
                    


#EXTRAÇÃO#                   
''' Fazendo o schema - Tipo de coluna'''
schema = StructType([StructField('Area', IntegerType()),
                     StructField('Perimeter', StringType()),
                     StructField('Major_Axis_Length', StringType()),
                     StructField('Minor_Axis_Length', StringType()),
                     StructField('Convex_Area', IntegerType()),
                     StructField('Equiv_Diameter', StringType()),
                     StructField('Eccentricity', StringType()),
                     StructField('Solidity', StringType()),
                     StructField('Extent',StringType()),
                     StructField('Roundness', StringType()),
                     StructField('Aspect_Ration', StringType()),
                     StructField('Compactness', StringType()),
                     StructField('Class', StringType()),
                     ])

 
''' Etapa de reconhecimento do dataset '''
dataset = spark.read.format("csv")\
               .option("encoding", "ISO-8859-1")\
               .option("header", "true")\
               .option("delimiter", ";")\
               .schema(schema)\
               .load(r"C:\Users\renat\OneDrive\Documentos\AulasSoulCode\ArquivosVisualCode\CursoPy3\ETL\Exercicio_ETL\Pumpkin_Seeds_Dataset.csv")
# dataset.show()
# print(dataset.count())
# print(schema)

#TRANSFORMAÇÃO#
converter_valor = lambda variavel: float(variavel.replace(",", ".")) # Converte os valores que estão com "," em ".".
udf_converter_valor = F.udf(converter_valor,FloatType())

dataset_padronizado_float = dataset.withColumn("Perimeter", udf_converter_valor(dataset["Perimeter"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Major_Axis_Length", udf_converter_valor(dataset["Major_Axis_Length"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Minor_Axis_Length", udf_converter_valor(dataset["Minor_Axis_Length"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Equiv_Diameter", udf_converter_valor(dataset["Equiv_Diameter"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Eccentricity", udf_converter_valor(dataset["Eccentricity"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Solidity", udf_converter_valor(dataset["Solidity"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Extent", udf_converter_valor(dataset["Extent"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Roundness", udf_converter_valor(dataset["Roundness"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Aspect_Ration", udf_converter_valor(dataset["Aspect_Ration"]))
dataset_padronizado_float = dataset_padronizado_float.withColumn("Compactness", udf_converter_valor(dataset["Compactness"]))
# dataset_padronizado_float.printSchema()
# dataset_padronizado_float.show(1, truncate=False) 

#CARREGAMENTO NO BANCO MONGODB#

#Função para conectar ao banco#
def get_database():
       conexao_banco = "mongodb://127.0.0.1:27017/"
       cliente = MongoClient(conexao_banco)
       print("conectado com Sucesso!")
       return cliente['soul_code_data_ETL'] # Nome da database soul_code_data_ETL
    
dbname = get_database()
collection_name = dbname["Exercico_py3"] # Nome da "tabela" Exercico_py3
df = dataset_padronizado_float.limit(100) 
df = df.toPandas() # Foi transformado em dataframe pandas pq até então era um dataframe spark e essa mudança é necessária pq precissmos transforma em dicionário já que o mongoDB  é do tipo dicionário
data_dict = df.to_dict('records') # para colocar em dicionário é necessário primeiro transformar em pandas.
collection_name.insert_many(data_dict)
print("Data Frame importado com Sucesso")

