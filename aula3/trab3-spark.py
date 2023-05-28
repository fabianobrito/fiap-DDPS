from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sqlContext = HiveContext(spark)
sqlContext.sql("""create external table diabetes_patient_data 
(gender string, age integer, hypertension int, heart_disease int, smoking_history string, bmi double, HbA1c_level double, blood_glucose_level integer, diabetes integer) 
row format delimited fields terminated by ','LOCATION '/user/trab3-spark/diabetes_patient_data'""").show()
df = spark.read.csv("/user/trab3-spark/diabetes_prediction_dataset.csv", sep =',', inferSchema = True, header = True)
df.printSchema()
# Renomeando colunas atraeves da funcao - withColumnRenamed
df = df.withColumnRenamed("gender", "genero")
df = df.withColumnRenamed("age", "idade")
df = df.withColumnRenamed("smoking_history", "tabagismo_historia")
df = df.withColumnRenamed("hypertension", "hipertensao")
df = df.withColumnRenamed("heart_disease", "doenca_cardiaca")
df = df.withColumnRenamed("bmi", "IMC")
df = df.withColumnRenamed("HbA1c_level", "Hemoglobina_A1c")
df = df.withColumnRenamed("blood_glucose_level", "niveis_glicose")
df = df.withColumnRenamed("heart_disease", "doenca_cardiaca")
# GrupBy em gender e diabetes.
# Na media os homens que possui diabetes possui na media 60 anos e tem um IMC na meia de 31,28 - primeira linha
df.groupBy("genero","diabetes").mean("idade","IMC").show()
# Estou selecionado somente quem tem diabetes
df_diabetes = df.filter(df.diabetes == 1).show()
# historico de fumante
# quem nunca fumou possui possui uma media de 60 anos e um IMC de 32. 
# Quem fuma atualmente possui uma media de 55 anos e um IMC 31. 
# Aparentemente essas duas categorias nao possui nenhum diferenca estatistica para IMC
# mas tem para idade
df_diabetes.groupBy("tabagismo_historia").mean("idade","IMC").show()
# Ordenando a variavel age
df.sort("idade").show()
# Temos no arquivo idade de 0,08 anos. SEgundo pesquisa no google crianca dessa idade pode sim ter diabetes
df.select('idade').describe().show()
# Pela media do nivel de glicose, exite uma diferenca considerada em quem tem diabete e quem nao tem.
df.groupBy("diabetes").mean("niveis_glicose").show(truncate=False)
# Olhando para o valor maximo de niveis de glicose, podemos perceber que nao existe diferenca entre homens e mulheres
df.groupBy("genero").max("niveis_glicose").show()