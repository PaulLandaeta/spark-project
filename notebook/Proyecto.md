```python
from pyspark import SparkContext
```


```python
sc = SparkContext(master ="local[2]")
```

    21/08/15 18:04:35 WARN Utils: Your hostname, MacBook-Pro-de-Paul-2.local resolves to a loopback address: 127.0.0.1; using 192.168.1.9 instead (on interface en0)
    21/08/15 18:04:35 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
    21/08/15 18:04:35 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).



```python
# Cargar UI
sc

```





<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://192.168.1.9:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.1.2</code></dd>
      <dt>Master</dt>
        <dd><code>local[2]</code></dd>
      <dt>AppName</dt>
        <dd><code>pyspark-shell</code></dd>
    </dl>
</div>





```python
# Crear una Sesion de spark
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder.appName("TextoClasificacion").getOrCreate()
```


```python
 # Cargando nuestros Datasets 
data_annotations = spark.read.option("header","true").option("inferSchema","true").csv("attack_annotations.csv")
data_comments = spark.read.option("header","true").option("inferSchema","true").csv("attack_annotated_comments_m.csv")
```

                                                                                    


```python
data_comments.show(5)
```

    +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
    |              rev_id|             comment|                year|           logged_in|                  ns|              sample|       split;;;;;;;;|
    +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
    |"37675,""`-NEWLIN...|                fine| legitimate criti...| I'll write up ``...| nor likely the t...|                fine| I can fix that t...|
    |"44816,""`NEWLINE...|            the Pope| etc.  here's Kar...| cognitive bias b...| etc.NEWLINE_TOKE...| it's clear that ...| someone is still...|
    |"49851,""NEWLINE_...| the situation as...|                2002|               False|             article|              random|      train";;;;;;;;|
    |      "89320,"" Next| maybe you could ...| both of which I ...| thanks. I really...| yet you were bei...| you can learn to...|                2002|
    |"93890,""This pag...|                2002|                True|             article|              random|      train";;;;;;;;|                null|
    +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 5 rows
    



```python
data_annotations.show(5)
```

    +------+---------+--------------+----------------+------------------+------------+------+
    |rev_id|worker_id|quoting_attack|recipient_attack|third_party_attack|other_attack|attack|
    +------+---------+--------------+----------------+------------------+------------+------+
    | 37675|     1362|           0.0|             0.0|               0.0|         0.0|   0.0|
    | 37675|     2408|           0.0|             0.0|               0.0|         0.0|   0.0|
    | 37675|     1493|           0.0|             0.0|               0.0|         0.0|   0.0|
    | 37675|     1439|           0.0|             0.0|               0.0|         0.0|   0.0|
    | 37675|      170|           0.0|             0.0|               0.0|         0.0|   0.0|
    +------+---------+--------------+----------------+------------------+------------+------+
    only showing top 5 rows
    



```python
# Unir las dos tablas por rev_id
data_comments = data_comments.join(data_annotations,data_comments.rev_id == data_annotations.rev_id,"inner")
data_comments.printSchema()
```

    root
     |-- rev_id: string (nullable = true)
     |-- comment: string (nullable = true)
     |-- year: string (nullable = true)
     |-- logged_in: string (nullable = true)
     |-- ns: string (nullable = true)
     |-- sample: string (nullable = true)
     |-- split;;;;;;;;: string (nullable = true)
     |-- rev_id: integer (nullable = true)
     |-- worker_id: integer (nullable = true)
     |-- quoting_attack: double (nullable = true)
     |-- recipient_attack: double (nullable = true)
     |-- third_party_attack: double (nullable = true)
     |-- other_attack: double (nullable = true)
     |-- attack: double (nullable = true)
    



```python
# Mostrando el nuevo esquema  
data_comments.show(n=5)
```

                                                                                    

    +------+--------------------+----+---------+-------+------+-------------+------+---------+--------------+----------------+------------------+------------+------+
    |rev_id|             comment|year|logged_in|     ns|sample|split;;;;;;;;|rev_id|worker_id|quoting_attack|recipient_attack|third_party_attack|other_attack|attack|
    +------+--------------------+----+---------+-------+------+-------------+------+---------+--------------+----------------+------------------+------------+------+
    |138117|`NEWLINE_TOKENNEW...|2002|     True|article|random|train;;;;;;;;|138117|      127|           0.0|             0.0|               0.0|         0.0|   0.0|
    |138117|`NEWLINE_TOKENNEW...|2002|     True|article|random|train;;;;;;;;|138117|       13|           0.0|             0.0|               0.0|         0.0|   0.0|
    |138117|`NEWLINE_TOKENNEW...|2002|     True|article|random|train;;;;;;;;|138117|       15|           0.0|             0.0|               0.0|         0.0|   0.0|
    |138117|`NEWLINE_TOKENNEW...|2002|     True|article|random|train;;;;;;;;|138117|       29|           0.0|             0.0|               0.0|         0.0|   0.0|
    |138117|`NEWLINE_TOKENNEW...|2002|     True|article|random|train;;;;;;;;|138117|       44|           0.0|             0.0|               0.0|         0.0|   0.0|
    +------+--------------------+----+---------+-------+------+-------------+------+---------+--------------+----------------+------------------+------------+------+
    only showing top 5 rows
    



```python
#Elimando las Columnas no Requeridas 
data_comments = data_comments.drop(*['rev_id','logged_in','ns','sample','year','split;;;;;;;;','worker_id','quoting_attack','recipient_attack','third_party_attack','other_attack'])
data_comments.show(n=5)
```

    +--------------------+------+
    |             comment|attack|
    +--------------------+------+
    |`NEWLINE_TOKENNEW...|   0.0|
    |`NEWLINE_TOKENNEW...|   0.0|
    |`NEWLINE_TOKENNEW...|   0.0|
    |`NEWLINE_TOKENNEW...|   0.0|
    |`NEWLINE_TOKENNEW...|   0.0|
    +--------------------+------+
    only showing top 5 rows
    



```python
# Verificando la cantidad de datos
(data_comments.count(), len(data_comments.columns))
```

                                                                                    




    (1600, 2)




```python
# Cargar los paquetes
import pyspark.ml.feature
dir(pyspark.ml.feature)
```




    ['Binarizer',
     'BucketedRandomProjectionLSH',
     'BucketedRandomProjectionLSHModel',
     'Bucketizer',
     'ChiSqSelector',
     'ChiSqSelectorModel',
     'CountVectorizer',
     'CountVectorizerModel',
     'DCT',
     'ElementwiseProduct',
     'FeatureHasher',
     'HasFeaturesCol',
     'HasHandleInvalid',
     'HasInputCol',
     'HasInputCols',
     'HasLabelCol',
     'HasMaxIter',
     'HasNumFeatures',
     'HasOutputCol',
     'HasOutputCols',
     'HasRelativeError',
     'HasSeed',
     'HasStepSize',
     'HasThreshold',
     'HasThresholds',
     'HashingTF',
     'IDF',
     'IDFModel',
     'Imputer',
     'ImputerModel',
     'IndexToString',
     'Interaction',
     'JavaEstimator',
     'JavaMLReadable',
     'JavaMLWritable',
     'JavaModel',
     'JavaParams',
     'JavaTransformer',
     'MaxAbsScaler',
     'MaxAbsScalerModel',
     'MinHashLSH',
     'MinHashLSHModel',
     'MinMaxScaler',
     'MinMaxScalerModel',
     'NGram',
     'Normalizer',
     'OneHotEncoder',
     'OneHotEncoderModel',
     'PCA',
     'PCAModel',
     'Param',
     'Params',
     'PolynomialExpansion',
     'QuantileDiscretizer',
     'RFormula',
     'RFormulaModel',
     'RegexTokenizer',
     'RobustScaler',
     'RobustScalerModel',
     'SQLTransformer',
     'SparkContext',
     'StandardScaler',
     'StandardScalerModel',
     'StopWordsRemover',
     'StringIndexer',
     'StringIndexerModel',
     'Tokenizer',
     'TypeConverters',
     'UnivariateFeatureSelector',
     'UnivariateFeatureSelectorModel',
     'VarianceThresholdSelector',
     'VarianceThresholdSelectorModel',
     'VectorAssembler',
     'VectorIndexer',
     'VectorIndexerModel',
     'VectorSizeHint',
     'VectorSlicer',
     'Word2Vec',
     'Word2VecModel',
     '_BucketedRandomProjectionLSHParams',
     '_CountVectorizerParams',
     '_IDFParams',
     '_ImputerParams',
     '_LSH',
     '_LSHModel',
     '_LSHParams',
     '_MaxAbsScalerParams',
     '_MinMaxScalerParams',
     '_OneHotEncoderParams',
     '_PCAParams',
     '_RFormulaParams',
     '_RobustScalerParams',
     '_Selector',
     '_SelectorModel',
     '_SelectorParams',
     '_StandardScalerParams',
     '_StringIndexerParams',
     '_UnivariateFeatureSelectorParams',
     '_VarianceThresholdSelectorParams',
     '_VectorIndexerParams',
     '_Word2VecParams',
     '__all__',
     '__builtins__',
     '__cached__',
     '__doc__',
     '__file__',
     '__loader__',
     '__name__',
     '__package__',
     '__spec__',
     '_convert_to_vector',
     '_jvm',
     'inherit_doc',
     'keyword_only',
     'since']




```python
# Load Our Trabsfomers & Extractor Pkgs.
from pyspark.ml.feature import Tokenizer, StopWordsRemover,CountVectorizer,IDF
from pyspark.ml.feature import StringIndexer
```


```python
# Stages for the Pipeline
tokenizer = Tokenizer(inputCol = 'comment', outputCol = 'mytokens')
stopwords_remover = StopWordsRemover(inputCol = 'mytokens', outputCol = 'filtered_tokens')
vectorizer = CountVectorizer(inputCol='filtered_tokens', outputCol = 'rawFeatures')
idf = IDF(inputCol = 'rawFeatures', outputCol = 'vectorizedFeatures')
```


```python
## Dividir el DataSet 
(train, test) = data_comments.randomSplit((0.7,0.3), seed = 42)
```


```python
train.show()
```

    [Stage 13:>                                                         (0 + 1) / 1]

    +--------------------+------+
    |             comment|attack|
    +--------------------+------+
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.  See the GFDL a...|   0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|
    |.com  All parts o...|   0.0|
    |.com  All parts o...|   0.0|
    |.com  All parts o...|   0.0|
    +--------------------+------+
    only showing top 20 rows
    


                                                                                    


```python
### Estimador 
from pyspark.ml.classification import LogisticRegression
```


```python
lr = LogisticRegression(featuresCol='vectorizedFeatures', labelCol='attack')
```


```python
from pyspark.ml import Pipeline
```


```python
pipeline = Pipeline(stages=[tokenizer,stopwords_remover,vectorizer,idf,lr])
```


```python
pipeline
```




    Pipeline_21d1eb16bab1




```python
pipeline.stages
```




    Param(parent='Pipeline_21d1eb16bab1', name='stages', doc='a list of pipeline stages')




```python
# Contruyendo el modelo
lr_model = pipeline.fit(train)
```

    21/08/15 18:05:55 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
    21/08/15 18:05:55 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
                                                                                    


```python
lr_model
```




    PipelineModel_6c8c9942d40a




```python
# Predictions on our Test Dataset
predictions = lr_model.transform(test)
```


```python
predictions.show()
```

    [Stage 94:>                                                         (0 + 1) / 1]

    +--------------------+------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----------+
    |             comment|attack|            mytokens|     filtered_tokens|         rawFeatures|  vectorizedFeatures|       rawPrediction|         probability|prediction|
    +--------------------+------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----------+
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.  See the GFDL a...|   0.0|[., , see, the, g...|[., , see, gfdl, ...|(1318,[0,2,41,52,...|(1318,[0,2,41,52,...|[21.2742222355105...|[0.99999999942360...|       0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|[.newline_token*t...|[.newline_token*t...|(1318,[206,1248,1...|(1318,[206,1248,1...|[24.3495336741877...|[0.99999999997338...|       0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|[.newline_token*t...|[.newline_token*t...|(1318,[206,1248,1...|(1318,[206,1248,1...|[24.3495336741877...|[0.99999999997338...|       0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|[.newline_token*t...|[.newline_token*t...|(1318,[206,1248,1...|(1318,[206,1248,1...|[24.3495336741877...|[0.99999999997338...|       0.0|
    |.NEWLINE_TOKEN*Th...|   0.0|[.newline_token*t...|[.newline_token*t...|(1318,[206,1248,1...|(1318,[206,1248,1...|[24.3495336741877...|[0.99999999997338...|       0.0|
    |.com  All parts o...|   0.0|[.com, , all, par...|[.com, , parts, a...|(1318,[0,2,30,129...|(1318,[0,2,30,129...|[18.5030831892947...|[0.99999999079098...|       0.0|
    |.com  All parts o...|   0.0|[.com, , all, par...|[.com, , parts, a...|(1318,[0,2,30,129...|(1318,[0,2,30,129...|[18.5030831892947...|[0.99999999079098...|       0.0|
    |.com  All parts o...|   0.0|[.com, , all, par...|[.com, , parts, a...|(1318,[0,2,30,129...|(1318,[0,2,30,129...|[18.5030831892947...|[0.99999999079098...|       0.0|
    |.com  All parts o...|   0.0|[.com, , all, par...|[.com, , parts, a...|(1318,[0,2,30,129...|(1318,[0,2,30,129...|[18.5030831892947...|[0.99999999079098...|       0.0|
    |.com  All parts o...|   0.0|[.com, , all, par...|[.com, , parts, a...|(1318,[0,2,30,129...|(1318,[0,2,30,129...|[18.5030831892947...|[0.99999999079098...|       0.0|
    |.com  All parts o...|   0.0|[.com, , all, par...|[.com, , parts, a...|(1318,[0,2,30,129...|(1318,[0,2,30,129...|[18.5030831892947...|[0.99999999079098...|       0.0|
    |An event mentione...|   0.0|[an, event, menti...|[event, mentioned...|(1318,[2,112,1291...|(1318,[2,112,1291...|[21.8906017913148...|[0.99999999968880...|       0.0|
    |An event mentione...|   0.0|[an, event, menti...|[event, mentioned...|(1318,[2,112,1291...|(1318,[2,112,1291...|[21.8906017913148...|[0.99999999968880...|       0.0|
    |An event mentione...|   0.0|[an, event, menti...|[event, mentioned...|(1318,[2,112,1291...|(1318,[2,112,1291...|[21.8906017913148...|[0.99999999968880...|       0.0|
    +--------------------+------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----------+
    only showing top 20 rows
    


                                                                                    


```python
# Select Columns
predictions.columns
```




    ['comment',
     'attack',
     'mytokens',
     'filtered_tokens',
     'rawFeatures',
     'vectorizedFeatures',
     'rawPrediction',
     'probability',
     'prediction']




```python
predictions.select('rawPrediction','probability','comment','attack','prediction').show(10)
```

    [Stage 96:>                                                         (0 + 1) / 1]

    +--------------------+--------------------+--------------------+------+----------+
    |       rawPrediction|         probability|             comment|attack|prediction|
    +--------------------+--------------------+--------------------+------+----------+
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[21.2742222355105...|[0.99999999942360...|.  See the GFDL a...|   0.0|       0.0|
    |[24.3495336741877...|[0.99999999997338...|.NEWLINE_TOKEN*Th...|   0.0|       0.0|
    |[24.3495336741877...|[0.99999999997338...|.NEWLINE_TOKEN*Th...|   0.0|       0.0|
    |[24.3495336741877...|[0.99999999997338...|.NEWLINE_TOKEN*Th...|   0.0|       0.0|
    +--------------------+--------------------+--------------------+------+----------+
    only showing top 10 rows
    


                                                                                    


```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
```


```python
evaluator = MulticlassClassificationEvaluator(labelCol='attack',predictionCol='prediction',metricName='accuracy')
```


```python
accuracy = evaluator.evaluate(predictions)
```

                                                                                    


```python
accuracy
```




    0.9483568075117371




```python
#### Method 2: Precision. F1Score (Classification Report)
from pyspark.mllib.evaluation import MulticlassMetrics
```


```python
lr_metric = MulticlassMetrics(predictions['attack','prediction'].rdd)
```

                                                                                    


```python
print("Accuracy:",lr_metric.accuracy)
print("Precision:",lr_metric.precision(1.0))
print("Recall:",lr_metric.recall(1.0))
print("F1Score:",lr_metric.fMeasure(1.0))
```

    [Stage 102:============================>                            (1 + 1) / 2]

    Accuracy: 0.9483568075117371
    Precision: 0.13636363636363635
    Recall: 0.5
    F1Score: 0.21428571428571427


                                                                                    


```python
from pyspark.sql.types import StringType
```


```python
ex1 = spark.createDataFrame([
    ("You are black",StringType())
],
# Column Name
["comment"]

)
```


```python
ex1.show()
```

    +-------------+---+
    |      comment| _2|
    +-------------+---+
    |You are black| {}|
    +-------------+---+
    



```python
# Show Full 
ex1.show(truncate=False)
```

    +-------------+---+
    |comment      |_2 |
    +-------------+---+
    |You are black|{} |
    +-------------+---+
    



```python

# Predict
pred_ex1 = lr_model.transform(ex1)
```


```python
pred_ex1.show()
```

    +-------------+---+-----------------+---------------+------------+------------------+--------------------+--------------------+----------+
    |      comment| _2|         mytokens|filtered_tokens| rawFeatures|vectorizedFeatures|       rawPrediction|         probability|prediction|
    +-------------+---+-----------------+---------------+------------+------------------+--------------------+--------------------+----------+
    |You are black| {}|[you, are, black]|        [black]|(1318,[],[])|      (1318,[],[])|[4.24584443690874...|[0.98587863607057...|       0.0|
    +-------------+---+-----------------+---------------+------------+------------------+--------------------+--------------------+----------+
    



```python

pred_ex1.columns
```




    ['comment',
     '_2',
     'mytokens',
     'filtered_tokens',
     'rawFeatures',
     'vectorizedFeatures',
     'rawPrediction',
     'probability',
     'prediction']




```python
pred_ex1.select('comment','rawPrediction','probability','prediction').show()
```

    +-------------+--------------------+--------------------+----------+
    |      comment|       rawPrediction|         probability|prediction|
    +-------------+--------------------+--------------------+----------+
    |You are black|[4.24584443690874...|[0.98587863607057...|       0.0|
    +-------------+--------------------+--------------------+----------+
    



```python

```
