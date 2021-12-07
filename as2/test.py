import findspark
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.session import SparkSession
import pandas as pd

# variable initialization
features = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide",
            "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"]
findspark.init()
spark = SparkSession.builder \
    .master("local") \
    .appName("CS642AS2") \
    .getOrCreate()
sc = spark.sparkContext

# read file
pandas_df = pd.read_csv('winequality-white.csv', delimiter=';')

# create spark dataframe
df = spark.createDataFrame(pandas_df)

# vector assembler to get features in one column
assembler = VectorAssembler(inputCols=features, outputCol="features")
df = assembler.transform(df)

# split data into train and test
(trainingData, testData) = df.randomSplit([0.9, 0.1], seed=642)

# set up rf model from save file
rf_model = RandomForestRegressionModel.load('test_rf')
rf_predictions = rf_model.transform(testData)
rf_predictions.select("prediction", "quality", "features").show(10)

# evaluate saved rf model
evaluator = RegressionEvaluator(
    labelCol="quality", predictionCol="prediction", metricName="rmse")
rf_rmse = evaluator.evaluate(rf_predictions)

# set up lr model from save file
lr_model = LinearRegressionModel.load('test_lr')
lr_predictions = lr_model.transform(testData)
lr_predictions.select("prediction", "quality", "features").show(10)

# evaluate saved lr model
evaluator = RegressionEvaluator(
    labelCol="quality", predictionCol="prediction", metricName="rmse")
lr_rmse = evaluator.evaluate(lr_predictions)

# output results
print("Root Mean Squared Error (RMSE) on Random Forest = %g" % rf_rmse) # 0.204
print("Root Mean Squared Error (RMSE) on Linear Regressor = %g" % lr_rmse) # 0.70

