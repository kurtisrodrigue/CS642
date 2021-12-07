import findspark
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.session import SparkSession
import pandas as pd
from imblearn.over_sampling import RandomOverSampler


# variable initialization
features = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide",
            "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"]
ros = RandomOverSampler(random_state=0)
findspark.init()
spark = SparkSession.builder \
    .master("local") \
    .appName("CS642AS2") \
    .getOrCreate()
sc = spark.sparkContext

# read csv file
pandas_df = pd.read_csv('winequality-white.csv', delimiter=';')

# over sample the data
pdx = pandas_df.drop(columns=['quality'])
pdy = pandas_df['quality']
pdx_ros, pdy_ros = ros.fit_resample(pdx, pdy)
pandas_df = pdx_ros.join(pdy_ros)

# transform to spark dataframe
df = spark.createDataFrame(pandas_df)

# vector assembler to get features in one column
assembler = VectorAssembler(inputCols=features, outputCol="features")
df = assembler.transform(df)

# split the data into test and train
(trainingData, testData) = df.randomSplit([0.9, 0.1], seed=642)

# Random forest
rf = RandomForestRegressor(featuresCol="features", labelCol="quality", maxDepth=20, numTrees=200)
rf_model = rf.fit(trainingData)
rf_predictions = rf_model.transform(testData)
rf_predictions.select("prediction", "quality", "features").show(10)

# measure performance of random forest
evaluator = RegressionEvaluator(
    labelCol="quality", predictionCol="prediction", metricName="rmse")
rf_rmse = evaluator.evaluate(rf_predictions)

# linear regression
lr = LinearRegression(featuresCol="features", labelCol="quality", maxIter=400)
lr_model = lr.fit(trainingData)
lr_predictions = lr_model.transform(testData)
lr_predictions.select("prediction", "quality", "features").show(10)

# measure performance of linear regression
evaluator = RegressionEvaluator(
    labelCol="quality", predictionCol="prediction", metricName="rmse")
lr_rmse = evaluator.evaluate(lr_predictions)

# save models
lr_model.write().overwrite().save('test_lr')
rf_model.write().overwrite().save('test_rf')

# output RMSE
print("Root Mean Squared Error (RMSE) on Random Forest = %g" % rf_rmse) # 0.67
print("Root Mean Squared Error (RMSE) on Linear Regressor = %g" % lr_rmse) # 0.72

