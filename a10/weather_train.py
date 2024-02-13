import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions

def main(inputs, output):

    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
    ])

    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25], seed=2023)
    train = train.cache()
    validation = validation.cache()

    sql_transformer = SQLTransformer(statement = """
        select today.latitude, today.longitude, today.elevation, date_part('DOY', today.date) as day_of_year, today.tmax as tmax, yesterday.tmax as yesterday_tmax 
        from __THIS__ as today
        inner join __THIS__ as yesterday
        on date_sub(today.date, 1) = yesterday.date
        and today.station = yesterday.station""")
    # tmax_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'day_of_year'], outputCol='features')
    tmax_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'day_of_year', 'yesterday_tmax'], outputCol='features')
    rf_regressor = RandomForestRegressor(numTrees=30, featuresCol='features', labelCol='tmax', seed=2023, maxDepth=4, maxBins=24)
    tmax_pipeline = Pipeline(stages=[sql_transformer, tmax_assembler, rf_regressor])
    model = tmax_pipeline.fit(train)
    predictions = model.transform(validation)

    rmse_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol='tmax', metricName='rmse')
    rmse_score = rmse_evaluator.evaluate(predictions)
    print("Validation RMSE score for tmax model: %g" % rmse_score)

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print("Validation R^2 score for tmax model: %g" % r2)

    model.write().overwrite().save(output)



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
