import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from datetime import datetime
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # get the data
    inputs = [('SFU', datetime.strptime('18-Nov-2023', '%d-%b-%Y'), 49.2771, -122.9146, 330.0, float('nan')),  ('SFU', datetime.strptime('17-Nov-2023', '%d-%b-%Y'), 49.2771, -122.9146, 330.0, 12.0)]
    test_tmax = spark.createDataFrame(inputs, schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(test_tmax)
    print('Predicted tmax tomorrow:', predictions.select('prediction').rdd.flatMap(list).collect()[0])

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
