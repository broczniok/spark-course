from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTreeRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    inputLines = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("realestate.csv")

    assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"], outputCol="features")
    df = assembler.transform(inputLines)
    finalized_df = df.select("features", "PriceOfUnitArea")
    finalized_df = finalized_df.selectExpr("features as features", "PriceOfUnitArea as label")


    # Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    # Perhaps you're importing data from a real database. Or you are using structured streaming
    # to get your data.

    # Let's split our data into training data and testing data
    trainTest = finalized_df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our linear regression model
    tree = DecisionTreeRegressor()

    # Train the model using our training data
    model = tree.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions and the "known" correct labels.
    fullPredictions.show()

    # Zip them together

    # Print out the predicted and actual values for each point



    # Stop the session
    spark.stop()
