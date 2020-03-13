# Spark Cheatsheet

Filter, GroupBy, Aggregate and Alias

    display(ssaDF
    	.filter(ssaDF['gender'] == 'F')
    	.filter( (ssaDF['year'] == 1885)
                  | (ssaDF['year'] == 1915)
                  | (ssaDF['year'] == 1945)
                  | (ssaDF['year'] == 1975)
                  | (ssaDF['year'] == 2005)
                 ).groupBy(['year'])
                  .max('total')
                  .select(['year', col('max(total)').alias('total')])
    )

## Registering UDFs

Register the function as a UDF by binding it with a Python variable, adding a name to access it in the SQL API and giving it a return type.

    from pyspark.sql.types import IntegerType
    
    manualAddPythonUDF = spark.udf.register("manualAddSQLUDF", 
    					manual_add, 
    					IntegerType())

When returning complex output, you must define the schema of the output in advance, like in the example below:

    from pyspark.sql.types import FloatType, StructType, StructField
    
    mathOperationsSchema = StructType([
      StructField("sum", FloatType(), True), 
      StructField("multiplication", FloatType(), True), 
      StructField("division", FloatType(), True) 
    ])
    
    manualMathPythonUDF = spark.udf.register("manualMathSQLUDF", 
    					  manual_math, 
    					  mathOperationsSchema)

## Joining Two Dataframes on a Key Column

    joinExpression = countryLookupDF [ "alpha2Code" ] == logWithIPDF [ 'IPLookupISO2' ] 
    
    logWithIPEnhancedDF = logWithIPDF.join(countryLookupDF, joinExpression)

Exploring the physical plan for a particular join (Look for BroadcastHashJoin and/or BroadcastExchange.):

    aggregatedDowDF.explain()

You can set a threshold that Spark can use to determine when a small look-up table should be broadcasted to a larger table, using the below command:

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)

Another way to do the same thing is to use the built-in function to explicitly let Spark know you want it to broadcast:

    from pyspark.sql.functions import broadcast
    
    logWithIPEnhancedDF = logWithIPDF.join(broadcast(countryLookupDF), joinExpression)

## Adjusting the Number of Partitions in a Dataframe

    # Reduce number of partitons
    wikiDF = wikiDF.coalesce(1)
    
    # Expand number of partitions
    wikiDF = wikiDF.repartition(10)

## Managed vs. Unmanaged Tables

**Question:**

What happens to the original data when I delete a managed table? What about an unmanaged table?

**Answer:**

Deleting a managed table deletes both the metadata and the data itself. Deleting an unmanaged table does not delete the original data.

### Creating a Managed Table

    df = spark.range(1, 100)
    df.write.mode("OVERWRITE").saveAsTable("myTableManaged")

### Creating an Unmanaged Table

    df.write.mode("OVERWRITE")
    	.option('path', '/tmp/myTableUnmanaged')
    	.saveAsTable("myTableUnmanaged")