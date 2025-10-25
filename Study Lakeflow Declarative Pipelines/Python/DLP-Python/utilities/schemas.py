from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType

bronze_schema=StructType([
StructField('CRASH_DATE', StringType(), True), 
StructField('CRASH_TIME', StringType(), True), 
StructField('BOROUGH', StringType(), True), 
StructField('ZIP_CODE', StringType(), True), 
StructField('LATITUDE', StringType(), True), 
StructField('LONGITUDE', StringType(), True), 
StructField('LOCATION', StringType(), True), 
StructField('ON_STREET_NAME', StringType(), True), 
StructField('CROSS_STREET_NAME', StringType(), True), 
StructField('OFF_STREET_NAME', StringType(), True), 
StructField('NUMBER_OF_PERSONS_INJURED', StringType(), True), 
StructField('NUMBER_OF_PERSONS_KILLED', StringType(), True), 
StructField('NUMBER_OF_PEDESTRIANS_INJURED', StringType(), True), StructField('NUMBER_OF_PEDESTRIANS_KILLED', StringType(), True), StructField('NUMBER_OF_CYCLIST_INJURED', StringType(), True), \
StructField('NUMBER_OF_CYCLIST_KILLED', StringType(), True), \
StructField('NUMBER_OF_MOTORIST_INJURED', StringType(), True), \
StructField('NUMBER_OF_MOTORIST_KILLED', StringType(), True), \
StructField('CONTRIBUTING_FACTOR_VEHICLE_1', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_2', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_3', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_4', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_5', StringType(), True), StructField('COLLISION_ID', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_1', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_2', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_3', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_4', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_5', StringType(), True)])

silver_schema=StructType([
StructField('CRASH_DATE', StringType(), True), 
StructField('CRASH_TIME', StringType(), True), 
StructField('BOROUGH', StringType(), True), 
StructField('ZIP_CODE', StringType(), True), 
StructField('LATITUDE', DoubleType(), True), 
StructField('LONGITUDE', DoubleType(), True), 
StructField('LOCATION', StringType(), True), 
StructField('ON_STREET_NAME', StringType(), True), 
StructField('CROSS_STREET_NAME', StringType(), True), 
StructField('OFF_STREET_NAME', StringType(), True), 
StructField('NUMBER_OF_PERSONS_INJURED', IntegerType(), True), 
StructField('NUMBER_OF_PERSONS_KILLED', IntegerType(), True), 
StructField('NUMBER_OF_PEDESTRIANS_INJURED', IntegerType(), True), StructField('NUMBER_OF_PEDESTRIANS_KILLED', IntegerType(), True), StructField('NUMBER_OF_CYCLIST_INJURED', IntegerType(), True), 
StructField('NUMBER_OF_CYCLIST_KILLED', IntegerType(), True), 
StructField('NUMBER_OF_MOTORIST_INJURED', IntegerType(), True), StructField('NUMBER_OF_MOTORIST_KILLED', IntegerType(), True), 
StructField('CONTRIBUTING_FACTOR_VEHICLE_1', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_2', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_3', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_4', StringType(), True), StructField('CONTRIBUTING_FACTOR_VEHICLE_5', StringType(), True), StructField('COLLISION_ID', IntegerType(), True), 
StructField('VEHICLE_TYPE_CODE_1', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_2', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_3', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_4', StringType(), True), 
StructField('VEHICLE_TYPE_CODE_5', StringType(), True),
StructField('Bronze_Ingestion_Timestamp', TimestampType(), True)])

