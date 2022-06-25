# Databricks notebook source
#Course: Big Data Tools 2
#Program: MBD 2021-2022
#Team: Hina Hussain, Dilda Zhaksybek, Enita Omuvwie

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

business_path = "/FileStore/tables/parsed_business.json"
covid_path = "/FileStore/tables/parsed_covid.json"
checkin_path = "/FileStore/tables/parsed_checkin.json"
tip_path = "/FileStore/tables/parsed_tip.json"
user_path = "/FileStore/tables/parsed_user.json"
review_path = "/FileStore/tables/parsed_review.json"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Business

# COMMAND ----------

#Load Business table
business = spark.read.option("multiline","true").json(business_path)
business.display()

# COMMAND ----------

print((business.count(), len(business.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Covid

# COMMAND ----------

covid = spark.read.json(covid_path)
covid.display()

# COMMAND ----------

print((covid.count(), len(covid.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checkin

# COMMAND ----------

checkin = spark.read.option("multiline","true").json(checkin_path)
checkin.display()

# COMMAND ----------

print((checkin.count(), len(checkin.columns)))

# COMMAND ----------

checkin.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Tips

# COMMAND ----------

tip = spark.read.option("multiline","true").json(tip_path)
tip.display()

# COMMAND ----------

print((tip.count(), len(tip.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Users

# COMMAND ----------

user = spark.read.json(user_path)
user.display()

# COMMAND ----------

print((user.count(), len(user.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Review

# COMMAND ----------

review = spark.read.option("multiline","true").json(review_path)

# COMMAND ----------

review.display(5)

# COMMAND ----------

print((review.count(), len(review.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Reviews

# COMMAND ----------

#####Compute discount factor to adjust ratings according to recency#####

#convert review date to timestamp format

review = review.withColumn("date",to_timestamp(col("date"),"yyyy-M-d H:mm:ss"))

#Calculate the difference in review date (recency) from 1 March 2020
review = review.withColumn("recency",datediff(lit("2020-03-01"),col("date")))

#calculate the discount factor based on maximum difference in days
maxdiff = review.select(max("recency")).collect()[0][0]
review = review.withColumn("DiscountFactor", 1 - (col("recency") / maxdiff))

#calculate the adjusted ratings by multiplying rating with discount factor
review = review.withColumn("AdjustedRating", col("stars") * col("DiscountFactor"))

review.show(5)

# COMMAND ----------

#####Compute aggregations per business#####
#sum of useful votes
#total number of reviews
#average adjusted rating
#count of positive reviews (i.e. when original rating either 4 or 5)
#count of negative reviews (i.e. when original rating either 1 or 2 or 3)



review_metrics = review.groupBy("business_ID").agg(sum("useful").alias("UsefulVote_Review"),count("review_id").alias("TotalReviews"), avg("AdjustedRating").alias("Avg_AdjustedRating"), count(when(col("stars") > 3, True)).alias("Count_PositiveReviews"), count(when(col("stars") < 4, True)).alias("Count_NegativeReviews"))

review_metrics.show(5) 

# COMMAND ----------

review_metrics = review_metrics.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Checkin

# COMMAND ----------

#####Obtain recency of checkin from 1 march 2020#####

#trim the white space from the start of the date
checkin = checkin.withColumn("date", ltrim(col("date")))

#convert date to timestamp format
checkin = checkin.withColumn("date",to_timestamp(col("date"),"yyyy-M-dd H:mm:ss"))

#Calculate the recency from 1 March 2020
checkin = checkin.withColumn("Checkin_Recency",datediff(lit("2020-03-01"),col("date")))

#Count of Checkin per business and days since first and last checkin
checkin_metrics = checkin.groupBy("business_ID").agg(count("date").alias("CheckinCount"), min("Checkin_Recency").alias("DaysSinceLastCheckin"), max("Checkin_Recency").alias("DaysSinceFirstCheckin"))



# COMMAND ----------

checkin_metrics.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Business

# COMMAND ----------

#Replace dots in column names with underscores
#Reference: https://mungingdata.com/pyspark/avoid-dots-periods-column-names/

business = business.toDF(*(c.replace('.', '_') for c in business.columns))

# COMMAND ----------

#only keep businesses that are in the food industry
business = business.filter(business.categories.like("%Restaurants%") | business.categories.like("%Food%")|business.categories.like("%Cafes%") | business.categories.like("%Bars%")) 

# COMMAND ----------

print(business.count())

# COMMAND ----------

#Only keep selected columns

business_filtered=business.select("business_id","attributes_Alcohol","attributes_Ambience","attributes_BikeParking","attributes_BusinessAcceptsBitcoin","attributes_BusinessAcceptsCreditCards","attributes_BusinessParking","attributes_Caters","attributes_DietaryRestrictions","attributes_DriveThru","attributes_GoodForMeal","attributes_Open24Hours","attributes_OutdoorSeating","attributes_RestaurantsDelivery","attributes_RestaurantsPriceRange2","attributes_RestaurantsTakeOut","attributes_WiFi","city","hours_Friday","hours_Monday","hours_Saturday","hours_Sunday","hours_Thursday","hours_Tuesday","hours_Wednesday","is_open","review_count","stars","state")


# COMMAND ----------

print(len(business_filtered.columns))
print(business_filtered.count())

# COMMAND ----------

#checking if the columns have any other value than 0, 1 or missing
cols = ['attributes_BikeParking', 'attributes_BusinessAcceptsBitcoin',"attributes_BusinessAcceptsCreditCards","attributes_Caters","attributes_Open24Hours","attributes_OutdoorSeating"]

for col in cols:
    print(col, business_filtered.select(col).distinct().collect())

# COMMAND ----------

#replacing true, false and other with 1, 0 and missing
import pyspark.sql.functions as F
from functools import reduce

cols = ['attributes_BikeParking', 'attributes_BusinessAcceptsBitcoin',"attributes_BusinessAcceptsCreditCards","attributes_Caters","attributes_Open24Hours","attributes_OutdoorSeating", "attributes_RestaurantsDelivery","attributes_RestaurantsTakeOut","attributes_DriveThru"]

business_encoded = reduce(lambda business_filtered, c: business_filtered.withColumn(c, F.when(business_filtered[c] == 'False', 0).when(business_filtered[c] == "True",1).otherwise("Missing")), cols, business_filtered)

# COMMAND ----------

business_encoded.display()

# COMMAND ----------

business_encoded.select("attributes_WiFi").distinct().collect()

# COMMAND ----------

from pyspark.sql.functions import when
business_encoded = business_encoded.withColumn("attributes_WiFi", when(business_encoded.attributes_WiFi == "'free'","free") \
      .when(business_encoded.attributes_WiFi == "'free'","free") \
      .when(business_encoded.attributes_WiFi == "u'paid'","paid") \
      .when(business_encoded.attributes_WiFi == "'paid'","paid") \
      .when(business_encoded.attributes_WiFi.isNull(),"Missing") \
      .otherwise("no_Wifi"))
business_encoded.display()

# COMMAND ----------

business_encoded.select("attributes_DietaryRestrictions").distinct().collect()


# COMMAND ----------

business_encoded.select("attributes_Alcohol").distinct().collect()

# COMMAND ----------

business_encoded = business_encoded.withColumn("attributes_Alcohol", \
      when(business_encoded.attributes_Alcohol == "''beer_and_wine''","beer_and_wine") \
      .when(business_encoded.attributes_Alcohol == "u'beer_and_wine'","beer_and_wine") \
      .when(business_encoded.attributes_Alcohol == "u'full_bar'","full_bar") \
      .when(business_encoded.attributes_Alcohol == "'full_bar'","full_bar") \
      .when(business_encoded.attributes_Alcohol.isNull(),"Missing").otherwise("no_alcohol"))
business_encoded.display()

# COMMAND ----------

import pyspark.sql.functions as f
#Reference:https://stackoverflow.com/questions/50766487/pyspark-removing-multiple-characters-in-a-dataframe-column


business_encoded = business_encoded.withColumn("attributes_Ambience", f.translate(f.col("attributes_Ambience"), "{}'", ""))

business_encoded = business_encoded.withColumn("attributes_BusinessParking", f.translate(f.col("attributes_BusinessParking"), "{}'", ""))

business_encoded = business_encoded.withColumn("attributes_DietaryRestrictions", f.translate(f.col("attributes_DietaryRestrictions"), "{}'", ""))
business_encoded = business_encoded.withColumn("attributes_GoodForMeal", f.translate(f.col("attributes_GoodForMeal"), "{}'", ""))


business_encoded.display()

# COMMAND ----------

business_encoded.select("business_id").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For attributes_Ambience column

# COMMAND ----------

#create maps
#https://stackoverflow.com/questions/47880851/how-to-convert-string-colon-separated-column-to-maptype

from pyspark.sql.functions import split, create_map, explode

ambiance = business_encoded.withColumn("attributes_Ambience", explode(split(F.col("attributes_Ambience"), ", ")))\
  .withColumn("key", split(F.col("attributes_Ambience"), ":").getItem(0))\
  .withColumn("value", split(F.col("attributes_Ambience"), ":").getItem(1))\
  .withColumn("attributes_Ambience", create_map(F.col("key"), F.col("value")))


ambiance.display()

# COMMAND ----------

#explode into keys and values per each business ID

ambiance = ambiance.select(ambiance.business_id,explode(ambiance.attributes_Ambience))

ambiance.display()


# COMMAND ----------

ambiance.select("value").distinct().collect()

# COMMAND ----------

#dropping all the values that are false and none. 
#Because i want to create dummies, and for that i only need 1. 
#0s would be absence of values
ambiance = ambiance.filter(ambiance.value == " True")
ambiance = ambiance.select(ambiance.business_id, ambiance.key)
ambiance.display()

# COMMAND ----------

ambiance = ambiance.groupBy("business_id").pivot("key").agg(F.lit(1)).fillna(0)
ambiance.display()

# COMMAND ----------

business_encoded = business_encoded.join(ambiance,on="business_id",how="left")
business_encoded.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####For attributes_BusinessParking column

# COMMAND ----------

#create maps
#https://stackoverflow.com/questions/47880851/how-to-convert-string-colon-separated-column-to-maptype

from pyspark.sql.functions import split, create_map, explode

business_parking = business_encoded.withColumn("attributes_BusinessParking", explode(split(F.col("attributes_BusinessParking"), ", ")))\
  .withColumn("key", split(F.col("attributes_BusinessParking"), ":").getItem(0))\
  .withColumn("value", split(F.col("attributes_BusinessParking"), ":").getItem(1))\
  .withColumn("attributes_BusinessParking", create_map(F.col("key"), F.col("value")))

#explode into keys and values per each business ID

business_parking = business_parking.select(business_parking.business_id,explode(business_parking.attributes_BusinessParking))

business_parking.display()

# COMMAND ----------

business_parking.select("value").distinct().collect()

# COMMAND ----------

#dropping all the values that are false and none. 
#Because i want to create dummies, and for that i only need 1. 
#0s would be absence of values
business_parking = business_parking.filter(business_parking.value == " True")
business_parking = business_parking.select(business_parking.business_id, business_parking.key)

business_parking = business_parking.groupBy("business_id").pivot("key").agg(F.lit(1)).fillna(0)

business_encoded = business_encoded.join(business_parking,on="business_id",how="left")
business_encoded.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For attributes_DietaryRestrictions column

# COMMAND ----------

#create maps
#https://stackoverflow.com/questions/47880851/how-to-convert-string-colon-separated-column-to-maptype

from pyspark.sql.functions import split, create_map, explode

dietary_restrictions = business_encoded.withColumn("attributes_DietaryRestrictions", explode(split(F.col("attributes_DietaryRestrictions"), ", ")))\
  .withColumn("key", split(F.col("attributes_DietaryRestrictions"), ":").getItem(0))\
  .withColumn("value", split(F.col("attributes_DietaryRestrictions"), ":").getItem(1))\
  .withColumn("attributes_DietaryRestrictions", create_map(F.col("key"), F.col("value")))

#explode into keys and values per each business ID

dietary_restrictions = dietary_restrictions.select(dietary_restrictions.business_id,explode(dietary_restrictions.attributes_DietaryRestrictions))

dietary_restrictions.display()

# COMMAND ----------

dietary_restrictions.select("value").distinct().collect()

# COMMAND ----------

#dropping all the values that are false and none. 
#Because i want to create dummies, and for that i only need 1. 
#0s would be absence of values
dietary_restrictions = dietary_restrictions.filter(dietary_restrictions.value == " True")
dietary_restrictions = dietary_restrictions.select(dietary_restrictions.business_id, dietary_restrictions.key)

dietary_restrictions = dietary_restrictions.groupBy("business_id").pivot("key").agg(F.lit(1)).fillna(0)

business_encoded = business_encoded.join(dietary_restrictions,on="business_id",how="left")
business_encoded.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For attributes_GoodForMeal column

# COMMAND ----------

#create maps
#https://stackoverflow.com/questions/47880851/how-to-convert-string-colon-separated-column-to-maptype

from pyspark.sql.functions import split, create_map, explode

goodformeal = business_encoded.withColumn("attributes_GoodForMeal", explode(split(F.col("attributes_GoodForMeal"), ", ")))\
  .withColumn("key", split(F.col("attributes_GoodForMeal"), ":").getItem(0))\
  .withColumn("value", split(F.col("attributes_GoodForMeal"), ":").getItem(1))\
  .withColumn("attributes_GoodForMeal", create_map(F.col("key"), F.col("value")))

#explode into keys and values per each business ID

goodformeal = goodformeal.select(goodformeal.business_id,explode(goodformeal.attributes_GoodForMeal))

goodformeal.display()


# COMMAND ----------

#dropping all the values that are false and none. 
#Because i want to create dummies, and for that i only need 1. 
#0s would be absence of values
goodformeal = goodformeal.filter(goodformeal.value == " True")
goodformeal = goodformeal.select(goodformeal.business_id, goodformeal.key)

goodformeal = goodformeal.groupBy("business_id").pivot("key").agg(F.lit(1)).fillna(0)

business_encoded = business_encoded.join(goodformeal,on="business_id",how="left")
business_encoded.display()

# COMMAND ----------

business_encoded.filter(business_encoded.attributes_DietaryRestrictions != "null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Working with hours

# COMMAND ----------

#looping through each day of the week to compute availability in hours (diff between )
weekday_working = ["hours_Monday","hours_Tuesday","hours_Wednesday","hours_Thursday","hours_Friday","hours_Saturday","hours_Sunday"]

from pyspark.sql.functions import col, concat
from pyspark.sql.functions import substring_index
from pyspark.sql.types import IntegerType
tets = business_encoded

for i in weekday_working:
    
#split value by dash - 
    #declaring variable name 
    x = i + "_open"
    y = i + "_close"
    g = i + "_availability"
    
    tets = tets.withColumn(x, split(tets[i], '-').getItem(0)) \
           .withColumn(y, split(tets[i], '-').getItem(1))
    #remove characters after : (rounding to nearest hour)
    tets = tets.withColumn(x, substring_index(tets[x], ':', 1).alias(x).cast(IntegerType()))\
        .withColumn(y, substring_index(tets[y], ':', 1).alias(y).cast(IntegerType()))

    #add 24 for those restaurants that close past midnight

    cond1 = col(y) <= col(x)
    cond2 = col(y).isNotNull()

    tets = tets.withColumn(y,
           when((cond1) & (cond2), col(y)+24)\
                   .otherwise(col(y)+0))

    #substract opening hours from closing hours
    tets = tets.withColumn(g, ( tets[y] - tets[x] ) )
    #dropping open and close hours
    tets = tets.drop(x, y)

tets.display()

# COMMAND ----------

#looping through each day of the week to compute availability in hours (diff between close and open hours)
weekly_avail = ["hours_Monday_availability","hours_Tuesday_availability","hours_Wednesday_availability","hours_Thursday_availability","hours_Friday_availability","hours_Saturday_availability","hours_Sunday_availability"]
for i in weekly_avail:
    tets = tets.withColumn(i, when(col(i).isNull(), 0)\
                   .otherwise(col(i)))

# COMMAND ----------

tets = tets.withColumn("Weekly_availability", col("hours_Monday_availability")+col("hours_Tuesday_availability")+col("hours_Wednesday_availability")+col("hours_Thursday_availability")+col("hours_Friday_availability")+col("hours_Saturday_availability")+col("hours_Sunday_availability"))
tets = tets.withColumn("Weekends_availability", col("hours_Saturday_availability")+col("hours_Sunday_availability"))
tets = tets.withColumn("Workdays_availability", col("hours_Monday_availability")+col("hours_Tuesday_availability")+col("hours_Wednesday_availability")+col("hours_Thursday_availability")+col("hours_Friday_availability"))

tets.display()

# COMMAND ----------

#dropping unnecessary columns
final_business = tets.drop("attributes_Ambience","attributes_BusinessParking",
                           "attributes_GoodForMeal", "hours_Monday", "hours_Tuesday","hours_Wednesday","hours_Thursday","hours_Friday",
                          "hours_Saturday","hours_Sunday","attributes_DietaryRestrictions")

# COMMAND ----------

final_business.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Tips

# COMMAND ----------

#Grouping business id by the  number of users
business_tip_count = tip.groupBy("business_id").agg(count('text').alias("tips_per_business"))
business_tip_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Users

# COMMAND ----------

#Obtain the number of fans of each user
user = user.select(user.user_id, user.fans)
user.display()

# COMMAND ----------

#join with the reviews table
users = review.join(user,on="user_id",how="inner")
users.display()

# COMMAND ----------

#obtain reviewers of each business and their fans
users = users.groupby("business_id", "user_id").agg(sum("fans").alias("fan_base_of_users"))

# COMMAND ----------

#obtain average number of fans of the reviewers(users) of each business
users = users.groupby("business_id").agg(mean("fan_base_of_users").alias("avg_fans_of_reviewers"))

# COMMAND ----------

users.display()

# COMMAND ----------

print(users.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Covid

# COMMAND ----------

covid = covid.select("business_id","delivery or takeout")

# COMMAND ----------

covid.display()

# COMMAND ----------

covid = covid.withColumn("delivery or takeout", when(covid['delivery or takeout'] == "TRUE","1") \
    .otherwise("0"))
covid.display()

# COMMAND ----------

covid = covid.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Basetable

# COMMAND ----------

interim = covid.join(final_business, on="business_id", how="inner")
interim.count()

# COMMAND ----------

interim = interim.join(users, on="business_id", how="inner")
interim.count()

# COMMAND ----------

interim = interim.join(business_tip_count, on="business_id", how="left")
interim.count()

# COMMAND ----------

interim = interim.join(review_metrics, on="business_id", how="left")
interim.count()


# COMMAND ----------

final_basetable = interim.join(checkin_metrics, on="business_id", how="left")
final_basetable.count()


# COMMAND ----------

#check columns with nulls and NAs
final_basetable.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in final_basetable.columns]).display()

# COMMAND ----------

#Check columns filled with "Missing" during preprocessing
final_basetable.select([count(when(col(c) == "Missing", c)).alias(c) for c in final_basetable.columns]).display()

# COMMAND ----------

#drop columns with majority values set to "Missing" or null and unnecessary columns
final_basetable = final_basetable.drop("attributes_Alcohol","attributes_BikeParking","attributes_BusinessAcceptsBitcoin","attributes_Ambience",
                                       "attributes_BusinessParking", "attributes_Caters","attributes_DriveThru","attributes_Open24Hours",
                                      "attributes_OutdoorSeating","attributes_RestaurantsPriceRange2","attributes_WiFi", "DaysSinceFirstCheckin")



# COMMAND ----------

#Replace missing days since last checkin by max recency

max_recency = final_basetable.select(max("DaysSinceLastCheckin")).collect()[0][0]
final_basetable = final_basetable.na.fill(max_recency, subset=["DaysSinceLastCheckin"])



# COMMAND ----------

#The remaining columns which were supposed to have 0 instead of nulls were filled with 0

final_basetable= final_basetable.fillna(0)

# COMMAND ----------

#For the following columns it was decided to fill the missing value with 0

#from pyspark.sql.functions import isnan, when, col

final_basetable = final_basetable.withColumn("attributes_BusinessAcceptsCreditCards", when(col("attributes_BusinessAcceptsCreditCards") == "Missing", 0).otherwise(col("attributes_BusinessAcceptsCreditCards")))
                           
final_basetable = final_basetable.withColumn("attributes_RestaurantsDelivery", when(col("attributes_RestaurantsDelivery") == "Missing", 0)
                             .otherwise(col("attributes_RestaurantsDelivery")))
                           
final_basetable = final_basetable.withColumn("attributes_RestaurantsTakeOut", when(col("attributes_RestaurantsTakeOut") == "Missing", 0)
                             .otherwise(col("attributes_RestaurantsTakeOut")))
                           
                           


# COMMAND ----------

#Create dummies for city and state
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline

#city
cityIndxr = StringIndexer().setInputCol("city").setOutputCol("cityInd")

#state
stateIndxr = StringIndexer().setInputCol("state").setOutputCol("stateInd")

#One-hot encoding
ohee_catv = OneHotEncoder(inputCols=["cityInd","stateInd"],outputCols=["city_dum","state_dum"])
pipe_catv = Pipeline(stages=[cityIndxr, stateIndxr, ohee_catv])

final_basetable = pipe_catv.fit(final_basetable).transform(final_basetable)
final_basetable = final_basetable.drop("cityInd","stateInd", "city", "state")


# COMMAND ----------

final_basetable.display(3)
#column "delivery or takeout is the target variable"

# COMMAND ----------

print((final_basetable.count(), len(final_basetable.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Train and Test Sets

# COMMAND ----------

final_basetable_label = final_basetable.withColumn("label",col("delivery or takeout").cast("double")).drop("delivery or takeout")

# COMMAND ----------

#Create a train and test set with a 70% train, 30% test split
basetable_train, basetable_test = final_basetable_label.randomSplit([0.7, 0.3], seed = 7)

print(basetable_train.count())
print(basetable_test.count())

# COMMAND ----------

#Transform the tables in a table of label, features format
from pyspark.ml.feature import RFormula

full = RFormula(formula="label ~ . - business_id").fit(final_basetable_label).transform(final_basetable_label)
train = RFormula(formula="label ~ . - business_id").fit(basetable_train).transform(basetable_train)

#print("full nobs: " + str(full.count()))
#print("train nobs: " + str(train.count()))


# COMMAND ----------

test = RFormula(formula="label ~ . - business_id").fit(basetable_test).transform(basetable_test)
#print("test nobs: " + str(test.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Selection

# COMMAND ----------

# MAGIC %md
# MAGIC Using ChiSqSelector

# COMMAND ----------

#Reference: https://antonhaugen.medium.com/feature-selection-with-pyspark-a172d214f0b7

from pyspark.ml.feature import ChiSqSelector

selector=ChiSqSelector(percentile=0.9, featuresCol="features", outputCol="selectedFeatures", labelCol= "label")

#Fit on train only and the transform both sets
model=selector.fit(train)

full = model.transform(full)
train = model.transform(train)
test = model.transform(test)

#Selected relevant columns
full=full.select('label','selectedFeatures').withColumnRenamed('selectedFeatures', 'features')
train=train.select('label','selectedFeatures').withColumnRenamed('selectedFeatures', 'features')
test=test.select('label','selectedFeatures').withColumnRenamed('selectedFeatures', 'features')

# COMMAND ----------

# MAGIC %md
# MAGIC # Modeling

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logistic Regression

# COMMAND ----------

#Train a Logistic Regression model
from pyspark.ml.classification import LogisticRegression

#Define the algorithm class
lr = LogisticRegression(maxIter=10, regParam = 0.01)

#Fit the model
lrModel = lr.fit(train)

# COMMAND ----------

#Print coefficients
lrModel.coefficients

# COMMAND ----------

#Notice the size of this array
lrModel.coefficients.toArray().size

# COMMAND ----------

#Print intercept
lrModel.intercept

# COMMAND ----------


#Show information on the final, trained model
summary = lrModel.summary
print(summary.areaUnderROC)
print(summary.accuracy)


# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics
preds = lrModel.transform(test)\
.select("prediction", "label")


out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print("AUC: " + str(metrics.areaUnderROC))

# COMMAND ----------

#Get more metrics
from pyspark.mllib.evaluation import MulticlassMetrics

#Cast a DF of predictions to an RDD to access RDD methods of MulticlassMetrics
preds_labels = lrModel.transform(test)\
.select("prediction", "label")\
.rdd.map(lambda x: (float(x[0]), float(x[1])))

metrics = MulticlassMetrics(preds_labels)

labels = preds.rdd.map(lambda lp: lp.label).distinct().collect()
for label in sorted(labels):
    print("Class %s precision = %s" % (label, metrics.precision(label)))
    print("Class %s recall = %s" % (label, metrics.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Forest

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier

rfClassifier = RandomForestClassifier(numTrees = 50)
rfcModel = rfClassifier.fit(train)


# COMMAND ----------

fi = rfcModel.featureImportances

# COMMAND ----------

#Prettify feature importances
import pandas as pd
def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
        varlist = pd.DataFrame(list_extract)
        varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
        return(varlist.sort_values('score', ascending = False))

ExtractFeatureImp(fi, train, "features").head(10)

# COMMAND ----------


#Show information on the final, trained model
summary = rfcModel.summary
print(summary.areaUnderROC)
print(summary.accuracy)


# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics
preds = rfcModel.transform(test)\
.select("prediction", "label")


out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print("AUC test: " + str(metrics.areaUnderROC))

# COMMAND ----------

#Get more metrics
from pyspark.mllib.evaluation import MulticlassMetrics

#Cast a DF of predictions to an RDD to access RDD methods of MulticlassMetrics
preds_labels = rfcModel.transform(test)\
.select("prediction", "label")\
.rdd.map(lambda x: (float(x[0]), float(x[1])))

metrics = MulticlassMetrics(preds_labels)

labels = preds.rdd.map(lambda lp: lp.label).distinct().collect()
for label in sorted(labels):
    print("Class %s precision = %s" % (label, metrics.precision(label)))
    print("Class %s recall = %s" % (label, metrics.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gradient Boosted Tree 

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
gbtClassifier = GBTClassifier()
gbtModel = gbtClassifier.fit(train)

# COMMAND ----------

# predict on the test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics
preds = gbtModel.transform(test)\
.select("prediction", "label")


out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print("AUC: " + str(metrics.areaUnderROC))

# COMMAND ----------

#Get more metrics
from pyspark.mllib.evaluation import MulticlassMetrics

#Cast a DF of predictions to an RDD to access RDD methods of MulticlassMetrics
preds_labels = gbtModel.transform(test)\
.select("prediction", "label")\
.rdd.map(lambda x: (float(x[0]), float(x[1])))

metrics = MulticlassMetrics(preds_labels)

labels = preds.rdd.map(lambda lp: lp.label).distinct().collect()
for label in sorted(labels):
    print("Class %s precision = %s" % (label, metrics.precision(label)))
    print("Class %s recall = %s" % (label, metrics.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))
