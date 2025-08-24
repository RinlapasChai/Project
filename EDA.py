# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display, HTML, display_html 
from pyspark.sql import Row
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.functions import lit, when, col, count, isnan, to_date, expr, from_unixtime, unix_timestamp, date_format, udf
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


# COMMAND ----------

train_users_2 = "dbfs:/FileStore/shared_uploads/s6404053620011@email.kmutnb.ac.th/train_users_2-3.csv"
train = spark.read.options(header="true",inferschema = "true").csv(train_users_2)
train = train.withColumnRenamed("id", "user_id")
train.display()

# COMMAND ----------

train.count()

# COMMAND ----------

#แปลง timestamp_first_active เป็นวันที่
train = train.withColumn("timestamp_first_active", (col("timestamp_first_active") / 1000000).cast("timestamp")) 

# COMMAND ----------

# age มีคนกรอกค่าผิดกรอกเป็นปี ค.ศ. แก้ให้เป็นปีอายุ
filtered_train = train.filter(train.age > 1000)
filtered_train.select('age').describe().display()
# แปลงเป็นปีอายุ
condition = (col('age') > 1000)
train = train.withColumn('age', when(condition, 2015 - col('age')).otherwise(col('age')))
train.select('age').describe().display()

# COMMAND ----------

#ขาจร จองก่อนสมัคร = 29 คน
train = train.withColumn("date_account_created", to_date("date_account_created"))
train = train.withColumn("date_first_booking", to_date("date_first_booking"))
result = train.filter(col("date_first_booking") < col("date_account_created"))
result.count()
# ใช้ join เพื่อลบ result ออกจาก train 29 คน
train = train.join(result, on='user_id', how='left_anti')
train.display()

# COMMAND ----------

# gender กับ first_browser มี -unknown- ให้แทนเป็น null
train = train.na.replace(['-unknown-'], [None], subset=["gender", "first_browser"])
train.display()

# COMMAND ----------

#นับค่า null ในแต่ละคอลัมน์ แต่ date_first_booking ไม่ใช่ค่า null ปกติ มันคือไม่ได้ทำการจอง
null_counts = train.select([count(when(col(c).isNull(), c)).alias(c) for c in train.columns])
null_counts.display()

# COMMAND ----------

#ค่า null ที่ age มีค่าเกือบครึ่งของข้อมูลเราจะทดสอบ correlations โดยแยกตารางที่แทนค่า null เป็น -99 ที่ age
train_null = train.withColumn("age", when(col("age").isNull(), -99).otherwise(col("age")))
train_null.display()

# COMMAND ----------

# dummy 
dum1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/s6404053620011@email.kmutnb.ac.th/train_drop_corr.csv")
dum1.display()

# COMMAND ----------

#ดูความสัมพันธ์ age ที่เป็น null
#ตาราง age = null = -99
columns = dum1.columns
exprs = [col(column).cast('int').alias(column) for column in columns]
dum1 = dum1.select(*exprs)

# สร้าง heatmap
fig, axes = plt.subplots()
fig.set_size_inches(15, 15)    
ax = sns.heatmap(dum1.toPandas().corr(), annot=True, ax=axes)
_ = ax.set_title('Heatmap')
plt.show()
#ค่า null ที่ age ไม่มีความสัมพันธ์กับข้อมูลจริงลบออกไปได้

# COMMAND ----------

#ตาราง ลบค่า null ออกทั้งหมดแล้ว
dum2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/s6404053620011@email.kmutnb.ac.th/train_drop_corr.csv")
dum2.display()

# COMMAND ----------

#แสดงความสัมพันธ์เมื่อลบค่า null ออกทั้งหมด
columns = dum2.columns
exprs = [col(column).cast('int').alias(column) for column in columns]
dum2 = dum2.select(*exprs)

# สร้าง heatmap
fig, axes = plt.subplots()
fig.set_size_inches(15, 15)    
ax = sns.heatmap(dum2.toPandas().corr(), annot=True, ax=axes)
_ = ax.set_title('Heatmap')
plt.show()

# COMMAND ----------

#เพิ่มตาราง codebook 0=ไม่ได้จอง, 1=จอง
train_Codebook = train_null.withColumn(
    "CodeBook",
    when(col("date_first_booking").isNull(), 0).otherwise(1))
train_Codebook.display()

# COMMAND ----------

#แสดงเฉพาะ user ที่จองที่พัก ("CodeBook" = 1)
TrainBook = train_Codebook.filter(col("CodeBook") == 1)
TrainBook.display()

#แสดงเฉพาะ user ที่ไม่จองที่พัก ("CodeBook" = 0)
TrainNobook = train_Codebook.filter(col("CodeBook") == 0)
TrainNobook.display()

# COMMAND ----------

train_Codebook.groupby("Codebook").count().show()

# COMMAND ----------

#ตารางที่ลบค่า null ออกหมดแล้วยกเว้น date first booking
non_null_columns = [column for column in train_Codebook.columns if column != 'date_first_booking']
train_drop = train_Codebook.na.drop(subset=non_null_columns)
train_drop = train_drop.filter((col("Age") >= 15) & (col("Age") <= 85))
train_drop = train_drop.filter((col("Gender") == "MALE") | (col("Gender") == "FEMALE"))

# สร้างคอลัมน์ 'year', 'month', 'year_month'
train_drop = train_drop.withColumn('year', date_format('date_account_created', 'yyyy'))
train_drop = train_drop.withColumn('month', date_format('date_account_created', 'MM'))
train_drop = train_drop.withColumn('year_month', date_format('date_account_created', 'yyyy-MM'))
train_drop.display()

# COMMAND ----------

#ตารางช่วงอายุ 15-86 ปั
train = train_drop.withColumn("age_group", 
                  when((col("age") >= 15) & (col("age") <= 24), "15-24")
                 .when((col("age") >= 25) & (col("age") <= 34), "25-34")
                 .when((col("age") >= 35) & (col("age") <= 44), "35-44")
                 .when((col("age") >= 45) & (col("age") <= 54), "45-54")
                 .when((col("age") >= 55) & (col("age") <= 64), "55-64")
                 .when((col("age") >= 65) & (col("age") <= 74), "65-74")
                 .when((col("age") >= 75) & (col("age") <= 86), "75-86")
                 .otherwise("Unknown")
              )


train.display()

# COMMAND ----------

train.count()

# COMMAND ----------

#สร้างตาราง segment ว่าบริษัทการจ่ายค่าโฆษณาหรือไม่ (paid = จ่ายค่าโฆษณา, not paid = ไม่จ่ายค่าโฆษณา)
def sets(i):
    if i in ["content", "sem-non-brand", "sem-brand", "remarketing", "api"]:
        return "paid"
    elif i in ["direct", "seo"]:
        return "not paid"
    else:
        return "other"
    
sets = udf(sets, StringType())
train = train.withColumn("segment", sets(col("affiliate_channel")))
train.display()

# COMMAND ----------

#จัดเรียงคอลัมน์ใหม่
train = train.select(["user_id", "date_account_created", "timestamp_first_active", "date_first_booking", "year_month", "month", "year", "age", "age_group", "gender", "language", "signup_method", "signup_flow", "affiliate_channel", "segment", "affiliate_provider", "signup_app", "first_device_type", "first_browser", "country_destination", "CodeBook"])
train.display()

# COMMAND ----------

#จำนวนครั้งของแต่ละค่าในคอลัมน์ 'first_browser' 
count_first_browser = train.groupBy('first_browser').count()
count_first_browser.display()

# COMMAND ----------

#จำนวนครั้งของแต่ละค่าในคอลัมน์ 'first_device_type'
count_first_device_type = train.groupBy('first_device_type').count()
count_first_device_type.display()

# COMMAND ----------

#จำนวน new user ที่เข้ามาจากช่องทางเสียค่าโฆษณา ,ไม่เสียค่าโฆษณา และอื่นๆ เป็นรายเดือน
ad_types = ["paid", "not paid", "other"]
year_months = []
for year in range(2010, 2014):
    for month in range(1, 13):
        year_month = f"{year}-{month:02}"  # ให้เลขเดือนมีรูปแบบ 2 ตำแหน่ง (01, 02, ..., 12)
        year_months.append(year_month)
for month in range(1, 7):
    year_month = f"2014-{month:02}"
    year_months.append(year_month)
 
ad_counts = {}
 
for year_month in year_months:
    for ad_type in ad_types:
        count = train.filter((col("segment") == ad_type) & (col("year_month") == year_month)).count()
        if year_month not in ad_counts:
            ad_counts[year_month] = {}
        ad_counts[year_month][ad_type] = count
 
for year_month, counts in ad_counts.items():
    print(f"Year-Month: {year_month}")
    for ad_type, count in counts.items():
        print(f"{ad_type}: {count}")

# COMMAND ----------

#กราฟแสดงจำนวน new user ที่เข้ามาจากช่องทางเสียค่าโฆษณา ,ไม่เสียค่าโฆษณา และอื่นๆ เป็นรายเดือน
ad_types = ["paid", "not paid", "other"]
year_months = []
for year in range(2010, 2014):
    for month in range(1, 13):
        year_month = f"{year}-{month:02}"
        year_months.append(year_month)
for month in range(1, 7):
    year_month = f"2014-{month:02}"
    year_months.append(year_month)
 
ad_counts = {}
 
for year_month in year_months:
    for ad_type in ad_types:
        count = train.filter((col("segment") == ad_type) & (col("year_month") == year_month)).count()
        if year_month not in ad_counts:
            ad_counts[year_month] = {}
        ad_counts[year_month][ad_type] = count
 
plt.figure(figsize=(20,10))
for ad_type in ad_types:
    counts = [ad_counts[year_month][ad_type] for year_month in year_months]
    plt.plot(year_months, counts, label=ad_type)
 
 
plt.xlabel('Year-Month')
plt.ylabel('Ad Counts')
plt.title('Segment')
plt.xticks(rotation=45)  
 
plt.legend()
plt.show()
 

# COMMAND ----------

#กราฟแสดง 5 อันดับแรกที่มากที่สุดของ 'first_browser' ที่ new user เข้ามา
df = count_first_browser.toPandas()
df = df.sort_values(by="count", ascending=False)
df = df.head(5)
df.plot(kind="bar", x="first_browser", y="count", title="number_first_browser")
plt.xlabel('first_browser')
plt.ylabel('count_first_browser')
plt.show()

# COMMAND ----------


#กราฟแสดง 'first_device_type' ของ new user
df = count_first_device_type.toPandas()
df = df.sort_values(by="count", ascending=False)
df.plot(kind="bar", x="first_device_type", y="count", title="number_first_device_type")
plt.xlabel('first_device_type')
plt.ylabel('count_first_device_type')
plt.show()

# COMMAND ----------

#กราฟแท่งแสดงอัตราส่วนที่ เสียค่าโฆษณาแล้วมี่คนจองห้องพัก, ไม่เสียค่าโฆษณาแล้วมี่คนจองห้องพัก, และช่องทางอื่นที่คนจองที่พัก
paid_ads_count = train.filter(col("segment") == "paid").count()
not_paid_ads_count = train.filter(col("segment") == "not paid").count()
other_ads_count = train.filter(col("segment") == "other").count()
 
paid_ads_booking_count = train.filter((col("segment") == "paid") & (col("CodeBook") == 1)).count()
not_paid_ads_booking_count = train.filter((col("segment") == "not paid")& (col("CodeBook") == 1)).count()
other_ads_booking_count = train.filter((col("segment") == "other")& (col("CodeBook") == 1)).count()
 
segments = ['paid', 'not paid', 'other']
segment_counts = [paid_ads_count, not_paid_ads_count, other_ads_count]
booking_counts = [paid_ads_booking_count, not_paid_ads_booking_count, other_ads_booking_count]
plt.bar(segments, segment_counts, label='Ads Count', color='b', alpha=0.7)
plt.bar(segments, booking_counts, label='Booking Count', color='orange', alpha=0.7)
plt.xlabel('Segment')
plt.ylabel('Count')
plt.title('Booking & Ads')
plt.legend()
plt.show()

# COMMAND ----------


