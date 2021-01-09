from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, LongType
from pyspark.sql.functions import desc, asc, year
from keras import models, layers
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

stock = "TSLA"

if __name__ == "__main__":
	# Khởi tạo Spark session
	conf = SparkConf().setMaster("local") \
        .setAppName("StockPredict")

	spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

	# Tạo DataFrame từ tập dữ liệu được lưu trữ trên HDFS
	df = spark.read.format("json") \
        .option("header", "false") \
        .option("inferSchema", "true") \
        .load("hdfs://masternode:9000/predict/"+ stock +".json").na.drop()
	df = df.withColumn('time', f.to_date('time')).withColumn('year', year("time"))
	dfPandas = df.toPandas()

	# Chia tập dữ liệu thành 2 phần là train và test
	training_set = df.select("time", "open").where(df["year"] < 2020)
	test_set = df.select("time", "open").where(df["year"] >=  2020)

	training_set = training_set.toPandas().values[:,1]
	test_set = test_set.toPandas().values[:,1]

	open_price = df.select("time", "open").toPandas().values[:,1]

	# Xây dựng mô hình dự đoán bằng LSTM với Lookback là 24 nghĩa là lấy giá trị của 24 ngày trước để dự đoán 1 ngày sau
	scale = MinMaxScaler()
	training_set_scaled = scale.fit_transform(open_price.reshape(-1, 1))
	LOOKBACK = 24
	X_train = []
	y_train = []
	for i in range(LOOKBACK, training_set.shape[0]):
		X_train.append(training_set_scaled[i-LOOKBACK:i, 0])
		y_train.append(training_set_scaled[i, 0])
	X_train, y_train = np.array(X_train), np.array(y_train)
	X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))

	model = Sequential()
	model.add(LSTM(units = 12, return_sequences = True, input_shape = (X_train.shape[1], 1)))
	model.add(Dropout(0.2))
	model.add(LSTM(units = 12, return_sequences = False))
	model.add(Dropout(0.2))
	model.add(Dense(units = 1))
	model.compile(loss='mean_absolute_error', optimizer='adam', metrics=['accuracy'])
	model.fit(X_train, y_train, epochs = 100, batch_size = 32)

	total_len = open_price.shape[0]
	actual_values = open_price[LOOKBACK:]
	inputs = open_price.reshape(-1,1)
	inputs = scale.transform(inputs)
	X_test = []
	for i in range(LOOKBACK, total_len):
		X_test.append(inputs[i-LOOKBACK:i, 0])
	X_test = np.array(X_test)
	X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

	# Dự đoán kết quả từ mô hình và lưu thành tập dữ liệu
	predicted_stock_price = model.predict(X_test)
	predicted_stock_price = scale.inverse_transform(predicted_stock_price)

	pd.DataFrame(predicted_stock_price).to_csv("result.csv")

	spark.sparkContext.stop()
