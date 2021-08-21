# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

|moo1
%md
# Sentiment Analysis with LSTM and MLflow

## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
- Build a bi-directional Long Short Term Memory (LSTM) model using [tensorflow.keras](https://www.tensorflow.org/api_docs/python/tf/keras) to classify the sentiment of text reviews
- Log model inputs and outputs using [MLflow](https://www.mlflow.org/docs/latest/index.html)


# COMMAND ----------

|moo1
%run ./Includes/Classroom-Setup


# COMMAND ----------

|moo1
from pyspark.sql.functions import col, when
import numpy as np
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pandas as pd
import mlflow
import mlflow.tensorflow


# COMMAND ----------

|moo1
text_df = (spark.read.parquet("/mnt/training/reviews/reviews_cleaned.parquet")
           .select("Text", "Score")
           .limit(5000) ### limit to only 5000 rows to reduce training time
          )


# COMMAND ----------

|moo1
### Ensure that there are no missing values
text_df.filter(col("Score").isNull()).count()


# COMMAND ----------

|moo1
text_df = text_df.withColumn("sentiment", when(col("Score") > 3, 1).otherwise(0))
display(text_df)


# COMMAND ----------

|moo1
positive_review_percent = text_df.filter(col("sentiment") == 1).count() / text_df.count() * 100
print(f"{positive_review_percent}% of reviews are positive")


# COMMAND ----------

|moo1
(train_df, test_df) = text_df.randomSplit([0.8, 0.2])


# COMMAND ----------

|moo1
train_positive_review_percent = train_df.filter(col("sentiment") == 1).count() / train_df.count() * 100
test_positive_review_percent = test_df.filter(col("sentiment") == 1).count() / test_df.count() * 100
print(f"{train_positive_review_percent}% of reviews in the train_df are positive")
print(f"{test_positive_review_percent}% of reviews in the test_df are positive")


# COMMAND ----------

|moo1
train_pdf = train_df.toPandas()
X_train = train_pdf["Text"].values
y_train = train_pdf["sentiment"].values


# COMMAND ----------

|moo1
%md
### Tokenization


# COMMAND ----------

|moo1
vocab_size = 10000
tokenizer = Tokenizer(num_words=vocab_size)
tokenizer.fit_on_texts(X_train)
### convert the texts to sequences
X_train_seq = tokenizer.texts_to_sequences(X_train)


# COMMAND ----------

|moo1
%md
Now, let's compute some basic statistics to understand our training data more!


# COMMAND ----------

|moo1
l = [len(i) for i in X_train_seq]
l = np.array(l)
print(f"minimum number of words: {l.min()}")
print(f"median number of words: {np.median(l)}")
print(f"average number of words: {l.mean()}")
print(f"maximum number of words: {l.max()}")


# COMMAND ----------

|moo1
print(X_train[0])
print("\n")
### The text gets converted to a list of integers
print(X_train_seq[0])


# COMMAND ----------

|moo1
%md
### Padding


# COMMAND ----------

|moo1
max_length = 800
X_train_seq_padded = pad_sequences(X_train_seq, maxlen=max_length)


# COMMAND ----------

|moo1
%md
### Repeat the process of tokenization and padding for `test_df`


# COMMAND ----------

|moo1
test_pdf = test_df.toPandas()
X_test = test_pdf["Text"].values
y_test = test_pdf["sentiment"].values
X_test_seq = tokenizer.texts_to_sequences(X_test)
X_test_seq_padded = pad_sequences(X_test_seq, maxlen=max_length)


# COMMAND ----------

|moo1
%md
### Define bi-directional LSTM Architecture

A bi-directional LSTM architecture is largely the same as the base LSTM architecture. But it has additional capacity to understand text as it can scan the text from right to left, in addition from left to right. The bi-directional architecture mimics how humans read text. We often read text to its left and right to figure out the context or to guess the meaning of an unknown word. 

There are a couple hyperparameters within the LSTM architecture itself that can be tuned:

- `embedding_dim` : The embedding layer encodes the input sequence into a sequence of dense vectors of dimension `embedding_dim`.
- `lstm_out` : The LSTM transforms the vector sequence into a single vector of size `lstm_out`, containing information about the entire sequence.

<img src="https://www.researchgate.net/profile/Latifa-Nabila-Harfiya/publication/344751031/figure/fig2/AS:948365760155651@1603119425682/The-unfolded-architecture-of-Bidirectional-LSTM-BiLSTM-with-three-consecutive-steps.png" width=500>


# COMMAND ----------

|moo1
embedding_dim = 128
lstm_out = 64

### Input for variable-length sequences of integers
inputs = keras.Input(shape=(None,), dtype="int32")

### Embed each integer (i.e. each word) in a 128-dimensional word vectors
x = layers.Embedding(vocab_size, embedding_dim)(inputs)

### Add 2 bidirectional LSTMs
x = layers.Bidirectional(layers.LSTM(lstm_out, return_sequences=True))(x)
x = layers.Bidirectional(layers.LSTM(lstm_out))(x)

### Add a classifier
outputs = layers.Dense(1, activation="sigmoid")(x)
model = keras.Model(inputs, outputs)
model.summary()


# COMMAND ----------

|moo1
%md
### Train LSTM and log using MLflow


# COMMAND ----------

|moo1
mlflow.tensorflow.autolog()

with mlflow.start_run() as run:
  
  model.compile(optimizer=keras.optimizers.Adam(lr=1e-3), 
                loss="binary_crossentropy", 
                metrics=["AUC"])
  
  model.fit(X_train_seq_padded, 
            y_train, 
            batch_size=32, 
            epochs=1, 
            validation_split=0.1)


# COMMAND ----------

|moo1
%md
### Evaluate on test_data


# COMMAND ----------

|moo1
test_loss, test_auc = model.evaluate(X_test_seq_padded, y_test, verbose=False)
print(f"Test loss is {test_loss}. Test AUC is {test_auc}")


# COMMAND ----------

|moo1
%md
### Make inference at scale using `mlflow.pyfunc.spark_udf`

You can read more about the function [here](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.spark_udf).


# COMMAND ----------

|moo1
logged_model = f"runs:/{run.info.run_id}/model"

### Load model as a Spark UDF
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)


# COMMAND ----------

|moo1
df = spark.createDataFrame(pd.concat([pd.DataFrame(data=y_test, columns=["label"]), 
                                      pd.DataFrame(X_test_seq_padded), 
                                      pd.DataFrame(data=X_test, columns=["text"])], axis=1))
pred_df = (df
           .withColumn("predictions", loaded_model(*df.drop("text", "label").columns))
           .select("text", "label", "predictions")
           .withColumn("predicted_label", when(col("predictions") > 0.5, 1).otherwise(0)))


# COMMAND ----------

|moo1
display(pred_df)


# COMMAND ----------

|moo1
%md
## Lab

Log the following information to the same MLflow run above:
 - parameter: 
   - data path (`dbfs:/mnt/training/reviews/reviews_cleaned.parquet`)
   - maximum vocabulary size (`vocab_size`)
   - maximum sentence length (`max_length`)
   - embedding dimension (`embedding_dim`)
   - lstm output (`lstm_out`)
 - tag:
   - team (`NLP`)
   - Note that a tag can be modified on the MLflow UI after it is logged, but parameters and metrics are non-editable.


# COMMAND ----------

|moo1
TODO 
with mlflow.start_run(run_id=# FILL_IN, experiment_id=# FILL_IN):
  mlflow.log_params({"data_path": # FILL_IN})
  mlflow.set_tag("team", "# FILL_IN")


# COMMAND ----------

|moo1
%md-sandbox
&copy; 2021 Databricks, Inc. All rights reserved.<br/>
Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
<br/>
<a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>|moo2
