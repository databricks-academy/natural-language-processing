# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

|moo1
%md
# Hyperparameter Tuning using Hyperopt

The [Hyperopt library](https://github.com/hyperopt/hyperopt) allows for parallel hyperparameter tuning using either random search or Tree of Parzen Estimators (TPE). With MLflow, we can record the hyperparameters and corresponding metrics for each hyperparameter combination. You can read more on [SparkTrials w/ Hyperopt](https://github.com/hyperopt/hyperopt/blob/master/docs/templates/scaleout/spark.md).

## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
- Tune the LSTM model we trained in the previous model using Hyperopt


# COMMAND ----------

|moo1
%run ./Includes/Classroom-Setup


# COMMAND ----------

|moo1
from pyspark.sql.functions import col, when
import numpy as np
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pandas as pd
import mlflow.tensorflow
import mlflow
from hyperopt import fmin, hp, tpe, STATUS_OK, SparkTrials


# COMMAND ----------

|moo1
text_df = (spark.read.parquet("/mnt/training/reviews/reviews_cleaned.parquet")
           .select("Text", "Score")
           .limit(5000) ### limit to only 5000 rows to reduce training time
          )
text_df = text_df.withColumn("sentiment", when(col("Score") > 3, 1).otherwise(0))
display(text_df)


# COMMAND ----------

|moo1
%md
### Preprocessing
- Includes tokenization and padding


# COMMAND ----------

|moo1
(train_df, test_df) = text_df.randomSplit([0.8, 0.2])
train_pdf = train_df.toPandas()
X_train = train_pdf["Text"].values
y_train = train_pdf["sentiment"].values


# COMMAND ----------

|moo1
vocab_size = 10000
max_length = 500
tokenizer = Tokenizer(num_words=vocab_size)
tokenizer.fit_on_texts(X_train)

### Convert the texts to sequences.
X_train_seq = tokenizer.texts_to_sequences(X_train)
X_train_seq_padded = pad_sequences(X_train_seq, maxlen=max_length)

### Repeat for test_df 
test_pdf = test_df.toPandas()
X_test = test_pdf["Text"].values
y_test = test_pdf["sentiment"].values
X_test_seq = tokenizer.texts_to_sequences(X_test)
X_test_seq_padded = pad_sequences(X_test_seq, maxlen=max_length)


# COMMAND ----------

|moo1
%md
### Define LSTM Architecture


# COMMAND ----------

|moo1
def create_lstm(hpo):
  ### Below is a slightly simplified architecture compared to the previous notebook to save time 
  embedding_dim = 64
  lstm_out = 32 
  
  inputs = keras.Input(shape=(None,), dtype="int32")
  x = layers.Embedding(vocab_size, embedding_dim)(inputs)
  x = layers.Bidirectional(layers.LSTM(lstm_out))(x)
  outputs = layers.Dense(1, activation="sigmoid")(x)
  model = keras.Model(inputs, outputs)

  return model


# COMMAND ----------

|moo1
%md
### Define Hyperopt's objective function

<img src="https://files.training.databricks.com/images/icon_note_24.png"/> We need to import `tensorflow` within the function due to a pickling issue.  <a href="https://docs.databricks.com/applications/deep-learning/single-node-training/tensorflow.html#tensorflow-2-known-issues" target="_blank">See known issues here.</a>


# COMMAND ----------

|moo1
def run_lstm(hpo):
  ### Need to include the TF import due to serialization issues
  import tensorflow as tf
  
  model = create_lstm(hpo)
  
  ### Select Optimizer
  optimizer_call = getattr(tf.keras.optimizers, hpo["optimizer"])
  optimizer = optimizer_call(learning_rate=hpo["learning_rate"])

  ### Compile model 
  model.compile(optimizer, loss="binary_crossentropy", metrics=["AUC"])
  history =  model.fit(X_train_seq_padded, 
                       y_train, 
                       batch_size=32, 
                       epochs=int(hpo["num_epoch"]), 
                       validation_split=0.1)

  ### Since we would like to maximize AUC, we need to add the negative sign
  obj_metric = history.history["loss"][-1]
  return {"loss": -obj_metric, "status": STATUS_OK}


# COMMAND ----------

|moo1
%md
### Define search space for tuning

We need to create a search space for HyperOpt and set up SparkTrials to allow HyperOpt to run in parallel using Spark worker nodes. We can also start a MLflow run to automatically track the results of HyperOpt's tuning trials.


# COMMAND ----------

|moo1
space = {
  "num_epoch": hp.quniform("num_epoch", 1, 3, 1), 
  "learning_rate": hp.loguniform("learning_rate", np.log(1e-4), 0), ## max is 1 because exp(0) is 1, added np.log to prevent exp() resulting in negative values
  "optimizer": hp.choice("optimizer", ["Adadelta", "Adam"])
 }

spark_trials = SparkTrials(parallelism=2)

with mlflow.start_run() as run:
  best_hyperparam = fmin(fn=run_lstm, 
                         space=space, 
                         algo=tpe.suggest, 
                         max_evals=4, ### ideally this should be an order of magnitude larger than parallelism
                         trials=spark_trials,
                         rstate=np.random.RandomState(42))

best_hyperparam


# COMMAND ----------

|moo1
%md
## Lab

Try tuning other configurations!
- "lstm_out": hp.quniform("lstm_out", 20, 150, 1), 
- "embedding_dim": hp.quniform("embedding_dim", 40, 512, 1),


# COMMAND ----------

|moo1
# ANSWER

### First modify the model 
def create_lstm(hpo):
  inputs = keras.Input(shape=(None,), dtype="int32")
  x = layers.Embedding(vocab_size, int(hpo["embedding_dim"]))(inputs)
  x = layers.Bidirectional(layers.LSTM(int(hpo["lstm_out"])))(x)
  outputs = layers.Dense(1, activation="sigmoid")(x)
  model = keras.Model(inputs, outputs)
  return model

### Then modify the search space 
space = {
  "embedding_size": hp.quniform("embedding_size", 40, 256, 1),
  "lstm_size": hp.quniform("lstm_size", 20, 150, 1),
  "num_epoch": hp.quniform("num_epoch", 1, 5, 1),
  "learning_rate": hp.loguniform("learning_rate", np.log(1e-4), 0), ## max is 1 because exp(0) is 1, added np.log to prevent exp() resulting in negative values
  "optimizer": hp.choice("optimizer", ["Adadelta", "Adam"])
 }

# COMMAND ----------

|moo1
%md-sandbox
&copy; 2021 Databricks, Inc. All rights reserved.<br/>
Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
<br/>
<a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>|moo2
