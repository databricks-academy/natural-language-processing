# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

|moo1
%md
# Pre-Processing Text at Scale

This lesson introduces basic text processing steps using distributed libraries like SparkML.


## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:
* Learn about distributed NLP libraries
* Tokenize text in parallel
* Remove stopwords
* Calculate the term-frequency-inverse-document-frequency (TFIDF) vectors for your dataset


# COMMAND ----------

|moo1
%run ./Includes/Classroom-Setup


# COMMAND ----------

|moo1
%md
## Distributed Libraries

As the amount of text-based data increases, many new distributed natural language processing libraries are being developed in addition to traditional single-node libraries to speed up processing time and increase the volume of processed text.

Some of these distributed libraries include:
* [SparkML](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ml.html)/MLlib
* [Spark CoreNLP](https://github.com/databricks/spark-corenlp) (Spark wrapper for [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/))
* John Snow Labs [SparkNLP](https://nlp.johnsnowlabs.com/docs/en/quickstart)

We will be using <a href="https://spark.apache.org/docs/latest/ml-guide.html" target="_blank">SparkML</a> for this lesson.


# COMMAND ----------

|moo1
%md
## Tokenize

The first step in NLP is to break up the text into smaller units, words. Instead of treating the entire text as 1 string, we want to break it up into a list of strings, where each string is a single word, for further processing. This process is called **tokenizing**. This is a crucial first step because we are able to match/compare and featurize specific words better than we can a single string of words.

The easiest way to tokenize in Python is to call the`.split()` method to split on whitespaces (you can pass in parameters to `.split()` too).


# COMMAND ----------

|moo1
%md
### Tokenizing using Python's `.split()`


# COMMAND ----------

|moo1
sentence = "We are tokenizing this sentence using Python string's .split() method!"

print(f"Sentence: {sentence}")
print(f"Tokenized Sentence: {sentence.split()}")


# COMMAND ----------

|moo1
%md
### Tokenizing using SparkML

You'll notice that we can process + tokenize each review independently, so this is a great use case for using SparkML.


Within SparkML, there are 2 different <a href="https://spark.apache.org/docs/latest/ml-features.html#tokenizer" target="_blank"> tokenization functions </a> that will execute in parallel across your DataFrame:
* `Tokenizer`: Built in tokenizer which splits your sentence on punctuation marks
* `RegexTokenizer`: Customizable tokenizer that uses specified regex strings to split the text

Another important first step of text processing is to **standardize the casing and punctuation** of each word so the strings are even more comparable. Both of these SparkML functions lowercase every token while creating it by default.


# COMMAND ----------

|moo1
%md
In our reviews dataset, there are a couple of places where the html text, `<\ br>`, shows up. The presence of this string doesn't contribute to the meaning of the text, so we don't want to include it as a token. Since punctuation within a short reviews text is not super important, we are simply going to use the `RegexTokenizer` with the regex string `\\W` which indicates we only want to keep alphanumeric tokens, removing all punctuations as well. If we used the `Tokenizer` instead, it will split up the html string, `<\ br>`, into 2 different tokens as it tries to optimize around the `<` and `>` punctuation marks, making its removal more difficult later on.

Sometimes you might want to keep punctuation (e.g. was it `!` or `!!!?!!?!!`?). However, for this example, we will remove punctuation.

After applying this `RegexTokenizer`, we will have accomplished the following objectives:
1. Creating purely alphanumeric tokens (all punctuation is removed)
2. Lowercasing all tokens to standardize case
3. Removing the arbitrary html text `<\ br>`


# COMMAND ----------

|moo1
%md

First read in the `textDF` DataFrame.

Note: We are tokenizing the same `textDF` that we created and saved in the previous notebook.


# COMMAND ----------

|moo1
text_df = spark.read.parquet("/mnt/training/reviews/reviews_cleaned.parquet")
display(text_df)


# COMMAND ----------

|moo1
%md
Run the following cell to apply the `RegexTokenizer` transformer to our text column!

A transformer has a `.transform()` method which converts one DataFrame into another, usually by appending one or more columns. In our specific case, the `RegexTokenizer` will use the `inputCol="Text"` column of the `textDF` that we give it to return a new DataFrame which is `textDF` with an additional column named `outputCol="Tokens"` containing the tokens of our text.


# COMMAND ----------

|moo1
from pyspark.ml.feature import Tokenizer, RegexTokenizer

# Uncomment the line below if you wish to use SparkML's Tokenizer function instead
# tokenizer = Tokenizer(inputCol="Text", outputCol="tokens")

tokenizer = RegexTokenizer(inputCol="Text", outputCol="Tokens", pattern="\\W")
tokenized_df = tokenizer.transform(text_df)
display(tokenized_df.limit(20))


# COMMAND ----------

|moo1
%md
### Comparison between SparkML's Tokenizer and Python's .split()

In the following 2 cells we will be comparing the effects of using SparkML's Tokenizer and Python's `.split()`.

Let's first try transforming `textDF` using SparkML so the returned DataFrame will have a new column containing the tokens list of each review's text.


# COMMAND ----------

|moo1
# using SparkML
spark_df = tokenizer.transform(text_df)


# COMMAND ----------

|moo1
%md
To achieve the same result using Python's `.split()` in series, we have to first convert our Spark DataFrame to a Pandas DataFrame which we call `textPDF`. 


# COMMAND ----------

|moo1
text_pdf = text_df.toPandas()


# COMMAND ----------

|moo1
%md
Now we will use Pandas' `.apply()` method to append a column, "Tokens", containing the result of calling `.split()` on each review in the "Text" column.


# COMMAND ----------

|moo1
# using Python's .split
text_pdf["Tokens"] = text_pdf["Text"].apply(lambda text: text.split())


# COMMAND ----------

|moo1
%md
Take a look at the execution times of using SparkML and of using `.split()`. Wow, was SparkML really that much faster than using Python in series? Not necessarily.

In Spark, there are 2 types of operations, a transformation or an action. A transformation (like `.transform`, `.select`, and `.groupby`) is lazily evaluated, while an action (like `display`, `.show`, and `.count`), is eagerly evaluated. In Python and Pandas, every operation is eagerly evaluated.

What this means for us is that `spark_df = tokenizer.transform(textDF)` didn't actually tokenize our DataFrame yet (no action called), while `textPDF["Tokens"] = textPDF["Text"].apply(lambda text: text.split())` was eagerly executed. If the dataset is large enough to outweigh the communication overhead of Spark, then using SparkML to tokenize will be significantly faster than using `.split` to accomplish the same thing.


# COMMAND ----------

|moo1
%md
After tokenizing our text, we can now take a look at the distribution of words in our dataset. Below are the top 25 most commonly occurring tokens.


# COMMAND ----------

|moo1
from pyspark.sql.functions import explode, col

word_dist = (
    tokenized_df.withColumn("token", explode(col("Tokens")))
    .groupBy("token")
    .count()
    .sort(col("count").desc())
)

display(word_dist.limit(30))


# COMMAND ----------

|moo1
%md

A quick glance at these "top" words reveals that most of the reviews are filled with many terms that do not necessarily add value to the content of the text (e.g. `the`, `is`, etc). The html text `br` even shows up as one of the top 10 tokens! In the following cells, we will learn how to handle this.


# COMMAND ----------

|moo1
%md
## Stopwords Remover

After tokenizing our text, we found that the most frequently occurring words are articles and prepositions such as "the", "a", "and", etc. and some domain specific words which carry little meaning such as "br", "href", "www", "http", and "com". Because none of these words significantly contribute to the overall meaning or rating of the review, we are going to remove them from our list of tokens in a process called **stopwords removal**. The tokens that we are removing are called `stopwords`.

We are going to use the built-in <a href="https://spark.apache.org/docs/latest/ml-features.html#stopwordsremover" target="_blank">SparkML</a> `StopWordsRemover` feature to accomplish this task. The function has a list of default stopwords (obtained through `StopWordsRemover().getStopWords()`) but we can also specify the exact stopwords that we wish to use by passing a list into its `stopWords` field. In the next cell we will create a list of stopwords to pass into our stop word remover.


# COMMAND ----------

|moo1
%md
### Setting Default and Custom Stopwords


# COMMAND ----------

|moo1
from pyspark.ml.feature import StopWordsRemover

# Use default stopwords and add "br" to stopwords list
stop_words = StopWordsRemover().getStopWords() + ["br", "href", "www", "http", "com"]
print(f"Our stopwords:\n {stop_words}")


# COMMAND ----------

|moo1
%md

Now use SparkML's `StopWordsRemover` to remove all instances of `stopWords` from our dataset and take a look at the resulting token distribution.


# COMMAND ----------

|moo1
stopwords_remover = StopWordsRemover(inputCol="Tokens", outputCol="CleanTokens", stopWords=stop_words)
processed_df = stopwords_remover.transform(tokenized_df)

# Resulting top 10 tokens
word_dist_new = (
    processed_df.withColumn("indivTokens", explode(col("CleanTokens")))
    .groupBy("indivTokens")
    .count()
    .sort(col("count").desc())
)

display(word_dist_new.limit(10))


# COMMAND ----------

|moo1
%md

Now the most frequently appearing words are more meaningful to us and we can move on to some more advanced preprocessing steps. We could continue to refine this stopword list (e.g. remove `m`), but let's continue.


# COMMAND ----------

|moo1
%md
## Term Frequency-Inverse Document Frequency

Now that we have a cleaned list of tokens per document, a simple way to quantify the importance of a word in a text is through its term frequency-inverse document frequency (tf-idf) weight.

There are 3 main steps in calculating the tf-idf weight of a word w.

1. Term Frequency of word w in document d
  - Raw count of a word in a document normalized by the length of that document

  $$ TF(w,d) = \frac{\text{Number of times w appears in d}}{\text{Total number of terms d}} $$

2. Inverse Document Frequency of word w given a set of documents D
  - Measure of how important a word is within the set of documents
    - If it's common across all documents the IDF value would be low

    $$ IDF(w,D)=\log\frac{(\text{Number of documents in }D)+1}{ DF(w,D)+1} $$ where $$DF(w,D) = \text{number of documents that contain the word w } $$
3. Term Frequency-Inverse Document Frequency
  - Combines both TF and IDF values through multiplication
  $$ TFIDF(w,d,D)=TF(w,d)⋅IDF(w,D)$$



#### From this equation we can see the following:
- High TF-IDF score: word w appears frequently in document d but not a lot in the rest of the documents
- Low TF-IDF score: word w appears frequently in a lot of other documents and not so much in document d

Thus the higher a TF-IDF score of word w in document d, the more important the word w is to document d within the corpus D of all documents.

**NOTE**: This would also give stopwords little importance because they occur many times in many documents.


# COMMAND ----------

|moo1
%md
### Using SparkML to calculate TF-IDF scores
You can access the [documentation guide](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) for a more detailed description, but the high level steps are outlined below.

Steps:
1. [HashingTF](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.HashingTF.html?highlight=hashingtf#pyspark.ml.feature.HashingTF) or [CountVectorizer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.CountVectorizer.html?highlight=countvectorizer#pyspark.ml.feature.CountVectorizer)
  - Both functions will generate a vector (list) for each text where each index represents a specific token in the entire dataset and the value at each index being the frequencies of the represented token.
  - `HashingTF` will generally be faster on a larger corpus so we will be using in it in the following code cell.
2. [IDF](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.IDF.html?highlight=idf#pyspark.ml.feature.IDF)
  - Will fit and return an IDFModel
  - Takes feature vectors (in our case, the result of the TF step) and scales the vector by its "importance".
3. [Normalizer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.Normalizer.html?highlight=normalizer#pyspark.ml.feature.Normalizer)
  - Normalize (l2 norm) resulting TF-IDF vectors so that each vector will have unit length and simplify the angle calculation between any 2 vectors

Note: Since our TF-IDF vectors will have a lot of zero entries (at indices where a token is not present), Spark stores and displays these vectors as [SparseVectors](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.linalg.SparseVector.html?highlight=sparsevector#pyspark.ml.linalg.SparseVector) to save space.


# COMMAND ----------

|moo1
%md
## Creating TF-IDF Feature Vectors for each row


# COMMAND ----------

|moo1
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, Normalizer

hashing_tf = HashingTF(inputCol="CleanTokens", outputCol="TermFrequencies")  # Or use CountVectorizer
idf = IDF(inputCol="TermFrequencies", outputCol="TFIDFScore")
normalizer = Normalizer(inputCol="TFIDFScore", outputCol="TFIDFScoreNorm", p=2)  # Normalize L2

# Adding functions into a pipeline to streamline calling process
tfidf_pipeline = Pipeline(stages=[hashing_tf, idf, normalizer])
tfidf_model = tfidf_pipeline.fit(processed_df)
tfidf_df = tfidf_model.transform(processed_df).drop("TFIDFScore")

display(tfidf_df.drop("Tokens").limit(10))


# COMMAND ----------

|moo1
%md
Take a look at the `TFIDFScoreNorm` column of the DataFrame which contains an alternate way for us to represent our text. Instead of referring to the texts as strings we can now use these high dimensional vectors.


# COMMAND ----------

|moo1
%md-sandbox
&copy; 2021 Databricks, Inc. All rights reserved.<br/>
Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
<br/>
<a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>|moo2
