# Databricks notebook source
# filter out warnings from python
# issue: https://github.com/RaRe-Technologies/smart_open/issues/319
import warnings
warnings.filterwarnings("ignore")

displayHTML("Preparing the learning environment...")


# COMMAND ----------

|moo1
%run "./Class-Utility-Methods"


# COMMAND ----------

|moo1
%run "./Dataset-Mounts"


# COMMAND ----------

|moo1
def init_mlflow_as_job():
  import mlflow
  job_experiment_id = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
      dbutils.entry_point.getDbutils().notebook().getContext().tags()
    )["jobId"]

  if job_experiment_id:
    mlflow.set_experiment(f"/Curriculum/Test Results/Experiments/{job_experiment_id}")
    
init_mlflow_as_job()


# COMMAND ----------

|moo1
courseType = "il"
username = getUsername()
userhome = getUserhome()
workingDir = getWorkingDir(courseType).replace("_pil", "")


# COMMAND ----------

|moo1
courseAdvertisements = dict()
courseAdvertisements["username"] = (
    "v",
    username,
    "No additional information was provided.",
)
courseAdvertisements["userhome"] = (
    "v",
    userhome,
    "No additional information was provided.",
)
courseAdvertisements["workingDir"] = (
    "v",
    workingDir,
    "No additional information was provided.",
)
allDone(courseAdvertisements)|moo2
