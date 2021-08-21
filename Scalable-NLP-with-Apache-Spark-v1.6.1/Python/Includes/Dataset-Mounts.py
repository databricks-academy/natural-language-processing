# Databricks notebook source
# MAGIC %scala
// ALL_NOTEBOOKS
def cloudAndRegion(cloudAndRegionOverride:Tuple2[String,String]) = {
  import com.databricks.backend.common.util.Project
  import com.databricks.conf.trusted.ProjectConf
  import com.databricks.backend.daemon.driver.DriverConf
  
  if (cloudAndRegionOverride != null) {
    // This override mechanisim is provided for testing purposes
    cloudAndRegionOverride
  } else {
    val conf = new DriverConf(ProjectConf.loadLocalConfig(Project.Driver))
    (conf.cloudProvider.getOrElse("Unknown"), conf.region)
  }
}

// These keys are read-only so they're okay to have here
val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
val awsAuth = s"${awsAccessKey}:${awsSecretKey}"

def AWS_REGION_MAP() = {
  Map(
    "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
    "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
    "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
    "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
    "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
    "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
    "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
    "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),
    "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
    "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
    "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
    "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
    "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
  )
}

def getAwsMapping(region:String):(String,Map[String,String]) = {
  AWS_REGION_MAP().getOrElse(region, AWS_REGION_MAP()("_default"))
}

def MSA_REGION_MAP() = {
  Map(
    "australiacentral"    -> ("dbtrainaustraliasoutheas", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiacentral2"   -> ("dbtrainaustraliasoutheas", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiaeast"       -> ("dbtrainaustraliaeast", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=FM6dy59nmw3f4cfN%2BvB1cJXVIVz5069zHmrda5gZGtU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiasoutheast"  -> ("dbtrainaustraliasoutheas", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "canadacentral"       -> ("dbtraincanadacentral", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=dwAT0CusWjvkzcKIukVnmFPTmi4JKlHuGh9GEx3OmXI%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "canadaeast"          -> ("dbtraincanadaeast", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SYmfKBkbjX7uNDnbSNZzxeoj%2B47PPa8rnxIuPjxbmgk%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "centralindia"        -> ("dbtraincentralindia", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=afrYm3P5%2BB4gMg%2BKeNZf9uvUQ8Apc3T%2Bi91fo/WOZ7E%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "centralus"           -> ("dbtraincentralus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=As9fvIlVMohuIV8BjlBVAKPv3C/xzMRYR1JAOB%2Bbq%2BQ%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastasia"            -> ("dbtraineastasia", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=sK7g5pki8bE88gEEsrh02VGnm9UDlm55zTfjZ5YXVMc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastus"              -> ("dbtraineastus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=tlw5PMp1DMeyyBGTgZwTbA0IJjEm83TcCAu08jCnZUo%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastus2"             -> ("dbtraineastus2", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=Y6nGRjkVj6DnX5xWfevI6%2BUtt9dH/tKPNYxk3CNCb5A%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "japaneast"           -> ("dbtrainjapaneast", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=q6r9MS/PC9KLZ3SMFVYO94%2BfM5lDbAyVsIsbBKEnW6Y%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "japanwest"           -> ("dbtrainjapanwest", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=M7ic7/jOsg/oiaXfo8301Q3pt9OyTMYLO8wZ4q8bko8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northcentralus"      -> ("dbtrainnorthcentralus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northcentralus"      -> ("dbtrainnorthcentralus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northeurope"         -> ("dbtrainnortheurope", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=35yfsQBGeddr%2BcruYlQfSasXdGqJT3KrjiirN/a3dM8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southcentralus"      -> ("dbtrainsouthcentralus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southcentralus"      -> ("dbtrainsouthcentralus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southindia"          -> ("dbtrainsouthindia", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=0X0Ha9nFBq8qkXEO0%2BXd%2B2IwPpCGZrS97U4NrYctEC4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southeastasia"       -> ("dbtrainsoutheastasia", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=H7Dxi1yqU776htlJHbXd9pdnI35NrFFsPVA50yRC9U0%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "uksouth"             -> ("dbtrainuksouth", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SPAI6IZXmm%2By/WMSiiFVxp1nJWzKjbBxNc5JHUz1d1g%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "ukwest"              -> ("dbtrainukwest", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=olF4rjQ7V41NqWRoK36jZUqzDBz3EsyC6Zgw0QWo0A8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westcentralus"       -> ("dbtrainwestcentralus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=UP0uTNZKMCG17IJgJURmL9Fttj2ujegj%2BrFN%2B0OszUE%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westeurope"          -> ("dbtrainwesteurope", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=csG7jGsNFTwCArDlsaEcU4ZUJFNLgr//VZl%2BhdSgEuU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westindia"           -> ("dbtrainwestindia", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=fI6PNZ7YvDGKjArs1Et2rAM2zgg6r/bsKEjnzQxgGfA%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westus"              -> ("dbtrainwestus", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=%2B1XZDXbZqnL8tOVsmRtWTH/vbDAKzih5ThvFSZMa3Tc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westus2"             -> ("dbtrainwestus2", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "_default"            -> ("dbtrainwestus2", "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z")
  )
}

def getAzureMapping(region:String):(String,Map[String,String]) = {
  val (account: String, sasKey: String) = MSA_REGION_MAP().getOrElse(region, MSA_REGION_MAP()("_default"))
  val blob = "training"
  val source = s"wasbs://$blob@$account.blob.core.windows.net/"
  val configMap = Map(
    s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
  )
  (source, configMap)
}

def retryMount(source: String, mountPoint: String): Unit = {
  try { 
    // Mount with IAM roles instead of keys for PVC
    dbutils.fs.mount(source, mountPoint)
    dbutils.fs.ls(mountPoint) // Test read to confirm successful mount.
  } catch {
    case e: Exception => throw new RuntimeException(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}", e)
  }
}

def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
  try {
    dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
    dbutils.fs.ls(mountPoint) // Test read to confirm successful mount.
  } catch {
    case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
    case e: Exception => throw new RuntimeException(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}", e)
  }
}

def autoMount(fix:Boolean = false, failFast:Boolean = false, mountPoint:String = "/mnt/training", cloudAndRegionOverride:Tuple2[String,String] = null, outputFormat:String = "HTML"): Unit = {
  val (cloud, region) = cloudAndRegion(cloudAndRegionOverride)
  spark.conf.set("com.databricks.training.cloud.name", cloud)
  spark.conf.set("com.databricks.training.region.name", region)
  
  if (cloud=="AWS")  {
    val (source, extraConfigs) = getAwsMapping(region)
    val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
    renderOutput(outputFormat, s"Mounting course-specific datasets to <b>$mountPoint</b>...<br/>"+resultMsg)
    
  } else if (cloud=="Azure") {
    val (source, extraConfigs) = initAzureDataSource(region)
    val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
    renderOutput(outputFormat, s"Mounting course-specific datasets to <b>$mountPoint</b>...<br/>"+resultMsg)
    
  } else {
    val (source, extraConfigs) = ("s3a://databricks-corp-training/common", Map[String,String]())
    val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
    renderOutput(outputFormat, s"Mounted course-specific datasets to <b>$mountPoint</b>.")
  }
}

// Utility method used to control output during testing
def renderOutput(outputFormat:String, html:String) = {
  if (outputFormat == "HTML") {
    displayHTML(html)
  } else {
    val text = "| " + html.replaceAll("""<b>""", "")
                          .replaceAll("""<\/b>""", "")
                          .replaceAll("""<br>""", "\n")
                          .replaceAll("""<\/br>""", "\n")
                          .replaceAll("""<br\/>""", "\n")
                          .trim()
                          .replaceAll("\n", "\n| ")
    println(text)
  }
}

def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
  val mapping = getAzureMapping(azureRegion)
  val (source, config) = mapping
  val (sasEntity, sasToken) = config.head

  val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
  spark.conf.set("com.databricks.training.azure.datasource", datasource)

  return mapping
}

def mountSource(fix:Boolean, failFast:Boolean, mountPoint:String, source:String, extraConfigs:Map[String,String]): String = {
  val mntSource = source.replace(awsAuth+"@", "")

  if (dbutils.fs.mounts().map(_.mountPoint).contains(mountPoint)) {
    val mount = dbutils.fs.mounts().filter(_.mountPoint == mountPoint).head
    if (mount.source == mntSource) {
      return s"""Datasets are already mounted to <b>$mountPoint</b>."""
      
    } else if (failFast) {
      throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
      
    } else if (fix) {
      println(s"Unmounting existing datasets ($mountPoint from ${mount.source}).")
      dbutils.fs.unmount(mountPoint)
      mountSource(fix, failFast, mountPoint, source, extraConfigs)
      
    } else {
      return s"""<b style="color:red">Invalid Mounts!</b></br>
                      <ul>
                      <li>The training datasets you are using are from an unexpected source</li>
                      <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
                      <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
                      <ol>
                        <li>Insert a new cell after this one</li>
                        <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
                        <li>Verify that the problem has been resolved.</li>
                      </ol>"""
    }
  } else {
    println(s"""Mounting course-specific datasets to $mountPoint...""")
    mount(source, extraConfigs, mountPoint)
    return s"""Mounted datasets to <b>$mountPoint</b> from <b>$mntSource<b>."""
  }
}

def fixMounts(): Unit = {
  autoMount(true)
}

autoMount(true)

# COMMAND ----------

def getAzureDataSource():
  datasource = spark.conf.get("com.databricks.training.azure.datasource").split("\t")
  source = datasource[0]
  sasEntity = datasource[1]
  sasToken = datasource[2]
  return (source, sasEntity, sasToken)


None # Suppress output
