// Databricks notebook source
displayHTML("""
<center><img src='files/learnspark_img/learn.JPG' width='250' height='250'>  <img src='files/learnspark_img/spark.png' width='270' height='250'></center>
<H1><center><B><font color="Blue">www.learntospark.com</font></B></center></H1>
<H1><center>Apache Spark - Scenario Based Question<center></H1>
<H2><center><U>How to Add Leading Zeros to a column in Spark DataFrame ? </U></center></H2>
<H2><center><font color='green'>Spark With Scala</font></center></H2>
<center><img src='files/learnspark_img/inp.JPG'> <img src='files/learnspark_img/out.JPG'></center>
"""
)

// COMMAND ----------

// DBTITLE 1,Create Input DF from List
var list_data=Seq(("Babu",20),("Raja",8),("Mani",75),("Kalam",100),("Zoin",7),("Kal",53))
val df1=list_data.toDF("name", "Score")
df1.show()

// COMMAND ----------

// DBTITLE 1,Method 1 - Lpad
import org.apache.spark.sql.functions.{lpad,col}
val df2=df1.withColumn("score_000",lpad(col("Score"),3,"0"))

display(df2)

// COMMAND ----------

// DBTITLE 1,Method  2 - Format_string
import org.apache.spark.sql.functions.{format_string,col}
val df2=df1.withColumn("score_000",format_string("%03d",col("score")))

display(df2)

display(df1.withColumn("score_000",format_string("%s#%03d",col("name"),col("score"))))

// COMMAND ----------

// DBTITLE 1,Method 3 - Concat and Substring
import org.apache.spark.sql.functions.{concat,substring,lit,col}

val df2=df1.withColumn("score_000",concat(lit("00"),col("score")))
val df3=df2.withColumn("score_000",substring(col("score_000"),-3,3))
display(df3)

display(df1.withColumn("score_000",substring(concat(lit("00"),col("score")),-3,3)))

// COMMAND ----------


