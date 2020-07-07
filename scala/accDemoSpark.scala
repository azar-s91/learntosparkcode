import org.apache.spark.sql.{Row, SparkSession}

object accDemoSpark {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("accumulator").getOrCreate()

    val counter = spark.sparkContext.longAccumulator

    val countervalue=new StringAccumulator()

    spark.sparkContext.register(countervalue)

    def validate(row: Row)={
      val age=row.getLong(row.fieldIndex("Age"))
      if(age < 18){
        counter.add(1)
        countervalue.add(age.toString)
      }

    }

    val df=spark.read.json("C:\\Users\\as5272\\OneDrive - Lennox International, Inc\\Documents\\blogspot\\df_add\\input.json")

    df.show()

    df.foreach(validate _)

    println(counter.value)
    println(countervalue.value)



  }


}
