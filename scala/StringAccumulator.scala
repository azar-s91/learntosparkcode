import org.apache.spark.util.AccumulatorV2

class StringAccumulator(private var _value: String) extends AccumulatorV2[String, String] {

  def this() {
    this("")
  }

  override def add(newValue: String): Unit = {
    _value = value + " " + newValue.trim
  }

  override def copy(): StringAccumulator = {
    new StringAccumulator(value)
  }

  override def isZero(): Boolean = {
    value.length() == 0
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    add(other.value)
  }

  override def reset(): Unit = {
    _value = ""
  }

  override def value(): String = {
    _value
  }
}