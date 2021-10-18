package Job2

import org.apache.hadoop.io.{IntWritable, RawComparator, WritableComparator}

/**
 * Custom class for sorting mapper keys of type IntWritable in descending order
 */
class CustomComparator extends WritableComparator(classOf[IntWritable]){
  override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
    val thisValue = WritableComparator.readInt(b1, s1)
    val thatValue = WritableComparator.readInt(b2, s2)
    if (thisValue < thatValue)
      1
    else if (thisValue == thatValue)
      0
    else
      -1
  }
}
