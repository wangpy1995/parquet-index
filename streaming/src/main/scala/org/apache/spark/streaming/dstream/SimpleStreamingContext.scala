package org.apache.spark.streaming.dstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

class SimpleStreamingContext(@transient ss: SparkSession, @transient duration: Duration) extends StreamingContext(ss.sparkContext, duration) {
  def sql(sqlText: String) = ss.sql(sqlText)
}
