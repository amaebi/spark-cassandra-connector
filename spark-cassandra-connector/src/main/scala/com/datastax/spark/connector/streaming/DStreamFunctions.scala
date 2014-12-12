package com.datastax.spark.connector.streaming

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnectionHint, CassandraConnector}
import com.datastax.spark.connector.writer.{TableWriter, WriteConf, RowWriterFactory, WritableToCassandra}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

class DStreamFunctions[T](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  override def sparkContext: SparkContext = dstream.context.sparkContext

  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: ColumnSelector = AllColumns,
                      writeConf: WriteConf = WriteConf.fromSparkConf(conf))
                     (implicit connector: CassandraConnector = CassandraConnector(conf, CassandraConnectionHint.forWriting),
                      rwf: RowWriterFactory[T]): Unit = {
    val writer = TableWriter(connector, keyspaceName, tableName, columnNames, writeConf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }
}
