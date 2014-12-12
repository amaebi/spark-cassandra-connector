package com.datastax.spark.connector

import com.datastax.spark.connector.cql.{CassandraConnectionHint, CassandraConnector}
import com.datastax.spark.connector.writer._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  override val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columns: ColumnSelector = AllColumns,
                      writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                     (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf, CassandraConnectionHint.forWriting),
                      rwf: RowWriterFactory[T]): Unit = {
    val writer = TableWriter(connector, keyspaceName, tableName, columns, writeConf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }
}
