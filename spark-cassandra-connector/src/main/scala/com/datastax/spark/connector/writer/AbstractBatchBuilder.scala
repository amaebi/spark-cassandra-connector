package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.util.Logging

import scala.collection.Iterator
import scala.collection.JavaConversions._

trait AbstractBatchBuilder[T] extends Iterator[Statement] with Logging {

  val batchType: BatchStatement.Type
  val rowWriter: RowWriter[T]
  val preparedStmt: PreparedStatement
  val protocolVersion: ProtocolVersion
  val routingKeyGenerator: RoutingKeyGenerator

  protected[this] def makeBatch(batch: Batch): Statement = {
    maybeCreateBatch(batch.statements)
  }

  protected[this] def bind(row: T): BoundStatement = {
    val bs = rowWriter.bind(row, preparedStmt, protocolVersion)
    bs
  }

  /** Converts a sequence of statements into a batch if its size is greater than 1. */
  protected[this] def maybeCreateBatch(stmts: Seq[BoundStatement]): Statement = {
    require(stmts.size > 0, "Statements list cannot be empty")
    val stmt = stmts.head
    // for batch statements, it is enough to set routing key for the first statement
    stmt.setRoutingKey(routingKeyGenerator.computeRoutingKey(stmt))

    if (stmts.size == 1) {
      stmt
    } else {
      new BatchStatement(batchType).addAll(stmts)
    }
  }

  /** Just make bound statements from data items. */
  protected[this] def boundStatements(data: Iterator[T]): Stream[Statement] =
    data.map(bind).toStream

}

object AbstractBatchBuilder {
  /** Calculate bound statement size in bytes. */
  def calculateDataSize(stmt: BoundStatement): Int = {
    var size = 0
    for (i <- 0 until stmt.preparedStatement().getVariables.size())
      if (!stmt.isNull(i)) size += stmt.getBytesUnsafe(i).remaining()

    size
  }
}
