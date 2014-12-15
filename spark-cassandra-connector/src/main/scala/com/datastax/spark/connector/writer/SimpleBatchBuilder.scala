package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}

import scala.collection.Iterator

/** This class is responsible for converting a stream of data items into stream of statements which are
  * ready to be executed with Java Driver. Depending on provided options the statements are grouped into
  * batches or not. */
class SimpleBatchBuilder[T](val batchType: BatchStatement.Type,
                            val rowWriter: RowWriter[T],
                            val preparedStmt: PreparedStatement,
                            val protocolVersion: ProtocolVersion,
                            val routingKeyGenerator: RoutingKeyGenerator,
                            batchSize: BatchSize,
                            data: Iterator[T])
  extends AbstractBatchBuilder[T] {

  import com.datastax.spark.connector.writer.AbstractBatchBuilder._

  /** Splits data items into groups of equal number of elements and make batches from these groups. */
  private def rowsLimitedBatches(data: Iterator[T], batchSizeInRows: Int): Stream[Statement] = {
    val batches = for (batch <- data.grouped(batchSizeInRows)) yield {
      val boundStmts = batch.map(bind)
      maybeCreateBatch(boundStmts)
    }

    batches.toStream
  }

  /** Splits data items into groups of size not greater than the provided limit in bytes and make batches
    * from these groups. */
  private def sizeLimitedBatches(data: Iterator[T], batchSizeInBytes: Int): Stream[Statement] = {
    val boundStmts = data.toStream.map(bind)

    def batchesStream(stmtsStream: Stream[BoundStatement]): Stream[Statement] = stmtsStream match {
      case Stream.Empty => Stream.Empty
      case head #:: rest =>
        val stmtSizes = rest.map(calculateDataSize)
        val cumulativeStmtSizes = stmtSizes.scanLeft(calculateDataSize(head).toLong)(_ + _).tail
        val addStmts = (rest zip cumulativeStmtSizes)
          .takeWhile { case (_, size) => size <= batchSizeInBytes}
          .map { case (boundStmt, _) => boundStmt}
        val stmtsGroup = (head #:: addStmts).toVector
        val batch = maybeCreateBatch(stmtsGroup)
        val remaining = stmtsStream.drop(stmtsGroup.size)
        batch #:: batchesStream(remaining)
    }

    batchesStream(boundStmts)
  }

  /** Depending on provided batch size, convert the data items into stream of bound statements, batches
    * of equal rows number or batches of equal size in bytes. */
  private def makeStatements(data: Iterator[T], batchSize: BatchSize): Stream[Statement] = {
    batchSize match {
      case RowsInBatch(1) | BytesInBatch(0) =>
        logInfo(s"Creating bound statements - batch size is $batchSize")
        boundStatements(data)
      case RowsInBatch(n) =>
        logInfo(s"Creating batches of $n rows")
        rowsLimitedBatches(data, n)
      case BytesInBatch(n) =>
        logInfo(s"Creating batches of $n bytes")
        sizeLimitedBatches(data, n)
    }
  }

  private[this] val iterator = makeStatements(data, batchSize).iterator

  override def hasNext: Boolean = iterator.hasNext

  override def next(): Statement = iterator.next()
}
