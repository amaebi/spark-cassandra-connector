package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.BatchSize
import com.datastax.spark.connector.util.QueuedHashMap
import com.google.common.collect.AbstractIterator

import scala.annotation.tailrec
import scala.collection.Iterator

class MultiBatchBuilder[T](val batchType: BatchStatement.Type,
                           val rowWriter: RowWriter[T],
                           val preparedStmt: PreparedStatement,
                           val protocolVersion: ProtocolVersion,
                           val routingKeyGenerator: RoutingKeyGenerator,
                           batchKeyGenerator: BoundStatement => BatchKey,
                           batchSize: BatchSize,
                           maxBatches: Int,
                           data: Iterator[T])
  extends AbstractIterator[Statement] with AbstractBatchBuilder[T] {

  private[this] val qmap = new QueuedHashMap[BatchKey, Batch](maxBatches)

  def processStatement(boundStatement: BoundStatement): Option[Batch] = {
    val batchKey = batchKeyGenerator(boundStatement)
    qmap.apply(batchKey) match {
      case Some(batch) =>
        batch.add(boundStatement)
        qmap.update(batchKey)
      case None =>
        val batch = Batch(batchSize)
        batch.add(boundStatement)
        qmap.add(batchKey, batch)
    }

    if (qmap.size > maxBatches || (qmap.size > 0 && qmap.head().isOversized)) {
      Some(qmap.remove())
    } else {
      None
    }
  }

  @tailrec
  final override def computeNext(): Statement = {
    if (data.hasNext) {
      processStatement(bind(data.next())) match {
        case Some(b) => makeBatch(b)
        case None => computeNext()
      }
    } else if (qmap.size() > 0) {
      makeBatch(qmap.remove())
    } else {
      endOfData()
    }
  }

}
