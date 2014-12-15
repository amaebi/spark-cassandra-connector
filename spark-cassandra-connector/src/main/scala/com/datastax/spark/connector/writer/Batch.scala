package com.datastax.spark.connector.writer

import java.nio.ByteBuffer

import com.datastax.driver.core.BoundStatement
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}

import scala.collection.mutable.ArrayBuffer


sealed trait Batch extends Ordered[Batch] {
  def add(stmt: BoundStatement): Unit

  def isOversized: Boolean

  def statements: Seq[BoundStatement]
}

object Batch {
  implicit val batchOrdering = new Ordering[Batch] {
    override def compare(x: Batch, y: Batch): Int = x.compare(y)
  }

  def apply(batchSize: BatchSize): Batch = {
    batchSize match {
      case RowsInBatch(rows) => new RowLimitedBatch(rows)
      case BytesInBatch(bytes) => new SizeLimitedBatch(bytes)
    }
  }
}

class RowLimitedBatch(val maxRows: Int) extends Batch {
  val buf = new ArrayBuffer[BoundStatement](maxRows)

  def add(stmt: BoundStatement): Unit = {
    buf += stmt
  }

  override def isOversized: Boolean = buf.size > maxRows

  override def compare(that: Batch): Int = that match {
    case thatBatch: RowLimitedBatch => buf.size.compare(thatBatch.maxRows)
    case _ => throw new ClassCastException("Not a RowLimitedBatch")
  }

  override def statements: Seq[BoundStatement] = buf
}

class SizeLimitedBatch(val maxBytes: Int) extends Batch {
  val buf = new ArrayBuffer[BoundStatement](10)
  var size = 0

  def add(stmt: BoundStatement): Unit = {
    buf += stmt
    size += AbstractBatchBuilder.calculateDataSize(stmt)
  }

  override def isOversized: Boolean = size > maxBytes

  override def compare(that: Batch): Int = that match {
    case thatBatch: SizeLimitedBatch => size.compare(thatBatch.size)
    case _ => throw new ClassCastException("Not a SizeLimitedBatch")
  }

  override def statements: Seq[BoundStatement] = buf
}

class BatchKey private(val bb: ByteBuffer) {
  override val hashCode: Int = bb.hashCode()

  def canEqual(other: Any): Boolean = other.isInstanceOf[BatchKey]

  override def equals(other: Any): Boolean = other match {
    case that: BatchKey => this.hashCode == that.hashCode && bb == that.bb
    case _ => false
  }
}

object BatchKey {
  def apply(bb: ByteBuffer) = new BatchKey(bb.duplicate())
}
