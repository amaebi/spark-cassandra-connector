package com.datastax.spark.connector.util

import org.apache.spark.util.MutablePair

import scala.annotation.tailrec
import scala.reflect.ClassTag

class PriorityQueueWithUplifting[V: Ordering : ClassTag](val capacity: Int) {
  /** A mutable pair which contains a real index and a value */
  type Box = MutablePair[Int, V]

  private[this] val buf = new Array[Box](capacity)
  private[this] var startPos = 0
  private[this] var curSize = 0
  private[this] val ordering = implicitly[Ordering[V]]

  def size: Int = curSize

  def apply(idx: Int): Box = {
    if (idx < 0 || idx >= curSize)
      throw new IndexOutOfBoundsException()

    buf(normalize(idx + startPos))
  }

  def update(idx: Int, value: V): Box = {
    if (idx < 0 || idx >= curSize)
      throw new IndexOutOfBoundsException()

    val box = setUnchecked(normalize(idx + startPos), value)
    upliftIfNeeded(box._1)
  }

  def update(box: Box): Box = {
    val b = setUnchecked(normalize(box._1), box._2)
    upliftIfNeeded(b._1)
  }

  def remove(): Box = {
    if (curSize > 0) {
      val result = buf(startPos)
      startPos = normalize(startPos + 1)
      curSize -= 1
      result
    } else {
      throw new NoSuchElementException("The queue is empty")
    }
  }

  def add(value: V): Box = {
    if (curSize >= capacity)
      throw new IllegalStateException("The queue is full")

    curSize += 1
    update(curSize - 1, value)
  }

  @inline
  private[this] def normalize(x: Int): Int =
    if (x < 0)
      capacity + (x % capacity)
    else
      x % capacity

  @tailrec
  private[this] def upliftIfNeeded(realIdx: Int): Box = {
    normalize(realIdx) match {
      case pos if pos == startPos => buf(startPos)
      case pos =>
        val prev = normalize(pos - 1)
        if (ordering.compare(buf(pos)._2, buf(prev)._2) > 0) {
          swapUnchecked(pos, prev)
          upliftIfNeeded(prev)
        } else {
          buf(pos)
        }
    }
  }

  private[this] def swapUnchecked(realIdx1: Int, realIdx2: Int): Unit = {
    val box1 = buf(realIdx1)
    buf(realIdx1) = buf(realIdx2)
    buf(realIdx2) = box1
    buf(realIdx1)._1 = realIdx1
    buf(realIdx2)._1 = realIdx2
  }


  private[this] def setUnchecked(realIdx: Int, value: V): Box = {
    if (buf(realIdx) == null) {
      buf(realIdx) = new Box(realIdx, value)
      buf(realIdx)
    } else
      buf(realIdx).update(realIdx, value)
  }

}
