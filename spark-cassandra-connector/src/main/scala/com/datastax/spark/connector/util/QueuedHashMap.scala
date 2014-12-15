package com.datastax.spark.connector.util

import org.apache.spark.util.MutablePair

import scala.collection.mutable
import scala.reflect.ClassTag

private[connector] class QueuedHashMap[K, V: Ordering : ClassTag](capacity: Int) {
  type Box = MutablePair[Int, (K, V)]

  implicit val ordering = new Ordering[(K, V)] {
    override def compare(x: (K, V), y: (K, V)): Int = implicitly[Ordering[V]].compare(x._2, y._2)
  }

  private[this] val data = new PriorityQueueWithUplifting[(K, V)](capacity + 1)
  private[this] val keyMap = mutable.HashMap[K, Box]().withDefaultValue(null)

  private[this] var dirty = true

  private[this] var lastKey: K = _
  private[this] var lastIdx: Int = _
  private[this] var lastValue: Option[V] = _

  def apply(key: K): Option[V] = {
    keyMap(key) match {
      case null =>
        None
      case box: Box =>
        Some(box._2._2)
    }
  }

  def update(key: K): Unit = {
    data.update(keyMap(key))
  }

  def add(key: K, value: V): Unit = {
    if (keyMap.contains(key))
      throw new IllegalStateException(s"Key $key already exists")

    val box = data.add((key, value))
    keyMap.put(key, box)
  }

  def remove(): V = {
    val box = data.remove()
    keyMap.remove(box._2._1)
    box._2._2
  }

  def size(): Int = {
    data.size
  }

  def head(): V = {
    data.apply(0)._2._2
  }

}


