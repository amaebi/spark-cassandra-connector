package com.datastax.spark.connector.writer

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core.{BatchStatement, BoundStatement, Session}
import com.datastax.spark.connector.{BytesInBatch, RowsInBatch}
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import scala.collection.JavaConversions._

class BatchMakerSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedEmbeddedCassandra {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(Set(cassandraHost))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS batch_maker_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS batch_maker_test.tab (id INT PRIMARY KEY, value TEXT)")
  }

  val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)
  val schema = Schema.fromCassandra(conn, Some("batch_maker_test"), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)].rowWriter(schema.tables.head, Seq("id", "value"))

  def makeBatchMaker(session: Session): BatchMaker[(Int, String)] = {
    val stmt = session.prepare("INSERT INTO batch_maker_test.tab (id, value) VALUES (:id, :value)")
    new BatchMaker[(Int, String)](Type.UNLOGGED, rowWriter, stmt, protocolVersion)
  }

  "BatchMaker" should "make bound statements when batch size is specified as RowsInBatch(1)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm.makeStatements(data.toIterator, RowsInBatch(1)).toList
      statements.foreach(_ shouldBe a[BoundStatement])
      statements should have size 3
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements when batch size is specified as BytesInBatch(0)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm.makeStatements(data.toIterator, BytesInBatch(0)).toList
      statements.foreach(_ shouldBe a[BoundStatement])
      statements should have size 3
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make a batch and a bound statements according to the number of statements in a group" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm.makeStatements(data.toIterator, RowsInBatch(2)).toList
      statements should have size 2
      statements(0) shouldBe a [BatchStatement]
      statements(1) shouldBe a [BoundStatement]
      statements.flatMap {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make equal batches when batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      val statements = bm.makeStatements(data.toIterator, RowsInBatch(2)).toList
      statements should have size 2
      statements foreach { _ shouldBe a [BatchStatement] }
      statements.flatMap {
        case s: BatchStatement =>
          s.size() should be (2)
          s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make batches of size not greater than the size specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq(
        (1, "a"),     // 5 bytes
        (2, "aa"),    // 6 bytes
        (3, "aaa"),   // 7 bytes
        (4, "aaaa"),  // 8 bytes
        (5, "aaaaa"), // 9 bytes
        (6, "aaaaaa"),// 10 bytes
        (7, "aaaaaaaaaaaa") // 16 bytes
      )
      val statements = bm.makeStatements(data.toIterator, BytesInBatch(15)).toList
      statements should have size 5
      statements.take(2) foreach { _ shouldBe a [BatchStatement] }
      statements.drop(2).take(3) foreach { _ shouldBe a [BoundStatement] }

      val stmtss = statements.map {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.getStatements.map(_.asInstanceOf[BoundStatement]).toList
      }

      stmtss.foreach(stmts => stmts.size should be > 0 )
      stmtss.foreach(stmts => if (stmts.size > 1) stmts.map(BatchMaker.calculateDataSize).sum should be <= 15 )
      stmtss.flatten.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsInOrderAs data
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq()
      val statements = bm.makeStatements(data.toIterator, RowsInBatch(10)).toList
      statements should have size 0
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchMaker(session)
      val data = Seq()
      val statements = bm.makeStatements(data.toIterator, BytesInBatch(10)).toList
      statements should have size 0
    }
  }


}
