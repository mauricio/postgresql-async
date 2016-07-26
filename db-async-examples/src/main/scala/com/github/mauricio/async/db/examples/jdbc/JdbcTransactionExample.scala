package com.github.mauricio.async.db.examples.jdbc

import java.util.concurrent.Executors

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.jdbc.{JdbcConnection, JdbcConnectionConfiguration}
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object JdbcTransactionExample extends App {

  case class Person(id: Int, name: String, createdAt: DateTime)

  val dbExecutor = Executors.newFixedThreadPool(10)
  val dbExecutionContext = ExecutionContext.fromExecutor(dbExecutor)

  val config = JdbcConnectionConfiguration(
    jdbcUrl = "jdbc:h2:mem:example",
    driverProps = Map(
      "DATABASE_TO_UPPER" -> "FALSE",
      "TRACE_LEVEL_SYSTEM_OUT" -> "2"
    )
  )
  val conn: Connection = JdbcConnection.forConfig(config, dbExecutionContext,
    stmtOptions = _ => (),
    connOptions = _.setAutoCommit(false)
  )

  implicit val defaultExecutionContext = ExecutionContext.global
  val personsFuture = for {
    _ <- conn.connect
    _ <- conn.sendQuery("create table persons (id int, name varchar(100), created_at timestamp)")
    _ <- conn.inTransaction { _ =>
      for {
        _ <- conn.sendQuery("insert into persons(id, name, created_at) values (1, 'bob', current_timestamp)")
        _ <- conn.sendPreparedStatement("insert into persons (id, name, created_at) values (?, ?, ?)", Seq(2, "alice", DateTime.now()))
      } yield ()
    }
    queryResult <- conn.sendQuery("select id, name, created_at from persons")
    _ <- conn.disconnect
  } yield {
    queryResult.rows.map(rs => {
      rs.map(row => {
        Person(
          row("id").asInstanceOf[Int],
          row("name").asInstanceOf[String],
          row("created_at").asInstanceOf[DateTime]
        )
      })
    }).getOrElse(Seq.empty)
  }

  println("Waiting for operations to complete")
  val persons = Await.result(personsFuture, Duration.Inf)

  println(s"Fetched persons = $persons")
  dbExecutor.shutdown()
}
