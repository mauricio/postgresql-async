package com.github.mauricio.async.db.examples.jdbc

import java.util.concurrent.Executors

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.jdbc.JdbcConnection

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object JdbcDataSourceExample extends App {

  case class Person(id: Int, name: String)

  val ds = new org.h2.jdbcx.JdbcDataSource()
  ds.setURL("jdbc:h2:mem:example;DATABASE_TO_UPPER=FALSE")

  val dbExecutor = Executors.newFixedThreadPool(10)
  val dbExecutionContext = ExecutionContext.fromExecutor(dbExecutor)
  val conn: Connection = JdbcConnection.forDataSource(ds, dbExecutionContext)

  implicit val defaultExecutionContext = ExecutionContext.global
  val personsFuture = for {
    _ <- conn.connect
    _ <- conn.sendQuery("create table persons (id int, name varchar(100))")
    _ <- conn.sendQuery("insert into persons(id, name) values (1, 'bob')")
    _ <- conn.sendPreparedStatement("insert into persons (id, name) values (?, ?)", Seq(2, "alice"))
    queryResult <- conn.sendQuery("select id, name from persons")
    _ <- conn.disconnect
  } yield {
    queryResult.rows.map(rs => {
      rs.map(row => {
        Person(
          row("id").asInstanceOf[Int],
          row("name").asInstanceOf[String]
        )
      })
    }).getOrElse(Seq.empty)
  }

  println("Waiting for operations to complete")
  val persons = Await.result(personsFuture, Duration.Inf)

  println(s"Fetched persons = $persons")
  dbExecutor.shutdown()
}
