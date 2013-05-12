/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mauricio.postgresql

import com.github.mauricio.async.db.column.{TimestampEncoderDecoder, TimeEncoderDecoder, DateEncoderDecoder}
import com.github.mauricio.async.db.exceptions.UnsupportedAuthenticationMethodException
import com.github.mauricio.async.db.postgresql.exceptions.{InsufficientParametersException, QueryMustNotBeNullOrEmptyException, GenericDatabaseException}
import com.github.mauricio.async.db.postgresql.messages.backend.InformationMessage
import com.github.mauricio.async.db.postgresql.{PostgreSQLConnection, DatabaseTestHelper}
import com.github.mauricio.async.db.{Configuration, QueryResult, Connection}
import concurrent.{Future, Await}
import org.specs2.mutable.Specification
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PostgreSQLConnectionSpec extends Specification with DatabaseTestHelper {

  val create = """create temp table type_test_table (
            bigserial_column bigserial not null,
            smallint_column smallint not null,
            integer_column integer not null,
            decimal_column decimal(10,4),
            real_column real,
            double_column double precision,
            serial_column serial not null,
            varchar_column varchar(255),
            text_column text,
            timestamp_column timestamp,
            date_column date,
            time_column time,
            boolean_column boolean,
            constraint bigserial_column_pkey primary key (bigserial_column)
          )"""

  val insert = """insert into type_test_table (
            smallint_column,
            integer_column,
            decimal_column,
            real_column,
            double_column,
            varchar_column,
            text_column,
            timestamp_column,
            date_column,
            time_column,
            boolean_column
            )
            VALUES (
            10,
            11,
            14.999,
            78.34,
            15.68,
            'this is a varchar field',
            'this is a long text field',
            '1984-08-06 22:13:45.888888',
            '1984-08-06',
            '22:13:45.888888',
            TRUE
            )
               """

  val select = "select * from type_test_table"

  val preparedStatementCreate = """create temp table prepared_statement_test (
    id bigserial not null,
    name varchar(255) not null,
    constraint bigserial_column_pkey primary key (id)
  )"""

  val preparedStatementInsert = " insert into prepared_statement_test (name) values ('John Doe')"
  val preparedStatementInsert2 = " insert into prepared_statement_test (name) values ('Mary Jane')"
  val preparedStatementInsert3 = " insert into prepared_statement_test (name) values ('Peter Parker')"
  val preparedStatementInsertReturning = " insert into prepared_statement_test (name) values ('John Doe') returning id"
  val preparedStatementSelect = "select * from prepared_statement_test"

  "handler" should {

    "connect to the database" in {

      withHandler {
        handler =>
          handler.isReadyForQuery must beTrue
      }

    }

    "create a table in the database" in {

      withHandler {
        handler =>
          executeDdl(handler, this.create)
      }

    }

    "insert a row in the database" in {

      withHandler {
        handler =>
          executeDdl(handler, this.create)
          executeDdl(handler, this.insert, 1)

      }

    }

    "select rows in the database" in {

      withHandler {
        handler =>
          executeDdl(handler, this.create)
          executeDdl(handler, this.insert, 1)
          val result = executeQuery(handler, this.select)

          val row = result.rows.get(0)

          row(0) === 1
          row(1) === 10
          row(2) === 11
          row(3).toString === "14.9990"
          row(4).toString === 78.34.toString
          row(5) === 15.68
          row(6) === 1
          row(7) === "this is a varchar field"
          row(8) === "this is a long text field"
          row(9) === TimestampEncoderDecoder.Instance.decode("1984-08-06 22:13:45.888888")
          row(10) === DateEncoderDecoder.decode("1984-08-06")
          row(11) === TimeEncoderDecoder.Instance.decode("22:13:45.888888")
          row(12) === true


      }

    }
    
    "select rows that has duplicate column names" in {
        
      withHandler {
        handler =>
          val result = executeQuery(handler, "SELECT 1 COL, 2 COL")

          val row = result.rows.get(0)

          row(0) === 1
          row(1) === 2

      }
        
    }

    "execute a prepared statement" in {

      withHandler {
        handler =>
          executeDdl(handler, this.preparedStatementCreate)
          executeDdl(handler, this.preparedStatementInsert, 1)
          val result = executePreparedStatement(handler, this.preparedStatementSelect)

          val row = result.rows.get(0)


          row(0) === 1
          row(1) === "John Doe"


      }

    }

    "execute a prepared statement with parameters" in {

      withHandler {
        handler =>
          executeDdl(handler, this.preparedStatementCreate)
          executeDdl(handler, this.preparedStatementInsert, 1)
          executeDdl(handler, this.preparedStatementInsert2, 1)
          executeDdl(handler, this.preparedStatementInsert3, 1)

          val select = "select * from prepared_statement_test where name like ?"

          val queryResult = executePreparedStatement(handler, select, Array("Peter Parker"))
          val row = queryResult.rows.get(0)

          val queryResult2 = executePreparedStatement(handler, select, Array("Mary Jane"))
          val row2 = queryResult2.rows.get(0)

          row(0) === 3
          row(1) === "Peter Parker"

          row2(0) === 2
          row2(1) === "Mary Jane"

      }

    }

    "login using MD5 authentication" in {

      val configuration = new Configuration(
        username = "postgres_md5",
        password = Some("postgres_md5"),
        port = databasePort,
        database = databaseName
      )

      withHandler(configuration, {
        handler =>
          val result = executeQuery(handler, "SELECT 0")
          result.rows.get.apply(0)(0) === 0
      })

    }

    "login using cleartext authentication" in {

      val configuration = new Configuration(
        username = "postgres_cleartext",
        password = Some("postgres_cleartext"),
        port = databasePort,
        database = databaseName
      )

      withHandler(configuration, {
        handler =>
          val result = executeQuery(handler, "SELECT 0")
          result.rows.get(0)(0) === 0
      })

    }

    "fail login using kerberos authentication" in {

      val configuration = new Configuration(
        username = "postgres_kerberos",
        password = Some("postgres_kerberos"),
        port = databasePort,
        database = databaseName
      )

      withHandler(configuration, {
        handler =>
          executeQuery(handler, "SELECT 0")
      }) must throwAn[UnsupportedAuthenticationMethodException]

    }

    "fail login using with an invalid credential exception" in {

      val configuration = new Configuration(
        username = "postgres_md5",
        password = Some("postgres_kerberos"),
        port = databasePort,
        database = databaseName
      )
      try {
        withHandler(configuration, {
          handler =>
            executeQuery(handler, "SELECT 0")
            failure("should not have come here")
        })
      } catch {
        case e: GenericDatabaseException => {
          e.errorMessage.fields(InformationMessage.Routine) === "auth_failed"
        }
        case e: Exception => {
          failure("should not have come here")
        }
      }

    }

    "transaction and flatmap example" in {

      val handler: Connection = new PostgreSQLConnection(defaultConfiguration)
      val result: Future[QueryResult] = handler.connect
        .map(parameters => handler)
        .flatMap(connection => connection.sendQuery("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        .flatMap(query => handler.sendQuery("SELECT 0"))
        .flatMap(query => handler.sendQuery("COMMIT").map(value => query))

      val queryResult: QueryResult = Await.result(result, Duration(5, SECONDS))

      queryResult.rows.get(0)(0) === 0

    }

    "use RETURNING in an insert statement" in {

      withHandler {
        connection =>
          executeDdl(connection, this.preparedStatementCreate)
          val result = executeQuery(connection, this.preparedStatementInsertReturning)
          result.rows.get(0)("id") === 1
      }

    }

    "execute a prepared statement with limit" in {

      withHandler {
        handler =>
          executeDdl(handler, this.preparedStatementCreate)
          executeDdl(handler, this.preparedStatementInsert, 1)
          executeDdl(handler, this.preparedStatementInsert2, 1)
          executeDdl(handler, this.preparedStatementInsert3, 1)

          val result = executePreparedStatement(handler, "select * from prepared_statement_test LIMIT 1").rows.get(0)

          result("name") === "John Doe"
      }

    }

    "execute an empty query" in {

      withHandler {
        handler =>
          executeQuery(handler, "").rows === None
      } must throwA[QueryMustNotBeNullOrEmptyException]

    }

    "execute an whitespace query" in {

      withHandler {
        handler =>
          executeQuery(handler, "   ").rows === None
      } must throwA[QueryMustNotBeNullOrEmptyException]

    }
    
    "execute multiple prepared statements" in {
      withHandler {
        handler =>
          executeDdl(handler, this.preparedStatementCreate)
          for (i <- 0 until 500)
            executePreparedStatement(handler, this.preparedStatementInsert)
          ok
        }
      }
    
  }

}
