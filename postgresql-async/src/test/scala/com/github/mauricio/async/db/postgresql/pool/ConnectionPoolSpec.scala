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

package com.github.mauricio.async.db.postgresql.pool

import java.util.UUID

import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.github.mauricio.async.db.postgresql.{PostgreSQLConnection, DatabaseTestHelper}
import org.specs2.execute.{Result, Success, AsResult}
import org.specs2.mutable.Specification
import org.specs2.specification.Fixture

object ConnectionPoolSpec {
  val Insert = "insert into transaction_test (id) values (?)"
}

class ConnectionPoolSpec extends Specification with DatabaseTestHelper {

  import ConnectionPoolSpec.Insert

  "pool" should {

    "give you a connection when sending statements" in {

      withPool{
        pool =>
          executeQuery(pool, "SELECT 8").rows.get(0)(0) === 8
          Thread.sleep(1000)
          pool.availables.size === 1
      }

    }

    "give you a connection for prepared statements" in {
      withPool{
        pool =>
          executePreparedStatement(pool, "SELECT 8").rows.get(0)(0) === 8
          Thread.sleep(1000)
          pool.availables.size === 1
      }
    }

    "return an empty map when connect is called" in {
      withPool {
        pool =>
          await(pool.connect) === pool
      }
    }

    "runs commands for a transaction in a single connection" ! attempts {_ =>

      val id = UUID.randomUUID().toString

      withPool {
        pool =>
          val operations = pool.inTransaction {
            connection =>
              connection.sendPreparedStatement(Insert, List(id)).flatMap {
                result =>
                  connection.sendPreparedStatement(Insert, List(id)).map {
                    failure =>
                      List(result, failure)
                  }
              }
          }

          await(operations) must throwA[GenericDatabaseException]

      }

    }

  }
  val attemptsCount = 20
  val attempts = new Fixture[Int] {
    def apply[R : AsResult](f: Int => R) = {
      (0 to attemptsCount).foldLeft(Success(): Result) { (res, i) =>
        res and AsResult(f(i))
      }
    }
  }
  def withPool[R]( fn : (ConnectionPool[PostgreSQLConnection]) => R ) : R = {

    val pool = new ConnectionPool( new PostgreSQLConnectionFactory(defaultConfiguration), PoolConfiguration.Default )
    try {
      fn(pool)
    } finally {
      await(pool.disconnect)
    }

  }

}
