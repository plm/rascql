/*
 * Copyright 2014 Philip L. McMahon
 *
 * Philip L. McMahon licenses this file to you under the Apache License,
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

package rascql.postgresql

import java.nio.charset.Charset
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import rascql.postgresql.protocol._
import rascql.postgresql.stream._

/**
 * A simple demonstration of PostgreSQL query execution.
 *
 * @author Philip L. McMahon
 */
object Demo extends App with DefaultParameterEncoders with DefaultColumnDecoders {

  implicit val system = ActorSystem("Example")
  val settings = ActorMaterializerSettings(
    system.settings.config.getConfig("rascql.stream.materializer"))
  implicit val materializer = ActorMaterializer(settings, "Rascql")

  var charset = Charset.forName("UTF-8")

  val Array(username, password) = args

  import system.dispatcher
  import GraphDSL.Implicits._

  val queries = Source.fromIterator(() => Iterator.from(1).flatMap { i => List(
    SendQuery(
      """BEGIN;
        |SELECT usename FROM pg_stat_activity;
        |COMMIT""".stripMargin
    ),
    SendQuery.Prepared(
      "SELECT usename AS usename" + i + " FROM pg_stat_activity WHERE usename = $1 LIMIT $2",
      username,
      1
    )
  )})

  val startup = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    val concat = b.add(Concat[FrontendMessage]())
    val rollover = b.add(Rollover[BackendMessage]())

    // Zip each query chunk with the ReadyForQuery message, so messages are
    // sent when the server is ready for them.

    rollover ~> Startup(username, password, Map(
      "database" -> username,
      "application_name" -> "rascql-demo"
    )) ~> concat

    BidiShape(concat.in(1), concat.out, rollover.in, rollover.out(1))
  })

  val conn = Tcp().outgoingConnection(host = "localhost", port = 5432)

  val stdout =
    Flow[Source[Any, Any]].
      flatMapConcat(identity).
      toMat(Sink.fold(0) { (acc, e) => println(e) ; acc + 1 })(Keep.right)

  val (_, count) =
    QueryExecution().
      atop(PreparedStatementCache(5)).
      atop(AsyncOperations(Sink.foreach(println))).
      atop(startup).
      atop(Codec(charset)).
      join(conn).
      runWith(queries, stdout)

  count.onComplete { c =>
    println(c)
    system.shutdown()
  }

}
