/*
 * Copyright 2015 Philip L. McMahon
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

package rascql.postgresql.stream

import scala.concurrent.duration._
import akka.stream.scaladsl._
import akka.stream.testkit._
import rascql.postgresql.protocol._
import org.scalatest.MustMatchers

/**
 * Tests for [[QueryExecution]].
 *
 * @author Philip L. McMahon
 */
class QueryExecutionSpec extends StreamSpec with MustMatchers {

  "A send query stage" should {

    import PreparedStatement.{Unnamed => Stmt}
    import Portal.{Unnamed => Ptl}

    val stage = Flow[SendQuery].transform(() => new SendQueryStage)

    "convert a simple query" in flow(stage) { (src, sink) =>
      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(3)
      pub.sendNext(SendQuery("SELECT 1"))
      sink.expectNext(Query("SELECT 1"))
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

    "convert a prepared statement" in flow(stage) { (src, sink) =>
      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(6)
      pub.sendNext(SendQuery.Prepared("SELECT 1", Nil))
      sink.expectNext(Parse("SELECT 1", Nil, Stmt))
      sink.expectNext(Bind(Nil, Ptl, Stmt, None))
      sink.expectNext(Describe(Ptl))
      sink.expectNext(Execute(Ptl))
      sink.expectNext(Sync)
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

  }

  "A query result stage" should {

    val stage = Flow[BackendMessage].transform(() => new QueryResultStage)

    "produce an empty result" in flow(stage) { (src, sink) =>
      val pub = src.expectSubscription()
      val sub = sink.expectSubscription()
      sub.request(2)
      pub.sendNext(EmptyQueryResponse)
      sink.expectNext(EmptyQuery)
      sink.expectNoMsg(100.millis)
      sub.cancel()
      pub.expectCancellation()
    }

  }

  "A query execution bidirectional flow" should {

    import CommandTag._

    val qexec = QueryExecution()
    val idle = ReadyForQuery(TransactionStatus.Idle)
    val emptyDesc = RowDescription(Vector.empty)
    val emptyData = DataRow(Vector.empty)

    "signal termination when the query source finishes" in bidi(qexec) { (queries, femsgs, bemsgs, results) =>
      val qpub = queries.expectSubscription()
      val bepub = bemsgs.expectSubscription()
      List(femsgs, results).map(_.expectSubscription()).foreach(_.request(2))
      bepub.sendNext(idle)
      femsgs.expectNoMsg(100.millis)
      qpub.sendComplete()
      femsgs.expectNext(Terminate)
      femsgs.expectComplete()
      bepub.sendComplete()
      results.expectComplete()
    }

    "group commands into a stream of results" in bidi(qexec) { (queries, femsgs, bemsgs, results) =>
      val qpub = queries.expectSubscription()
      val bepub = bemsgs.expectSubscription()
      List(femsgs, results).foreach(_.expectSubscription().request(Int.MaxValue))
      bepub.sendNext(idle)
      1 to 5 foreach { i =>
        qpub.sendNext(SendQuery("BEGIN; SELECT $i; COMMIT"))
        femsgs.expectNext(Query("BEGIN; SELECT $i; COMMIT"))
        bepub.sendNext(CommandComplete(NameOnly("BEGIN")))
        val sink = TestSubscriber.probe[QueryResult]()
        results.expectNext().runWith(Sink.fromSubscriber(sink))
        sink.expectSubscription().request(Int.MaxValue)
        sink.expectNext(QueryComplete(NameOnly("BEGIN")))
        bepub.sendNext(emptyDesc)
        bepub.sendNext(emptyData)
        bepub.sendNext(CommandComplete(RowsAffected("SELECT", i)))
        val QueryRowSet(_, _, rows) = sink.expectNext()
        rows must have size(1)
        bepub.sendNext(CommandComplete(NameOnly("COMMIT")))
        sink.expectNext(QueryComplete(NameOnly("COMMIT")))
        bepub.sendNext(idle)
        sink.expectComplete()
      }
      qpub.sendComplete()
      femsgs.expectNext(Terminate)
      femsgs.expectComplete()
      bepub.sendComplete()
      results.expectComplete()
    }

    "complete an in-flight result when the query source finishes" in bidi(qexec) { (queries, femsgs, bemsgs, results) =>
      val qpub = queries.expectSubscription()
      val bepub = bemsgs.expectSubscription()
      val fesub = femsgs.expectSubscription()
      val rsub = results.expectSubscription()
      fesub.request(10)
      rsub.request(1)
      bepub.sendNext(idle)
      qpub.sendNext(SendQuery("SELECT 1"))
      qpub.sendComplete()
      femsgs.expectNext(Query("SELECT 1"))
      femsgs.expectNext(Terminate)
      femsgs.expectComplete()
      bepub.sendNext(emptyDesc)
      bepub.sendNext(emptyData)
      bepub.sendNext(CommandComplete(RowsAffected("SELECT", 1)))
      bepub.sendNext(idle)
      bepub.sendComplete()
      val source = results.expectNext()
      val sink = TestSubscriber.probe[QueryResult]()
      source.runWith(Sink.fromSubscriber(sink))
      sink.expectSubscription().request(2) // Get QRS and complete message
      val QueryRowSet(_, _, rows) = sink.expectNext()
      rows must have size(1)
      sink.expectComplete()
      results.expectComplete()
    }

  }

}
