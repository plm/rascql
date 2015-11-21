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

import akka.stream._
import akka.stream.scaladsl._
import rascql.postgresql.protocol._

/**
 * Executes simple and extended (prepared) queries, backpressuring when a query
 * is received and resuming when the server is ready for the next query.
 *
 * A [[Terminate]] is sent when the [[SendQuery]] source finishes, tearing down
 * the stream. The current in-flight result, if any, will be returned and then
 * the result sink will finish.
 *
 * {{{
 *                      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
 *                      .                                                                                               .
 *                      .                                                                           +---------------+   .
 *                      .                                                                           |               |   .
 *                      .                                                                           |   terminate   |   .
 *                      .                                                                           |               |   .
 *                      .                                                                           +------[o]------+   .
 *                      .                                                                                   |           .
 *                      .                                                                                   v           .
 *                      .                           +---------------+       +---------------+       +------[i]------+   .
 *                      .                           |               |       |               |       |               |   .
 *           SendQuery --------------------------> [i]             [o] --> [i]   queries   [o] --> [i]   concat    [o] --> FrontendMessage
 *                      .                           |               |       |               |       |               |   .
 *                      .                           |               |       +---------------+       +---------------+   .
 *                      .                           |     query     |                                                   .
 *                      .                           |     cycle     |                                                   .
 *                      .   +---------------+       |               |                                                   .
 *                      .   |               |       |               |                                                   .
 * Source[QueryResult] <-- [o]   results   [i] <-- [o]             [i] <-------------------------------------------------- BackendMessage
 *                      .   |               |       |               |                                                   .
 *                      .   +---------------+       +---------------+                                                   .
 *                      .                                                                                               .
 *                      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
 *
 * @author Philip L. McMahon
 */
object QueryExecution {

  import GraphDSL.Implicits._

  def apply(): BidiFlow[SendQuery, FrontendMessage, BackendMessage, Source[QueryResult, Unit], Unit] =
    BidiFlow.fromGraph(GraphDSL.create() { implicit b =>

      val qc = b.add(new QueryCycle)
      val concat = b.add(Concat[FrontendMessage]())
      val queries = b.add(Flow[SendQuery].transform(() => new SendQueryStage))
      val terminate = b.add(Source.single(Terminate))
      val results = b.add(Flow[BackendMessage].
        splitAfter(_.isInstanceOf[ReadyForQuery]).
        transform(() => new QueryResultStage).
        prefixAndTail(0).
        map(_._2).
        concatSubstreams)

      qc.out1 ~> queries ~> concat
               terminate ~> concat
      qc.out2 ~> results

      BidiShape(qc.in1, concat.out, qc.in2, results.outlet)
    } named("QueryExecution"))

}
