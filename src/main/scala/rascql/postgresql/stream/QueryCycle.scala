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
import akka.stream.stage._
import rascql.postgresql.protocol._

/**
 * Don't request the next statement unless we've seen the prior statement
 * complete.
 *
 * @author Philip L. McMahon
 */
class QueryCycle
  extends GraphStage[BidiShape[SendQuery, SendQuery, BackendMessage, BackendMessage]] {

  val shape = BidiShape(Inlet[SendQuery]("query.in"),
                        Outlet[SendQuery]("query.out"),
                        Inlet[BackendMessage]("response.in"),
                        Outlet[BackendMessage]("response.out"))

  def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {

    object Query {
      val In = shape.in1
      val Out = shape.out1
    }
    object Reply {
      val In = shape.in2
      val Out = shape.out2
    }

    // FIXME Only send RFQ upstream to result if we get element from Query.in
    // Since then we know/expect a result stream to follow
    // Note that doing so makes some assumptions about how results will be
    // split into substreams by the next upstream stage

    val drainResult = new InHandler {
      def onPush() = {
        val msg = grab(Reply.In)
        if (msg.isInstanceOf[ReadyForQuery]) complete(Reply.Out)
        else push(Reply.Out, msg)
      }
    }

    val queryExecution = new InHandler {
      def onPush() = {
        val msg = grab(Reply.In)
        push(Reply.Out, msg)
        if (msg.isInstanceOf[ReadyForQuery]) pull(Query.In)
      }
    }

    // Push SendQuery downstream whenever one is available (demand is regulated by RFQ)
    setHandler(Query.In, new InHandler {
      def onPush() = push(Query.Out, grab(Query.In))
      override def onUpstreamFinish() = {
        setHandler(Reply.In, drainResult)
        complete(Query.Out)
      }
    })

    // Pull from SendQuery input is driven by RFQ messages from backend
    setHandler(Query.Out, new OutHandler {
      def onPull() = ()
      override def onDownstreamFinish() = cancel(Query.In)
    })

    // When a RFQ is received, request first query
    // TODO Handle query input finishing?
    setHandler(Reply.In, new InHandler {
      def onPush() = {
        grab(Reply.In) match {
          case ReadyForQuery(status) =>
            pull(Query.In) // Initial demand for query elements
            setHandler(Reply.In, queryExecution)
          case _ =>
            // Drop message
        }
        // Since no downstream push occurred, trigger demand again
        pull(Reply.In)
      }
    })

    setHandler(Reply.Out, new OutHandler {
      def onPull() = pull(Reply.In)
    })

  }

}
