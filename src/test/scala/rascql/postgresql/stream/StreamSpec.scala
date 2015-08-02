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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.testkit._
import akka.testkit._
import org.scalatest._

/**
 * Base trait for stream-based test specifications.
 *
 * @author Philip L. McMahon
 */
private[stream] trait StreamSpec extends TestKitBase with WordSpecLike with BeforeAndAfterAll {

  implicit lazy val system = ActorSystem(this.getClass.getSimpleName)

  implicit lazy val materializer = ActorMaterializer()

  override protected def afterAll() = system.shutdown()

  type PubProbe[T] = TestPublisher.ManualProbe[T]
  type SubProbe[T] = TestSubscriber.ManualProbe[T]

  def flow[In, Out](flow: Flow[In, Out, _])(fn: (PubProbe[In], SubProbe[Out]) => Unit): Unit = {
    val src = TestPublisher.manualProbe[In]()
    val sink = TestSubscriber.manualProbe[Out]()

    Source.fromPublisher(src).via(flow).runWith(Sink.fromSubscriber(sink))

    fn(src, sink)
  }

  def bidi[I1, O1, I2, O2](bidi: BidiFlow[I1, O1, I2, O2, _])(fn: (PubProbe[I1], SubProbe[O1], PubProbe[I2], SubProbe[O2]) => Unit): Unit = {
    val lsrc = TestPublisher.manualProbe[I1]()
    val rsink = TestSubscriber.manualProbe[O1]()
    val rsrc = TestPublisher.manualProbe[I2]()
    val lsink = TestSubscriber.manualProbe[O2]()

    bidi.join(Flow.fromSinkAndSourceMat(Sink.fromSubscriber(rsink), Source.fromPublisher(rsrc))(Keep.none)).
      runWith(Source.fromPublisher(lsrc), Sink.fromSubscriber(lsink))

    fn(lsrc, rsink, rsrc, lsink)
  }

}
