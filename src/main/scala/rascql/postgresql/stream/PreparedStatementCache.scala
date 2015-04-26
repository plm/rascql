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
import akka.stream.stage._
import rascql.postgresql.protocol._

/**
 * A [[PreparedStatement]] cache which automatically evicts and closes the
 * least-recently used statement when capacity is exceeded.
 *
 * Upstream [[FrontendMessage]]s will be backpressured while parsing is in
 * progress, to ensure that the cache is consistent when each [[Parse]] is
 * received. Statements which fail parsing are not cached.
 *
 * {{{
 *                  . . . . . . . . . . . . .
 *                  .                       .
 *                  .   +---------------+   .
 *                  .   |               |   .
 * FrontendMessage --> [i]    cache    [o] --> FrontendMessage
 *                  .   |               |   .
 *                  .   +------[i]------+   .
 *                  .           ^           .
 *                  .           |           .
 *                  .   +------[o]------+   .
 *                  .   |               |   .
 *                  .   |    command    |   .
 *                  .   |               |   .
 *                  .   +------[i]------+   .
 *                  .           ^           .
 *                  .           |           .
 *                  .   +------[o]------+   .
 *                  .   |               |   .
 *  BackendMessage <-- [o]  broadcast  [i] <-- BackendMessage
 *                  .   |               |   .
 *                  .   +---------------+   .
 *                  .                       .
 *                  . . . . . . . . . . . . .
 * }}}
 *
 * @author Philip L. McMahon
 */
object PreparedStatementCache {

  import GraphDSL.Implicits._

  private[stream] sealed trait Command
  private[stream] case object Insert extends Command
  private[stream] case object Reset extends Command

  def apply(capacity: Int): BidiFlow[FrontendMessage, FrontendMessage, BackendMessage, BackendMessage, Unit] =
    BidiFlow.fromGraph(GraphDSL.create() { implicit b =>

      val cache = b.add(new PreparedStatementCache(capacity))
      val command = b.add(Flow[BackendMessage].collect {
        case ParseComplete => Insert
        case _: ReadyForQuery => Reset
      })
      val broadcast = b.add(Broadcast[BackendMessage](2))

      broadcast.out(1) ~> command ~> cache.in1

      BidiShape(cache.in0, cache.out, broadcast.in, broadcast.out(0))
    } named("PreparedStatementCache"))

}

/**
 * Caches [[PreparedStatement]]s using a simple LRU cache.
 *
 * Demand for commands is constant, so any pending commands, regardless of
 * recent messages, will be handled (or ignored). Demand for messages is
 * activate when a `Reset` command is received and stops when a `Query` or
 * `Sync` message is received, backpressuring further queries until the
 * current cycle completes.
 *
 * Queries which parse successfully are cached up to the configured capacity
 * limit. When caching a new query, the capacity will be exceeded until the
 * server handles any [[Close]] messages triggered for evicted queries.
 */
private[stream] class PreparedStatementCache(capacity: Int)
  extends GraphStage[FanInShape2[FrontendMessage, PreparedStatementCache.Command, FrontendMessage]] {

  require(capacity > 0)

  import PreparedStatementCache._

  type Behavior[T] = PartialFunction[T, Unit]

  case class Entry(id: Int, prepared: PreparedStatement, portal: Portal)

  val shape = new FanInShape2[FrontendMessage, PreparedStatementCache.Command, FrontendMessage]("PreparedStatementCache")

  def createLogic(attr: Attributes) = new GraphStageLogic(shape) {

    // TODO statement ID should be derived from query -- S_$hashCode.$length?
    // Do conversion in a separate stage, where we generate PrepStmt -> Int?
    var statements = Iterator.from(0).map("S_" + _.toHexString).map(PreparedStatement(_))
    var cache = Map.empty[Int, Entry]
    var recent = List.empty[Entry] // most recent is head of list

    object In {
      val Messages = shape.in0
      val Commands = shape.in1
    }
    val Out = shape.out

    var needMessage = false

    // Used to simulate "become" functionality
    object Behavior {
      var Messages: Behavior[FrontendMessage] = PartialFunction.empty
      var Commands: Behavior[Command] = PartialFunction.empty

      // Convenience method
      def apply[T](b: Behavior[T]): Behavior[T] = b
      def collect[T](pf: PartialFunction[T, FrontendMessage]): Behavior[T] =
        pf.andThen(push(Out, _))
    }

    val passThru = Behavior[FrontendMessage] {
      case msg => push(Out, msg)
    }

    val maybeParse: Behavior[FrontendMessage] = Behavior[FrontendMessage] {
      case Parse(query, types, _) =>
        val id = query.hashCode()
        Behavior.Messages = inject(cache.get(id).fold {
          val prepared = statements.next()
          push(Out, Parse(query, types, prepared))
          // Don't filter any messages
          val entry = Entry(id, prepared, Portal.Unnamed)
          Behavior.Commands = maybeCache(entry)
          entry
        } { entry =>
          val (newer, older) = recent.span(_ eq entry) // TODO Use filter(_ eq stmt)?
          recent = entry :: newer ++ older
          // Drop Parse since statement is cached,and re-trigger upstream demand
          pull(In.Messages)
          entry
        })
      case msg @ (_: Query | Sync) =>
        // Operations which complete a cycle
        push(Out, msg)
    } orElse passThru

    // Wait until all statement-specific messages have passed through
    def inject(entry: Entry) = Behavior.collect[FrontendMessage] {
      case Describe(c: Closable) =>
        // Until Sync is received, replace any describe request with one for our entry
        Describe(c match {
          case _: PreparedStatement => entry.prepared
          case _: Portal => entry.portal
        })
      case msg @ Close(p: PreparedStatement) =>
        // Drop from cache
        val (evict, keep) = recent.partition(_.prepared == p)
        recent = keep
        evict.foreach(cache -= _.id)
        msg
      case Bind(params, _, _, formats) =>
        Bind(params, entry.portal, entry.prepared, formats)
      case Execute(_, rows) =>
        Execute(entry.portal, rows)
      case Sync =>
        Behavior.Messages = maybeParse
        needMessage = false
        Sync
    } orElse passThru

    val reset = Behavior[Command] {
      case Reset =>
        needMessage = true
        if (!hasBeenPulled(In.Messages)) pull(In.Messages)
        pull(In.Commands)
    }

    def maybeCache(entry: Entry) = Behavior[Command] {
      case Insert =>
        cache += entry.id -> entry
        // Can only evict one at a time (and only cache one at a time)
        val (keep, evict) = recent.splitAt(capacity - 1)
        recent = entry :: keep
        Behavior.Commands = reset
        // If we did't evict an element, re-trigger demand upstream
        if (evict.isEmpty) pull(In.Commands)
        // Otherwise push element(s) downstream
        else evict.foreach {
          case Entry(k, p, _) =>
            cache -= k
            emit(Out, Close(p))
        }
    } orElse reset

    // Set initial state and behaviors

    Behavior.Commands = reset
    setHandler(In.Commands, new InHandler {
      def onPush() = Behavior.Commands.apply(grab(In.Commands))
    })

    Behavior.Messages = maybeParse
    setHandler(In.Messages, new InHandler {
      def onPush() = Behavior.Messages.apply(grab(In.Messages))
    })

    setHandler(Out, new OutHandler {
      def onPull() = {
        // Always express demand for commands
        if (!hasBeenPulled(In.Commands)) pull(In.Commands)
        // Trigger message demand only if query cycle is started
        if (needMessage) pull(In.Messages)
      }
    })

  }

}
