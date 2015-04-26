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

import org.scalatest.MustMatchers
import rascql.postgresql.protocol._

/**
 * Tests for [[PreparedStatementCache]].
 *
 * @author Philip L. McMahon
 */
class PreparedStatementCacheSpec extends StreamSpec with MustMatchers {

  "A prepared statement cache bidirectional flow" should {

    val cache = PreparedStatementCache(1)

    val parse1 = Parse("SELECT 1 WHERE 'a' = $1")
    val parse2 = Parse("SELECT 2 WHERE 'b' = $1")
    val idle = ReadyForQuery(TransactionStatus.Idle)

    "cache parsed statements" in bidi(cache) { (fin, fout, bin, bout) =>
      val fpub = fin.expectSubscription()
      val bpub = bin.expectSubscription()
      List(fout, bout).map(_.expectSubscription()).foreach(_.request(Int.MaxValue))
      bpub.sendNext(idle)
      bout.expectNext(idle)
      fpub.sendNext(parse1)
      val stmt1 = fout.expectNextPF {
        case Parse(parse1.query, parse1.types, dest) => dest
      }
      stmt1 must not equal PreparedStatement.Unnamed
      fpub.sendNext(Sync)
      fout.expectNext(Sync)
      bpub.sendNext(ParseComplete)
      bpub.sendNext(idle)
      bout.expectNext(ParseComplete)
      bout.expectNext(idle)
      fpub.sendNext(parse1)
      fpub.sendNext(Sync)
      fout.expectNext(Sync)
      bpub.sendNext(idle)
      bout.expectNext(idle)
      fpub.sendNext(parse2)
      val stmt2 = fout.expectNextPF {
        case Parse(_, _, dest) => dest
      }
      stmt2 must not equal PreparedStatement.Unnamed
      fpub.sendNext(Sync)
      fout.expectNext(Sync)
      bpub.sendNext(ParseComplete)
      bpub.sendNext(idle)
      bout.expectNext(ParseComplete)
      bout.expectNext(idle)
      fout.expectNext(Close(stmt1))
      bpub.sendNext(CloseComplete)
      bout.expectNext(CloseComplete)
    }

    "pass simple queries" in bidi(cache) { (fin, fout, bin, bout) =>
      val fpub = fin.expectSubscription()
      val bpub = bin.expectSubscription()
      List(fout, bout).map(_.expectSubscription()).foreach(_.request(Int.MaxValue))
      bpub.sendNext(idle)
      bout.expectNext(idle)
      (0 to 5).map { i => Query(s"SELECT $i")}.foreach { query =>
        fpub.sendNext(query)
        fout.expectNext(query)
        bpub.sendNext(idle) // Normal messages omitted
        bout.expectNext(idle)
      }
    }

  }

}
