package trie
import org.scalatest.FunSpec
import org.scalatest.Matchers
import java.io._

class TrieSpec extends FunSpec with Matchers {

  def tests(newstorage: () => Writer, name: String) = {

    describe("in memory storage " + name) {
      it("simple") {
        val s = newstorage()
        s.set(5, Array[Byte](0, 1, 2, 3, 4))
        val buf = Array.ofDim[Byte](10)
        s.get(0, buf)
        buf.toList should equal(List[Byte](0, 0, 0, 0, 0, 0, 1, 2, 3, 4))
        s.size should equal(10)
      }
    }

    describe("node storage " + name) {
      it("simple") {
        val n1 = Node(Array.fill(256)(0L), 113L).copy(address = 0L)
        val n2 = Node(Array.fill(256)(0L), 114L).copy(address = 2056L)
        val s = newstorage()
        val ns = new NodeWriter(s)
        ns.append(n1)
        ns.read(0).get should equal(n1)
        ns.append(n2)
        ns.read(0).get should equal(n1)
        // ns.read(2056).get should equal(n2)
        // ns.read(2057) should equal(None)
      }
    }

    describe("cnode storage " + name) {
      it("simple") {
        val n1 = CNode(Array.fill(256)(0L).updated(1, 13L), 113L, List[Byte](0, 1)).copy(address = 0L)
        val n2 = CNode(Array.fill(256)(0L), 114L, List[Byte](0, 1, 2, 3))
        val s = newstorage()
        val ns = new CNodeWriter(s)
        ns.append(n1)
        ns.read(0).get should equal(n1)
        ns.readAddress(0, 1) should equal(13L)
        ns.readPartial(0) should equal(PartialCNode(0, List[Byte](0, 1)))
        ns.append(n2)
        ns.read(0).get should equal(n1)
        ns.updatePayload(n1, 3L)
        ns.read(0).get should equal(n1.copy(payload = 3L))
        ns.updateRoute(n1, 2, 3L)
        ns.read(0).get should equal(n1.copy(payload = 3L, children = n1.children.updated(2, 3L)))
        // ns.read(2056).get should equal(n2)
        // ns.read(2057) should equal(None)
      }
    }

    describe("ctrie " + name) {
      it("write+query small") {
        val data = List("aaa" -> 0L, "a" -> 1L, "aa" -> 3L, "b" -> 2L, "bb" -> 4L, "ab" -> 5L, "aba" -> 6L, "abb" -> 7L, "tester" -> 8L, "test" -> 9L, "team" -> 10L, "toast" -> 11L).take(3)
          .map(x => x._1.getBytes("US-ASCII").toSeq -> x._2)
        val s = newstorage()
        val ns = new CNodeWriter(s)
        CTrie.build(data.iterator, ns)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns, k) should equal(Some(v))
        }
        CTrie.query(ns, "abc".getBytes("US-ASCII").toSeq) should equal(None)
        CTrie.query(ns, "c".getBytes("US-ASCII").toSeq) should equal(None)
        CTrie.query(ns, "aaaa".getBytes("US-ASCII").toSeq) should equal(None)
        CTrie.query(ns, "bba".getBytes("US-ASCII").toSeq) should equal(None)
        CTrie.query(ns, "bbb".getBytes("US-ASCII").toSeq) should equal(None)

      }

    }

    describe("trie " + name) {
      it("write+query small") {
        val data = List("a" -> 1L, "b" -> 2L, "aa" -> 3L, "bb" -> 4L, "ab" -> 5L, "aba" -> 6L, "abb" -> 7L)
          .map(x => x._1.getBytes("US-ASCII").toSeq -> x._2)
        val s = newstorage()
        val ns = new NodeWriter(s)
        Trie.build(data.iterator, ns)
        data.foreach {
          case (k, v) =>
            Trie.query(ns, k) should equal(Some(v))
        }
        Trie.query(ns, "abc".getBytes("US-ASCII").toSeq) should equal(None)
        Trie.query(ns, "c".getBytes("US-ASCII").toSeq) should equal(None)
        Trie.query(ns, "aaa".getBytes("US-ASCII").toSeq) should equal(None)
        Trie.query(ns, "bba".getBytes("US-ASCII").toSeq) should equal(None)
        Trie.query(ns, "bbb".getBytes("US-ASCII").toSeq) should equal(None)

      }

    }
  }

  tests(() => new InMemoryStorage, "inmemory")
  // tests(() => {
  //   val tmp = File.createTempFile("dfs", "dfsd")
  //   new FileWriter(tmp)
  // }, "file")

  describe("big") {
    ignore("trie ") {
      val data1 = 0 to 10000 map (i => 0 to 100 map (j => scala.util.Random.nextPrintableChar) mkString)
      val data = data1.zipWithIndex
        .map(x => x._1.getBytes("US-ASCII").toSeq -> x._2.toLong)
      val s = {
        val tmp = File.createTempFile("trie", "dfsd")
        println(tmp)
        new FileWriter(tmp)
      }
      val ns = new NodeWriter(s)
      val t1 = System.nanoTime
      Trie.build(data.iterator, ns)
      println((System.nanoTime - t1) / 1E9)
      val ts = data.map {
        case (k, v) =>
          val t1 = System.nanoTime
          Trie.query(ns, k) should equal(Some(v))
          (System.nanoTime - t1) / 1E9
      }
      println(ts.sum / ts.size)
      Trie.query(ns, "abc".getBytes("US-ASCII").toSeq) should equal(None)
      Trie.query(ns, "c".getBytes("US-ASCII").toSeq) should equal(None)
      Trie.query(ns, "aaa".getBytes("US-ASCII").toSeq) should equal(None)
      Trie.query(ns, "bba".getBytes("US-ASCII").toSeq) should equal(None)
      Trie.query(ns, "bbb".getBytes("US-ASCII").toSeq) should equal(None)

    }

    it("ctrie ") {
      val data1 = 0 to 100000 map (i => 0 to 30 map (j => scala.util.Random.nextPrintableChar) mkString)
      val data = data1.zipWithIndex
        .map(x => x._1.getBytes("US-ASCII").toSeq -> x._2.toLong)
      val tmp = File.createTempFile("ctrie", "dfsd")
      println(tmp)
      val s =
        new FileWriter(tmp)

      val ns = new CNodeWriter(s)
      val t1 = System.nanoTime
      CTrie.build(data.iterator, ns)
      println((System.nanoTime - t1) / 1E9)
      val ts = data.map {
        case (k, v) =>
          val t1 = System.nanoTime
          CTrie.query(ns, k) should equal(Some(v))
          (System.nanoTime - t1) / 1E9
      }
      println(ts.sum / ts.size)
      CTrie.query(ns, "abc".getBytes("US-ASCII").toSeq) should equal(None)
      CTrie.query(ns, "c".getBytes("US-ASCII").toSeq) should equal(None)
      CTrie.query(ns, "aaa".getBytes("US-ASCII").toSeq) should equal(None)
      CTrie.query(ns, "bba".getBytes("US-ASCII").toSeq) should equal(None)
      CTrie.query(ns, "bbb".getBytes("US-ASCII").toSeq) should equal(None)
      println(tmp.length / 1024 / 1024)
      tmp.delete
    }
  }
}
