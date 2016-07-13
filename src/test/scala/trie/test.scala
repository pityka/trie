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
        val buf = s.readBytes(0, 10)
        buf.toVector should equal(List[Byte](0, 0, 0, 0, 0, 0, 1, 2, 3, 4))
        s.readBytesInto(6, buf, 0, 3)
        buf.toVector should equal(List[Byte](1, 2, 3, 0, 0, 0, 1, 2, 3, 4))
        s.size should equal(10)
        s.readByte(6) should equal(1.toByte)
      }
    }

    describe("cnode adaptive storage " + name) {
      it("simple") {
        val n1 = CNode(Array.fill(256)(-1L).updated(1, 13L), 113L, Vector[Byte](0, 1)).copy(address = 0L)
        val n2 = CNode(Array.fill(256)(-1L), 114L, Vector[Byte](0, 1, 2, 3))
        val s = newstorage()
        val ns = new CANodeWriter(s)
        ns.append(n1)
        ns.read(0).get should equal(n1)
        ns.readAddress(0, 1) should equal(13L)
        val buf = Array[Byte](3, 2, 1)
        val (a1, a2, a3) = ns.readPartial(0, buf, 1)
        (a1, a2.toVector, a3) should equal((0, List[Byte](3, 0, 1), 3))
        // ns.append(n2)
        ns.read(0).get should equal(n1)
        ns.updatePayload(n1, 3L)
        ns.read(0).get should equal(n1.copy(payload = 3L))
        ns.updateRoute(n1, 2, 3L)
        ns.read(0).get should equal(n1.copy(payload = 3L, children = n1.children.updated(2, 3L)))
        0 to 48 map { i =>
          ns.updateRoute(n1, i.toByte, i.toLong)
        }
        ns.read(0).get.children.take(49).toVector should equal(0 to 48 toVector)
        // ns.read(2056).get should equal(n2)
        // ns.read(2057) should equal(None)
      }
    }

    describe("catrie " + name) {
      it("write+query small") {
        val data = List("aaa" -> 0L, "a" -> 1L, "aa" -> 3L, "b" -> 2L, "bb" -> 4L, "ab" -> 5L, "aba" -> 6L, "abb" -> 7L, "tester" -> 8L, "test" -> 9L, "team" -> 10L, "toast" -> 11L)
          .map(x => x._1.getBytes("US-ASCII").toVector -> x._2)
        val s = newstorage()
        val ns = new CANodeWriter(s)
        CTrie.build(data.iterator, ns)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns, k) should equal(Some(v))
        }
        CTrie.query(ns, "abc".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns, "c".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns, "aaaa".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns, "bba".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns, "bbb".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.prefixPayload(ns, "a".toVector.map(_.toByte)).toSet should equal(Set(0l, 1L, 3L, 7L, 6L, 5L))
        CTrie.prefixPayload(ns, "z".toVector.map(_.toByte)).toSet should equal(Set())
        CTrie.prefixPayload(ns, "tes".toVector.map(_.toByte)).toSet should equal(Set(8L, 9L))

      }

    }

  }

  tests(() => new InMemoryStorage, "inmemory")
  tests(() => {
    val tmp = File.createTempFile("dfs", "dfsd")
    FileWriter.open(tmp)
  }, "file")

  def bigTest(openWriter: (File) => Writer, openReader: File => Reader, name: String) = {
    describe("big") {

      it("kmer" + name) {
        def kmer(i: Int, c: List[Byte], acc: List[Vector[Byte]]): List[Vector[Byte]] =
          if (i == 0) acc
          else kmer(i - 1, c, c.flatMap(c => acc.map(v => v :+ c)))

        val buf = Array.ofDim[Byte](50)
        val alphabet = List('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o').map(_.toByte)
        val data = kmer(5, alphabet, alphabet.toVector :: Nil).zipWithIndex.map(x => x._1 -> x._2.toLong)
        val data1 = scala.util.Random.shuffle(data)
        val data2 = scala.util.Random.shuffle(data)
        println("kmers size: " + data.size)
        val tmp = File.createTempFile("catrie", "dfsd")
        println(tmp)
        val s = openWriter(tmp) //FileWriter.open(tmp)

        val ns = new CANodeWriter(s)
        val t1 = System.nanoTime
        CTrie.build(data1.iterator, ns)
        s.close
        println(name + " " + (System.nanoTime - t1) / 1E9)

        val s2 = openReader(tmp) //FileReader.open(tmp)
        val ns2 = new CANodeReader(s2)
        val ts = data2.map {
          case (k, v) =>
            val t1 = System.nanoTime
            CTrie.prefixPayload(ns2, k) should equal(Vector(v))
            (System.nanoTime - t1) / 1E9
        }.toVector
        println(name + " " + ts.sorted.apply(ts.size / 2))
        println(name + " " + ts.sorted.apply((ts.size * 0.25).toInt))
        println(name + " " + ts.sorted.apply((ts.size * 0.75).toInt))
        println(name + " " + ts.min)
        println(name + " " + ts.zipWithIndex.maxBy(_._1))
        val max = data(ts.zipWithIndex.maxBy(_._1)._2)._1
        val tmax = 0 until 10000 map { i =>
          val t1 = System.nanoTime
          CTrie.query(ns2, max)
          (System.nanoTime - t1) / 1E9
        }
        println(name + " " + tmax.min + " " + tmax.sorted.apply(5000) + " " + tmax.max)
        println(name + " " + tmp.length / 1024 / 1024)
        tmp.delete
      }

      it("random " + name) {
        val buf = Array.ofDim[Byte](50)

        def data = {
          val rnd = new scala.util.Random(1)
          (0 to 1000000 iterator).map(i => 0 to 30 map (j => rnd.nextPrintableChar) mkString).zipWithIndex
            .map(x => x._1.getBytes("US-ASCII").toVector -> x._2.toLong)
        }
        val tmp = File.createTempFile("catrie", "dfsd")
        println(tmp)
        val s = openWriter(tmp)

        val ns = new CANodeWriter(s)
        val t1 = System.nanoTime
        CTrie.build(data, ns)
        s.close
        println(name + " " + (System.nanoTime - t1) / 1E9)

        val s2 = openReader(tmp)
        val ns2 = new CANodeReader(s2)
        val ts = data.map {
          case (k, v) =>
            val t1 = System.nanoTime
            CTrie.query(ns2, k) should equal(Some(v))
            (System.nanoTime - t1) / 1E9
        }.toVector
        println(name + " " + ts.sorted.apply(ts.size / 2))
        println(name + " " + ts.sorted.apply((ts.size * 0.25).toInt))
        println(name + " " + ts.sorted.apply((ts.size * 0.75).toInt))
        println(name + " " + ts.min)
        println(name + " " + ts.zipWithIndex.maxBy(_._1))
        CTrie.query(ns2, "abc".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns2, "c".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns2, "aaa".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns2, "bba".getBytes("US-ASCII").toVector) should equal(None)
        CTrie.query(ns2, "bbb".getBytes("US-ASCII").toVector) should equal(None)
        println(tmp.length / 1024 / 1024)
        tmp.delete
      }
    }
  }

  bigTest(
    (f: File) => FileWriter.open(f),
    (f: File) => FileReader.open(f),
    "nio"
  )

}
