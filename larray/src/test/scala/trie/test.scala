package trie
import org.scalatest.FunSpec
import org.scalatest.Matchers
import java.io._
import scala.collection.mutable.ArrayBuffer

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
        val n1 = CNode(ArrayBuffer(1.toByte -> 13L), 113L, Array[Byte](0, 1)).copy(address = 0L)
        val n2 = CNode(ArrayBuffer(), 114L, Array[Byte](0, 1, 2, 3))
        val s = newstorage()
        val ns = new CANodeWriter(s)
        ns.append(n1, 0)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.readAddress(0, 1) should equal(13L)
        val buf = Array[Byte](3, 2, 1)
        val (a2, a3) = ns.readPartial(0, buf, 1)
        (a2.toVector, a3) should equal((List[Byte](3, 0, 1), 3))
        // ns.append(n2)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updatePayload(n1.address, 3L)
        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updateRoute(n1.address, 2, 3L)
        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(ArrayBuffer(1.toByte -> 13L, 2.toByte -> 3L))
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)

        0 to 48 map { i =>
          ns.updateRoute(n1.address, i.toByte, i.toLong)
        }
        ns.read(0).get.children.take(49).toSet should equal(0 to 48 map (x => x.toByte -> x.toLong) toSet)
        ns.append(ns.read(0).get, 0).children.toVector should equal(ns.read(0).get.children.toVector)
        // ns.read(2056).get should equal(n2)
        // ns.read(2057) should equal(None)
      }
    }

    describe("cahnode adaptive storage " + name) {
      it("short") {
        val n1 = CNode(ArrayBuffer(1.toByte -> 13L), 113L, Array[Byte](0, 1)).copy(address = 0L)
        val n2 = CNode(ArrayBuffer(), 114L, Array[Byte](0, 1, 2, 3))
        val s = newstorage()
        val ns = new CAHNodeWriter(s)
        ns.append(n1, 0)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.readAddress(0, 1) should equal(13L)
        val buf = Array[Byte](3, 2, 1)
        val (a2, a3) = ns.readPartial(0, buf, 1)
        (a2.toVector, a3) should equal((List[Byte](3, 0, 1), 3))
        // // ns.append(n2)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updatePayload(n1.address, 3L)
        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updateRoute(n1.address, 2, 3L)

        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(n1.children :+ (2, 3L))
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)

        0 to 48 map { i =>
          ns.updateRoute(n1.address, i.toByte, i.toLong)
        }

        ns.read(0).get.children.take(49).toMap should equal(0 to 48 map (x => x.toByte -> x.toLong) toMap)
        // ns.append(ns.read(0).get).children.toVector should equal(ns.read(0).get.children.toVector)

      }
      it("hash") {
        val n1 = CNode(ArrayBuffer(1.toByte -> 13L, 2.toByte -> 14L, 3.toByte -> 15L), 113L, Array[Byte](0, 1)).copy(address = 0L)
        val s = newstorage()
        val ns = new CAHNodeWriter(s)
        ns.append(n1, 0)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.readAddress(0, 1) should equal(13L)
        ns.readAddress(0, 2) should equal(14L)
        ns.readAddress(0, 3) should equal(15L)
        val buf = Array[Byte](3, 2, 1)
        val (a2, a3) = ns.readPartial(0, buf, 1)
        (a2.toVector, a3) should equal((List[Byte](3, 0, 1), 3))
        // // ns.append(n2)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updatePayload(n1.address, 3L)
        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updateRoute(n1.address, 2, 3L)

        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(ArrayBuffer(1.toByte -> 13L, 2.toByte -> 3L, 3.toByte -> 15L))
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)

        0 to 48 map { i =>
          ns.updateRoute(n1.address, i.toByte, i.toLong)
        }

        ns.read(0).get.children.take(49).toMap should equal(0 to 48 map (x => x.toByte -> x.toLong) toMap)
        // ns.append(ns.read(0).get).children.toVector should equal(ns.read(0).get.children.toVector)

      }
    }

    describe("buffered " + name) {
      it("simple") {
        val n1 = CNode(ArrayBuffer(1.toByte -> 13L), 113L, Array[Byte](0, 1)).copy(address = 0L)
        val n2 = CNode(ArrayBuffer(), 114L, Array[Byte](0, 1, 2, 3))
        val s = newstorage()
        val ns = new BufferedCNodeWriter(new CAHNodeWriter(s), 2)
        ns.append(n1, 0)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.readAddress(0, 1) should equal(13L)
        val buf = Array[Byte](3, 2, 1)
        val (a2, a3) = ns.readPartial(0, buf, 1)
        (a2.toVector, a3) should equal((List[Byte](3, 0, 1), 3))
        // ns.append(n2)
        ns.read(0).get.payload should equal(n1.payload)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updatePayload(n1.address, 3L)
        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(n1.children)
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)
        ns.updateRoute(n1.address, 2, 3L)
        ns.read(0).get.payload should equal(3L)
        ns.read(0).get.children should equal(ArrayBuffer(1.toByte -> 13L, 2.toByte -> 3L))
        ns.read(0).get.prefix.toList should equal(n1.prefix.toList)

        0 to 48 map { i =>
          ns.updateRoute(n1.address, i.toByte, i.toLong)
        }
        ns.read(0).get.children.take(49).toMap should equal(0 to 48 map (x => x.toByte -> x.toLong) toMap)

        ns.append(ns.read(0).get, 0).children.toVector should equal(ns.read(0).get.children.toVector)
        // ns.read(2056).get should equal(n2)
        // ns.read(2057) should equal(None)
      }

      it("write+query small") {
        val data = List("aaa" -> 0L, "a" -> 1L, "aa" -> 3L, "b" -> 2L, "bb" -> 4L, "ab" -> 5L, "aba" -> 6L, "abb" -> 7L, "tester" -> 8L, "test" -> 9L, "team" -> 10L, "toast" -> 11L).take(10)
          .map(x => x._1.getBytes("US-ASCII") -> x._2)
        val s = newstorage()
        val ns = new BufferedCNodeWriter(new CAHNodeWriter(s), 2)
        CTrie.build(data.iterator, ns)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns, k) should equal(Some(v))
        }
        CTrie.query(ns, "abc".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "c".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "aaaa".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "bba".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "bbb".getBytes("US-ASCII")) should equal(None)
        CTrie.prefixPayload(ns, "a".toVector.map(_.toByte).toArray).toSet should equal(Set(0l, 1L, 3L, 7L, 6L, 5L))
        CTrie.prefixPayload(ns, "z".toVector.map(_.toByte).toArray).toSet should equal(Set())
        CTrie.prefixPayload(ns, "tes".toVector.map(_.toByte).toArray).toSet should equal(Set(8L, 9L))

        // CTrie.traverse(ns).toList.map(x => new String(x.prefix.map(_.toChar))) should equal(List("", "a", "b", "t", "a", "b", "b", "e", "oast", "a", "a", "b", "am", "st", "er"))

        val s2 = newstorage()
        val ns2 = new BufferedCNodeWriter(new CAHNodeWriter(s2), 2)
        CTrie.copy(ns, ns2)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns2, k) should equal(Some(v))
        }

      }
    }

    describe("catrie " + name) {
      it("write+query small") {
        val data = List("aaa" -> 0L, "a" -> 1L, "aa" -> 3L, "b" -> 2L, "bb" -> 4L, "ab" -> 5L, "aba" -> 6L, "abb" -> 7L, "tester" -> 8L, "test" -> 9L, "team" -> 10L, "toast" -> 11L)
          .map(x => x._1.getBytes("US-ASCII") -> x._2)
        val s = newstorage()
        val ns = new CANodeWriter(s)
        CTrie.build(data.iterator, ns)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns, k) should equal(Some(v))
        }
        CTrie.query(ns, "abc".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "c".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "aaaa".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "bba".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "bbb".getBytes("US-ASCII")) should equal(None)
        CTrie.prefixPayload(ns, "a".toVector.map(_.toByte).toArray).toSet should equal(Set(0l, 1L, 3L, 7L, 6L, 5L))
        CTrie.prefixPayload(ns, "z".toVector.map(_.toByte).toArray).toSet should equal(Set())
        CTrie.prefixPayload(ns, "tes".toVector.map(_.toByte).toArray).toSet should equal(Set(8L, 9L))

        // CTrie.traverse(ns).toList.map(x => new String(x.prefix.map(_.toChar))) should equal(List("", "a", "b", "t", "a", "b", "b", "e", "oast", "a", "a", "b", "am", "st", "er"))

        val s2 = newstorage()
        val ns2 = new CANodeWriter(s2)
        CTrie.copy(ns, ns2)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns2, k) should equal(Some(v))
        }

      }

    }

    describe("cahtrie " + name) {
      it("write+query small") {
        val data = List("aaa" -> 0L, "a" -> 1L, "aa" -> 3L, "b" -> 2L, "bb" -> 4L, "ab" -> 5L, "aba" -> 6L, "abb" -> 7L, "tester" -> 8L, "test" -> 9L, "team" -> 10L, "toast" -> 11L)
          .map(x => x._1.getBytes("US-ASCII") -> x._2)
        val s = newstorage()
        val ns = new CAHNodeWriter(s)
        CTrie.build(data.iterator, ns)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns, k) should equal(Some(v))
        }
        CTrie.query(ns, "abc".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "c".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "aaaa".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "bba".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns, "bbb".getBytes("US-ASCII")) should equal(None)
        CTrie.prefixPayload(ns, "a".toVector.map(_.toByte).toArray).toSet should equal(Set(0l, 1L, 3L, 7L, 6L, 5L))
        CTrie.prefixPayload(ns, "z".toVector.map(_.toByte).toArray).toSet should equal(Set())
        CTrie.prefixPayload(ns, "tes".toVector.map(_.toByte).toArray).toSet should equal(Set(8L, 9L))

        // CTrie.traverse(ns).toList.map(x => new String(x.prefix.map(_.toChar))) should equal(List("", "a", "b", "t", "a", "b", "b", "e", "oast", "a", "a", "b", "am", "st", "er"))

        val s2 = newstorage()
        val ns2 = new CAHNodeWriter(s2)
        CTrie.copy(ns, ns2)
        data.foreach {
          case (k, v) =>
            CTrie.query(ns2, k) should equal(Some(v))
        }

      }

    }

  }

  tests(() => new InMemoryStorage, "inmemory")
  // tests(() => new InMemoryStorageLArray, "inmemorylarray")
  // tests(() => {
  //   val tmp = File.createTempFile("dfs", "dfsd")
  //   FileWriter.open(tmp)
  // }, "file")
  // tests(() => {
  //   val tmp = File.createTempFile("dfs", "dfsd")
  //   println(tmp)
  //   new LFileWriter(tmp)
  // }, "lfile")

  def bigTest(dataSize: Int, openWriter: (File) => Writer, openReader: File => Reader, name: String, openNodeWriter: Writer => CNodeWriter, openNodeReader: Reader => CNodeReader) = {
    describe("big " + dataSize) {

      it("digits " + name) {

        val buf = Array.ofDim[Byte](50)
        val data = (0 until dataSize)
          .toList.map(_.toString.getBytes("US-ASCII")).zipWithIndex.map(x => x._1.toArray -> x._2.toLong)
        val random = new scala.util.Random(3)
        val data1 = random.shuffle(data)
        val data2 = random.shuffle(data)
        println("digits data size: " + data.size)
        val tmp = File.createTempFile("catrie", "dfsd")
        println("digits file " + tmp)
        val s = openWriter(tmp) //FileWriter.open(tmp)

        val ns = openNodeWriter(s)
        val t1 = System.nanoTime
        CTrie.build(data1.iterator, ns)
        s.close
        println("digits " + name + " build time " + (System.nanoTime - t1) / 1E9)

        val s2 = openReader(tmp) //FileReader.open(tmp)
        val ns2 = openNodeReader(s2)
        val ts = data2.map {
          case (k, v) =>
            val t1 = System.nanoTime
            CTrie.query(ns2, k) should equal(Some(v))
            (System.nanoTime - t1) / 1E9
        }.toVector
        data2.foreach {
          case (k, v) =>
            CTrie.query(ns2, k) should equal(Some(v))
        }
        println("digits " + name + " read median " + ts.sorted.apply(ts.size / 2))
        println("digits " + name + "1q " + ts.sorted.apply((ts.size * 0.25).toInt))
        println("digits " + name + "3q " + ts.sorted.apply((ts.size * 0.75).toInt))
        println("digits " + name + "min " + ts.min)
        println("digits " + name + "max " + ts.zipWithIndex.maxBy(_._1))
        val max = data(ts.zipWithIndex.maxBy(_._1)._2)._1
        val tmax = 0 until 10000 map { i =>
          val t1 = System.nanoTime
          CTrie.query(ns2, max)
          (System.nanoTime - t1) / 1E9
        }
        println("digits " + name + " " + tmax.min + " " + tmax.sorted.apply(5000) + " " + tmax.max)
        println("digits " + name + " digits: " + tmp.length / 1024 / 1024)
        println("digits " + name + " digits nodes: " + CTrie.traverse(ns2).size)

        println("digits " + name + " copying..")
        val tmp2 = File.createTempFile("catrie", "dfsd")
        println("digits copy" + tmp2)
        val s3 = openWriter(tmp2)
        val ns3 = openNodeWriter(s3)
        CTrie.copy(ns2, ns3)
        s3.close
        val s4 = openReader(tmp2)
        val ns4 = openNodeReader(s4)
        val ts2 = data2.map {
          case (k, v) =>
            val t1 = System.nanoTime
            CTrie.query(ns4, k) should equal(Some(v))
            (System.nanoTime - t1) / 1E9
        }.toVector
        println("digits " + name + " copy q 2q " + ts2.sorted.apply(ts.size / 2))
        println("digits " + name + "1q " + ts2.sorted.apply((ts.size * 0.25).toInt))
        println("digits " + name + "2q " + ts2.sorted.apply((ts.size * 0.75).toInt))
        println("digits " + name + "min " + ts2.min)
        println("digits " + name + "max " + ts2.zipWithIndex.maxBy(_._1))
        println("digits " + name + "  copied: " + tmp2.length / 1024 / 1024)
        println("digits " + name + "  nodes: " + CTrie.traverse(ns4).size)
        tmp2.delete
        tmp.delete
      }

      it("random " + name) {
        val buf = Array.ofDim[Byte](50)
        def data = {
          val rnd = new scala.util.Random(1)

          (0 to dataSize iterator).map(i => 0 to 16 map (j => rnd.nextPrintableChar) mkString).zipWithIndex
            .map(x => x._1.getBytes("US-ASCII") -> x._2.toLong)
        }
        val tmp = File.createTempFile("catrie", "dfsd")
        println("random " + tmp)
        val s = openWriter(tmp)

        val ns = openNodeWriter(s)
        val t1 = System.nanoTime
        CTrie.build(data, ns)
        ns match {
          case x: BufferedCNodeReader =>
            println(x.inmemory2.size)
          case x => ()
        }
        s.close
        println("random " + name + " build time " + (System.nanoTime - t1) / 1E9)
        println("level stat" + ns.counter.zipWithIndex.filter(_._1 > 0).sortBy(_._1).toList)
        println("hit rate " + ns.hitcount.toDouble / (ns.hitmiss + ns.hitcount))
        println("mean insert ns " + ns.inserttimer.toDouble / ns.insertcount)
        val s2 = openReader(tmp)
        val ns2 = openNodeReader(s2)

        {
          val ts = data.map {
            case (k, v) =>
              val t1 = System.nanoTime
              CTrie.query(ns2, k) should equal(Some(v))
              (System.nanoTime - t1) / 1E9
          }.toVector
          println("random " + name + " 2q " + ts.sorted.apply(ts.size / 2))
          println("random " + name + " 1q" + ts.sorted.apply((ts.size * 0.25).toInt))
          println("random " + name + " 3q" + ts.sorted.apply((ts.size * 0.75).toInt))
          println("random " + name + " min" + ts.min)
          println("random " + name + " max" + ts.zipWithIndex.maxBy(_._1))
        }

        {
          val ts = data.map {
            case (k, v) =>
              val t1 = System.nanoTime
              CTrie.query(ns2, k) should equal(Some(v))
              (System.nanoTime - t1) / 1E9
          }.toVector
          println("random " + name + " 2q " + ts.sorted.apply(ts.size / 2))
          println("random " + name + " 1q" + ts.sorted.apply((ts.size * 0.25).toInt))
          println("random " + name + " 3q" + ts.sorted.apply((ts.size * 0.75).toInt))
          println("random " + name + " min" + ts.min)
          println("random " + name + " max" + ts.zipWithIndex.maxBy(_._1))
        }
        CTrie.query(ns2, "abc".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns2, "c".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns2, "aaa".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns2, "bba".getBytes("US-ASCII")) should equal(None)
        CTrie.query(ns2, "bbb".getBytes("US-ASCII")) should equal(None)
        println("random file size:" + tmp.length / 1024 / 1024)
        println("random " + name + " random nodes: " + CTrie.traverse(ns2).size)

        println("random " + name + " copying..")
        val tmp2 = File.createTempFile("catrie", "dfsd")
        println("random " + tmp2)
        val s3 = openWriter(tmp2)
        val ns3 = openNodeWriter(s3)
        val ct1 = System.nanoTime
        CTrie.copy(ns2, ns3)
        s3.close
        println("random " + "copy took: " + (System.nanoTime - ct1) / 1E9)
        val s4 = openReader(tmp2)
        val ns4 = openNodeReader(s4)
        val ts2 = data.map {
          case (k, v) =>
            val t1 = System.nanoTime
            CTrie.prefixPayload(ns4, k) should equal(Vector(v))
            (System.nanoTime - t1) / 1E9
        }.toVector
        println("random " + name + " 2q" + ts2.sorted.apply(ts2.size / 2))
        println("random " + name + " 1q" + ts2.sorted.apply((ts2.size * 0.25).toInt))
        println("random " + name + " 3q" + ts2.sorted.apply((ts2.size * 0.75).toInt))
        println("random " + name + " min" + ts2.min)
        println("random " + name + " max" + ts2.zipWithIndex.maxBy(_._1))
        println("random " + name + " random copied: " + tmp2.length / 1024 / 1024)
        println("random " + name + " random nodes: " + CTrie.traverse(ns4).size)
        tmp2.delete
        tmp.delete
      }
    }
  }

  // bigTest(
  //   (f: File) => FileWriter.open(f),
  //   (f: File) => FileReader.open(f),
  //   "nio"
  // )
  //

  // bigTest(
  //   100000,
  //   (f: File) => FileWriter.open(f),
  //   (f: File) => FileReader.open(f),
  //   "nio-caH",
  //   (s: Writer) => new CAHNodeWriter(s),
  //   (s: Reader) => new CAHNodeReader(s)
  // )
  //
  val inmemory = new InMemoryStorage
  bigTest(
    100000,
    (f: File) => inmemory,
    (f: File) => inmemory,
    "inmemory-caH",
    (s: Writer) => new CAHNodeWriter(s),
    (s: Reader) => new CAHNodeReader(s)
  )

  bigTest(
    1000000,
    (f: File) => FileWriter.open(f),
    (f: File) => FileReader.open(f),
    "buffered-caH",
    (s: Writer) => {
      new BufferedCNodeWriter(new CAHNodeWriter(s), 3)
    },
    (s: Reader) => {

      new BufferedCNodeReader(new CAHNodeReader(s), 3)
    }
  )
}
