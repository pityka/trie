package trie
import scala.collection.mutable.ArrayBuffer

case class CNode(
    address: Long,
    children: ArrayBuffer[(Byte, Long)],
    payload: Long,
    prefix: Array[Byte]
) {
  override def toString = "CNode(" + address + "," + children + "," + payload + "," + new String(prefix.map(_.toChar)) + ")"
}

object CNode {
  def empty = CNode(-1L, ArrayBuffer(), -1L, Array())
  def apply(children: ArrayBuffer[(Byte, Long)], payload: Long, prefix: Array[Byte]): CNode = CNode(-1L, children, payload, prefix)
}

trait CNodeReader {
  def read(i: Long, l: Int): Option[CNode]
  def readAddress(i: Long, b: Byte, l: Int): Long
  def readPayload(i: Long, l: Int): Long
  def readPartial(i: Long, buffer: Array[Byte], offset: Int, l: Int): (Array[Byte], Int)

  var prefixBuffer: Array[Byte] = Array.ofDim[Byte](1000)

  def read(i: Long): Option[CNode] = read(i, 0)
  def readAddress(i: Long, b: Byte): Long = readAddress(i, b, 0)
  def readPayload(i: Long): Long = readPayload(i, 0)
  def readPartial(i: Long, buffer: Array[Byte], offset: Int): (Array[Byte], Int) =
    readPartial(i, buffer, offset, 0)

  var counter: Array[Int] = Array.fill(1000)(0)
  var hitcount = 0
  var hitmiss = 0
}

trait CNodeWriter extends CNodeReader {
  def write(n: CNode): Unit
  def updatePayload(old: Long, n: Long): Unit
  def updateRoute(old: Long, b: Byte, a: Long): Unit
  def append(n: CNode, l: Int): CNode

  var inserttimer = 0L
  var insertcount = 0L
}

object CTrie {

  def sharedPrefix(a: Array[Byte], b: Array[Byte], i: Int): Int = sharedPrefix(a, 0, a.size, b, i)

  def sharedPrefix(p: Array[Byte], start: Int, len: Int, b: Array[Byte], i: Int): Int =
    if (i + start >= len || b.size <= i || p(i + start) != b(i)) i
    else sharedPrefix(p, start, len, b, i + 1)

  def startsWith(q: Array[Byte], b: Array[Byte], start: Int, len: Int, i: Int): Boolean =
    if (q.size <= i) false
    else {
      if (i + start < len) {
        val h = q(i)
        val v = b(i + start)
        if (h == v) startsWith(q, b, start, len, i + 1)
        else false
      } else true
    }

  def drop(key: Array[Byte], v: Int) =
    {
      val ar = Array.ofDim[Byte](key.size - v)
      System.arraycopy(key, v, ar, 0, ar.size)
      ar
    }

  def take(key: Array[Byte], v: Int) =
    {
      val ar = Array.ofDim[Byte](v)
      System.arraycopy(key, 0, ar, 0, ar.size)
      ar
    }

  def insert(key: Array[Byte], value: Long, storage: CNodeWriter, root: CNode): Unit = {
    storage.insertcount += 1
    val t1 = System.nanoTime
    val r = queryNode(storage, key) match {
      case Right((lastNodeAddress, level, _)) => storage.updatePayload(lastNodeAddress, value)
      case Left((lastNodeAddress, level, prefix)) => {
        if (startsWith(key, prefix, 0, prefix.size, 0)) {

          val rest = drop(key, prefix.size)
          val n = CNode(ArrayBuffer(), value, rest)
          val n1 = storage.append(n, level + 1)

          storage.updateRoute(lastNodeAddress, rest(0), n1.address)
        } else {
          val lastNode = storage.read(lastNodeAddress, level).get

          val sharedP = sharedPrefix(key, prefix, 0)
          val keyRest = drop(key, sharedP)
          val lastNodePrefixRest = drop(prefix, sharedP)
          val lastNodePrefixUsed = take(lastNode.prefix, lastNode.prefix.size - lastNodePrefixRest.size)

          val m = CNode(lastNode.children, lastNode.payload, lastNodePrefixRest)
          val m1 = storage.append(m, level)

          if (keyRest.isEmpty) {
            val routes = ArrayBuffer(lastNodePrefixRest.head -> m1.address)

            storage.write(lastNode.copy(children = routes, prefix = lastNodePrefixUsed, payload = value))
          } else {
            val n = CNode(ArrayBuffer(), value, keyRest)
            val n1 = storage.append(n, level + 1)

            val routes = ArrayBuffer(
              keyRest.head -> n1.address,
              lastNodePrefixRest.head -> m1.address
            )

            storage.write(lastNode.copy(children = routes, prefix = lastNodePrefixUsed, payload = (-1L)))
          }

        }
      }
    }
    storage.inserttimer += (System.nanoTime - t1)
    // assert(query(storage, key).get == value)
    r

  }

  def build(data: Iterator[(Array[Byte], Long)], storage: CNodeWriter): Unit = {
    val root = storage.read(0, 0).getOrElse {
      val n = CNode.empty
      storage.append(n, 0)
    }
    data.foreach {
      case (key, value) =>
        insert(key, value, storage, root)
    }
  }

  def prefixPayload(trie: CNodeReader, q: Array[Byte]): Vector[Long] = {
    val first = queryNode(trie, q)
    first match {
      case Left((nodeAddress, level, p)) => {
        if (p.startsWith(q)) {
          val node = trie.read(nodeAddress, level).get
          node.payload +: node.children.map(i => childrenPayload(trie, i._2, level + 1)).flatten.toVector
        } else Vector()
      }
      case Right((nodeAddress, level, pl)) => {
        val node = trie.read(nodeAddress, level).get
        node.payload +: node.children.map(i => childrenPayload(trie, i._2, level + 1)).flatten.toVector
      }
    }
  }

  def childrenPayload(trie: CNodeReader, n: Long, level: Int): Vector[Long] = {
    val node = trie.read(n, level)
    if (node.isDefined) {
      node.get.payload +: node.get.children.map(i => childrenPayload(trie, i._2, level + 1)).flatten.toVector
    } else Vector()

  }

  def loop(trie: CNodeReader, node: Long, p: Array[Byte], start: Int, off: Int, q: Array[Byte], level: Int): (Long, Array[Byte], Int, Boolean, Int) = {
    if (q.size == 0) {
      (node, p, off, true, level)
    } else {
      val sharedP = sharedPrefix(p, start, off, q, 0)
      if (sharedP == q.size) {
        (node, p, off, true, level)
      } else {
        val nextAddr: Long = trie.readAddress(node, q(sharedP), level)
        if (nextAddr == -1) {
          (node, p, off, false, level)
        } else {
          val (p2, off2) = trie.readPartial(nextAddr, p, off, level)
          val tail = drop(q, sharedP)
          if (startsWith(tail, p2, off, off2, 0)) {
            loop(trie, nextAddr, p2, off, off2, tail, level + 1)
          } else {
            val shared2 = sharedPrefix(p2, off, off2, tail, 0)
            (nextAddr, p2, off2, shared2 == tail.size && shared2 == (off2 - off), level)
          }
        }
      }
    }

  }

  def queryNode(trie: CNodeReader, q: Array[Byte]): Either[(Long, Int, Array[Byte]), (Long, Int, Long)] = {

    val (p2, off2) = trie.readPartial(0, trie.prefixBuffer, 0, 0)
    val root = 0
    val (lastNodeAddress, prefix, prefixLen, succ, level) = loop(trie, root, p2, 0, off2, q, 0)
    trie.counter(level) += 1
    if (succ) Right((lastNodeAddress, level, trie.readPayload(lastNodeAddress, level)))
    else {
      Left((lastNodeAddress, level, prefix.slice(0, prefixLen)))
    }

  }

  def query(trie: CNodeReader, q: Array[Byte]): Option[Long] = {

    val (p2, off2) = trie.readPartial(0, trie.prefixBuffer, 0, 0)
    val root = 0
    val (lastNodeAddress, prefix, prefixLen, succ, level) = loop(trie, root, p2, 0, off2, q, 0)
    if (succ) {
      val pl = trie.readPayload(lastNodeAddress, level)
      if (pl >= 0) Some(pl)
      else None
    } else None

  }

  def traverse(trie: CNodeReader, queue: Vector[(CNode, Int)]): Stream[(CNode, Int)] = {
    if (queue.isEmpty) Stream()
    else {
      val level = queue.head._2 + 1
      val children = queue.head._1.children.map(l => trie.read(l._2, level).get -> level)
      queue.head #:: traverse(trie, queue.tail ++ children)
    }
  }

  def traverse(trie: CNodeReader): Stream[(CNode, Int)] = {
    val root = trie.read(0, 0).get
    traverse(trie, Vector(root -> 0))
  }

  def copy(stream: Iterator[(CNode, Int)], writer: CNodeWriter): Unit = {
    val mmap = scala.collection.mutable.LongMap[(Long, Byte)]()
    stream.foreach {
      case (node, level) =>
        val appended = writer.append(node, level)

        appended.children.foreach {
          case (char, ch) =>
            mmap.update(ch, appended.address -> char)
        }
        mmap.get(node.address).foreach {
          case (parent, char) =>
            writer.updateRoute(parent, char, appended.address)
        }
        mmap.remove(node.address)

    }
  }

  def copy(trie: CNodeReader, dst: CNodeWriter): Unit = copy(traverse(trie).iterator, dst)

}
