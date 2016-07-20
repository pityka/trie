package trie

case class CNode(
    address: Long,
    children: Map[Byte, Long],
    payload: Long,
    prefix: Array[Byte]
) {
  override def toString = "CNode(" + address + "," + children + "," + payload + "," + new String(prefix.map(_.toChar)) + ")"
}

object CNode {
  def empty = CNode(-1L, Map(), -1L, Array())
  def apply(children: Map[Byte, Long], payload: Long, prefix: Array[Byte]): CNode = CNode(-1L, children, payload, prefix)
}

trait CNodeReader {
  def read(i: Long): Option[CNode]
  def readAddress(i: Long, b: Byte): Long
  def readPayload(i: Long): Long
  def readPartial(i: Long, buffer: Array[Byte], offset: Int): (Long, Array[Byte], Int)
  var prefixBuffer: Array[Byte]
}

trait CNodeWriter extends CNodeReader {
  def write(n: CNode): Unit
  def updatePayload(old: CNode, n: Long): Unit
  def updateRoute(old: Long, b: Byte, a: Long): Unit
  def append(n: CNode): CNode
}

object CTrie {

  def sharedPrefix(a: Array[Byte], b: Array[Byte], acc: List[Byte], i: Int): List[Byte] = sharedPrefix(a, 0, a.size, b, acc, i)

  def sharedPrefix(p: Array[Byte], start: Int, len: Int, b: Array[Byte], acc: List[Byte], i: Int): List[Byte] =
    if (i + start >= len || b.size <= i || p(i + start) != b(i)) acc.reverse
    else sharedPrefix(p, start, len, b, b(i) :: acc, i + 1)

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
    queryNode(storage, key) match {
      case Right((lastNode, _)) => storage.updatePayload(lastNode, value)
      case Left((lastNode, prefix)) => {
        if (startsWith(key, prefix, 0, prefix.size, 0)) {
          val rest = drop(key, prefix.size)
          val n = CNode(Map(), value, rest)
          val n1 = storage.append(n)
          storage.updateRoute(lastNode.address, rest(0), n1.address)
        } else {

          val sharedP = sharedPrefix(key, prefix, List(), 0).toArray
          val keyRest = drop(key, sharedP.size)
          val lastNodePrefixRest = drop(prefix, sharedP.size)
          val lastNodePrefixUsed = take(lastNode.prefix, lastNode.prefix.size - lastNodePrefixRest.size)

          val m = CNode(lastNode.children, lastNode.payload, lastNodePrefixRest)
          val m1 = storage.append(m)

          if (keyRest.isEmpty) {
            val routes = Map(lastNodePrefixRest.head -> m1.address)

            storage.write(lastNode.copy(children = routes, prefix = lastNodePrefixUsed, payload = value))
          } else {
            val n = CNode(Map(), value, keyRest)
            val n1 = storage.append(n)

            val routes = Map(
              keyRest.head -> n1.address,
              lastNodePrefixRest.head -> m1.address
            )

            storage.write(lastNode.copy(children = routes, prefix = lastNodePrefixUsed, payload = (-1L)))
          }

        }
      }
    }
    // assert(query(storage, key).get == value)
  }

  def build(data: Iterator[(Array[Byte], Long)], storage: CNodeWriter): Unit = {
    val root = storage.read(0).getOrElse {
      val n = CNode.empty
      storage.append(n)
    }
    data.foreach {
      case (key, value) =>
        insert(key, value, storage, root)
    }
  }

  def prefixPayload(trie: CNodeReader, q: Array[Byte]): Vector[Long] = {
    val first = queryNode(trie, q)
    first match {
      case Left((node, p)) => {
        if (p.startsWith(q)) node.payload +: node.children.map(i => childrenPayload(trie, i._2)).flatten.toVector
        else Vector()
      }
      case Right((node, pl)) => node.payload +: node.children.map(i => childrenPayload(trie, i._2)).flatten.toVector
    }
  }

  def childrenPayload(trie: CNodeReader, n: Long): Vector[Long] = {
    val node = trie.read(n)
    if (node.isDefined) {
      node.get.payload +: node.get.children.map(i => childrenPayload(trie, i._2)).flatten.toVector
    } else Vector()

  }

  def loop(trie: CNodeReader, node: Long, p: Array[Byte], start: Int, off: Int, q: Array[Byte]): (Long, Array[Byte], Int, Boolean) = {
    if (q.size == 0) {
      (node, p, off, true)
    } else {
      val sharedP = sharedPrefix(p, start, off, q, Nil, 0)
      if (sharedP.size == q.size) {
        (node, p, off, true)
      } else {
        val nextAddr: Long = trie.readAddress(node, q(sharedP.size))
        if (nextAddr == -1) {
          (node, p, off, false)
        } else {
          val (n, p2, off2) = trie.readPartial(nextAddr, p, off)
          val tail = drop(q, sharedP.size)
          if (startsWith(tail, p2, off, off2, 0)) {
            loop(trie, n, p2, off, off2, tail)
          } else {
            val shared2 = sharedPrefix(p2, off, off2, tail, Nil, 0)
            (n, p2, off2, shared2.size == tail.size && shared2.size == (off2 - off))
          }
        }
      }
    }

  }

  def queryNode(trie: CNodeReader, q: Array[Byte]): Either[(CNode, Array[Byte]), (CNode, Long)] = {

    val (root, p2, off2) = trie.readPartial(0, trie.prefixBuffer, 0)
    val (lastNodeAddress, prefix, prefixLen, succ) = loop(trie, root, p2, 0, off2, q)
    val lastNode = trie.read(lastNodeAddress).get

    if (succ) Right(lastNode -> lastNode.payload)
    else {
      Left((lastNode, prefix.slice(0, prefixLen)))
    }

  }

  def query(trie: CNodeReader, q: Array[Byte]): Option[Long] = {

    val (root, p2, off2) = trie.readPartial(0, trie.prefixBuffer, 0)
    val (lastNodeAddress, prefix, prefixLen, succ) = loop(trie, root, p2, 0, off2, q)
    if (succ) {
      val pl = trie.readPayload(lastNodeAddress)
      if (pl >= 0) Some(pl)
      else None
    } else None

  }

  def traverse(trie: CNodeReader, queue: Vector[CNode]): Stream[CNode] = {
    if (queue.isEmpty) Stream()
    else {
      val children = queue.head.children.map(l => trie.read(l._2).get)
      queue.head #:: traverse(trie, queue.tail ++ children)
    }
  }

  def traverse(trie: CNodeReader): Stream[CNode] = {
    val root = trie.read(0).get
    traverse(trie, Vector(root))
  }

  def copy(stream: Iterator[CNode], writer: CNodeWriter): Unit = {
    val mmap = scala.collection.mutable.LongMap[(Long, Byte)]()
    stream.foreach { node =>
      val appended = writer.append(node)

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
