package trie

case class CNode(
    address: Long,
    children: IndexedSeq[Long],
    payload: Long,
    prefix: Vector[Byte]
) {
  assert(children.size == 256)
}

object CNode {
  def empty = CNode(-1L, Array.fill(256)(-1L), -1L, Vector())
  def apply(children: IndexedSeq[Long], payload: Long, prefix: Vector[Byte]): CNode = CNode(-1L, children, payload, prefix)
}

// case class PartialCNode(
//   address: Long,
//   prefix: Array[Byte]
// )

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
  def updateRoute(old: CNode, b: Byte, a: Long): Unit
  def append(n: CNode): CNode
}

object CTrie {

  def sharedPrefix(a: List[Byte], b: List[Byte], acc: Vector[Byte]): Vector[Byte] =
    if (a.isEmpty || b.isEmpty || a.head != b.head) acc.reverse
    else sharedPrefix(a.tail, b.tail, a.head +: acc)

  def sharedPrefix(p: Array[Byte], start: Int, len: Int, b: Vector[Byte], acc: List[Byte], i: Int): List[Byte] =
    if (i + start >= len || b.isEmpty || p(i + start) != b.head) acc.reverse
    else sharedPrefix(p, start, len, b.tail, b.head :: acc, i + 1)

  def build(data: Iterator[(Vector[Byte], Long)], storage: CNodeWriter): Unit = {
    val root = storage.read(0).getOrElse {
      val n = CNode.empty
      storage.append(n)
    }
    data.foreach {
      case (key, value) =>
        queryNode(storage, key) match {
          case Right((lastNode, _)) => storage.updatePayload(lastNode, value)
          case Left((lastNode, prefix)) => {
            if (key.startsWith(prefix)) {
              val rest = key.drop(prefix.size)
              val n = CNode(Array.fill(256)(-1L), value, rest)
              val n1 = storage.append(n)
              storage.updateRoute(lastNode, rest.head, n1.address)
            } else {

              val sharedP = sharedPrefix(key.toList, prefix.toList, Vector())
              val keyRest = key.drop(sharedP.size)
              val lastNodePrefixRest = prefix.drop(sharedP.size)
              val lastNodePrefixUsed = lastNode.prefix.take(lastNode.prefix.size - lastNodePrefixRest.size)

              val m = CNode(lastNode.children, lastNode.payload, lastNodePrefixRest)
              val m1 = storage.append(m)

              if (keyRest.isEmpty) {
                val routes = Array.fill(256)(-1L)
                  .updated(lastNodePrefixRest.head, m1.address)

                storage.write(lastNode.copy(children = routes, prefix = lastNodePrefixUsed, payload = value))
              } else {
                val n = CNode(Array.fill(256)(-1L), value, keyRest)
                val n1 = storage.append(n)

                val routes = Array.fill(256)(-1L)
                  .updated(keyRest.head, n1.address)
                  .updated(lastNodePrefixRest.head, m1.address)

                storage.write(lastNode.copy(children = routes, prefix = lastNodePrefixUsed, payload = (-1L)))
              }

            }
          }
        }
    }
  }

  def startsWith(q: Vector[Byte], b: Array[Byte], start: Int, len: Int, i: Int): Boolean =
    if (q.isEmpty) false
    else {
      if (i + start < len) {
        val h = q.head
        val t = q.tail
        val v = b(i + start)
        if (h == v) startsWith(t, b, start, len, i + 1)
        else false
      } else true
    }

  def prefixPayload(trie: CNodeReader, q: Vector[Byte]): Vector[Long] = {
    val first = queryNode(trie, q)
    first match {
      case Left((node, p)) =>
        if (p.startsWith(q)) node.payload +: node.children.filter(_ >= 0).map(i => childrenPayload(trie, i)).flatten.toVector
        else Vector()
      case Right((node, pl)) => node.payload +: node.children.filter(_ >= 0).map(i => childrenPayload(trie, i)).flatten.toVector
    }
  }

  def childrenPayload(trie: CNodeReader, n: Long): Vector[Long] = {
    val node = trie.read(n)
    if (node.isDefined) {
      node.get.payload +: node.get.children.filter(_ >= 0).map(i => childrenPayload(trie, i)).flatten.toVector
    } else Vector()

  }

  def loop(trie: CNodeReader, node: Long, p: Array[Byte], start: Int, off: Int, q: Vector[Byte]): (Long, Array[Byte], Int) = {
    q match {
      case Vector() => {
        (node, p, off)
      }
      case xxs => {
        val sharedP = sharedPrefix(p, start, off, q, Nil, 0)
        if (sharedP.size == q.size) {
          (node, p, off)
        } else {
          val nextAddr: Long = trie.readAddress(node, q.drop(sharedP.size).head)
          if (nextAddr == -1) {
            (node, p, off)
          } else {
            val (n, p2, off2) = trie.readPartial(nextAddr, p, off)
            val tail = q.drop(sharedP.size)
            if (startsWith(tail, p2, off, off2, 0)) {
              loop(trie, n, p2, off, off2, tail)
            } else {
              (n, p2, off2)
            }
          }
        }
      }
    }
  }

  def queryNode(trie: CNodeReader, q: Vector[Byte]): Either[(CNode, Vector[Byte]), (CNode, Long)] = {

    val (root, p2, off2) = trie.readPartial(0, trie.prefixBuffer, 0)
    val (lastNodeAddress, prefix, prefixLen) = loop(trie, root, p2, 0, off2, q)
    val lastNode = trie.read(lastNodeAddress).get

    val lastNodePrefix = prefix.slice(0, prefixLen).toVector
    if (lastNodePrefix == q) Right(lastNode -> lastNode.payload)
    else {
      Left((lastNode, lastNodePrefix))
    }

  }

  def query(trie: CNodeReader, q: Vector[Byte]): Option[Long] = {

    val (root, p2, off2) = trie.readPartial(0, trie.prefixBuffer, 0)
    val (lastNodeAddress, prefix, prefixLen) = loop(trie, root, p2, 0, off2, q)

    val lastNodePrefix = prefix.slice(0, prefixLen).toVector
    if (lastNodePrefix == q) {
      val pl = trie.readPayload(lastNodeAddress)
      if (pl >= 0) Some(pl)
      else None
    } else None

  }

}
