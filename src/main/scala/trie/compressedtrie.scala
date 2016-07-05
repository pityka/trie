package trie

case class CNode(
    address: Long,
    children: IndexedSeq[Long],
    payload: Long,
    prefix: Seq[Byte]
) {
  assert(children.size == 256)
}

object CNode {
  def empty = CNode(-1L, Array.fill(256)(-1L), -1L, Nil)
  def apply(children: IndexedSeq[Long], payload: Long, prefix: Seq[Byte]): CNode = CNode(-1L, children, payload, prefix)
}

case class PartialCNode(
  address: Long,
  prefix: Seq[Byte]
)

trait CNodeReader {
  def read(i: Long): Option[CNode]
  def readAddress(i: Long, b: Byte): Long
  def readPartial(i: Long): PartialCNode
}

trait CNodeWriter extends CNodeReader {
  def write(n: CNode): Unit
  def updatePayload(old: CNode, n: Long): Unit
  def updateRoute(old: CNode, b: Byte, a: Long): Unit
  def append(n: CNode): CNode
}

class SimpleCNodeReader(backing: Reader) extends CNodeReader {
  val bufInt = Array.fill[Byte](4)(0)
  val bufLong = Array.fill[Byte](8)(0)
  val bufLongs = Array.fill[Byte](257 * 8)(0)
  val bufLongs2 = Array.fill[Byte](256 * 8)(0)

  def read(i: Long): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i, bufInt)
      val ar = Array.ofDim[Byte](recordSize)
      backing.get(i + 4, ar)
      val values = Conversions.fromArray(ar, 257)
      val payload = values.head
      val children = values.drop(1)
      val prefix = ar.slice(257 * 8, ar.size)
      Some(CNode(i, children.toArray, payload, prefix))
    } else None
  }

  def readAddress(i: Long, b: Byte): Long = {
    backing.readLong(i + 4 + 8 + b.toInt * 8, bufLong)
  }

  def readPartial(i: Long): PartialCNode = {
    val recordSize = backing.readInt(i, bufInt)
    val ar = Array.ofDim[Byte](recordSize - 257 * 8)
    backing.get(i + 4 + 257 * 8, ar)
    PartialCNode(i, ar.toSeq)
  }
}

class SimpleCNodeWriter(backing: Writer) extends SimpleCNodeReader(backing)
    with CNodeWriter {
  def write(n: CNode) = {
    val values = n.payload +: n.children
    val raw = Conversions.toArray(values, bufLongs)
    backing.writeInt(raw.size + n.prefix.size, n.address, bufInt)
    backing.set(n.address + 4, raw)
    backing.set(n.address + 4 + raw.size, n.prefix.toArray)
  }
  def updatePayload(old: CNode, n: Long) = {
    backing.writeLong(n, old.address + 4, bufLong)
  }
  def updateRoute(old: CNode, b: Byte, a: Long) = {
    backing.writeLong(a, old.address + 4 + 8 + b.toInt * 8, bufLong)
  }
  def append(n: CNode) = {
    val m = n.copy(address = backing.size)
    write(m)
    m
  }
}

object CTrie {

  def sharedPrefix(a: List[Byte], b: List[Byte], acc: List[Byte]): List[Byte] =
    if (a.isEmpty || b.isEmpty || a.head != b.head) acc.reverse
    else sharedPrefix(a.tail, b.tail, a.head :: acc)

  def build(data: Iterator[(Seq[Byte], Long)], storage: CNodeWriter): Unit = {
    data.foreach {
      case (key, value) =>
        val root = storage.read(0).getOrElse {
          val n = CNode.empty
          storage.append(n)
        }
        queryNode(storage, key) match {
          case Right((lastNode, _)) => storage.updatePayload(lastNode, value)
          case Left((lastNode, prefix)) => {
            if (key.startsWith(prefix)) {
              val rest = key.drop(prefix.size)
              val n = CNode(Array.fill(256)(-1L), value, rest)
              val n1 = storage.append(n)
              storage.updateRoute(lastNode, rest.head, n1.address)
            } else {

              val sharedP = sharedPrefix(key.toList, prefix.toList, Nil)
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
  def queryNode(trie: CNodeReader, q: Seq[Byte]): Either[(CNode, Seq[Byte]), (CNode, Long)] = {
    def loop(node: PartialCNode, p: Seq[Byte], q: List[Byte]): (PartialCNode, Seq[Byte]) = q match {
      case Nil => {
        node -> p
      }
      case xxs => {
        val sharedP = sharedPrefix(node.prefix.toList, q.toList, Nil)
        if (sharedP.size == q.size) (node, p)
        else {
          val nextAddr: Long = trie.readAddress(node.address, q.drop(sharedP.size).head) //node.children(q.drop(sharedP.size).head)
          if (nextAddr == -1) {
            (node, p)
          } else {
            val n = trie.readPartial(nextAddr)
            val tail = q.drop(sharedP.size)
            if (tail.startsWith(n.prefix))
              loop(n, p ++ n.prefix, tail)
            else {
              (n, p ++ n.prefix)
            }
          }
        }
      }
    }

    val root = trie.readPartial(0)
    val (lastNodePartial, lastNodePrefix) = loop(root, Seq(), q.toList)
    val lastNode = trie.read(lastNodePartial.address).get

    if (lastNodePrefix == q) Right(lastNode -> lastNode.payload)
    else {
      Left((lastNode, lastNodePrefix))
    }

  }
  def query(trie: CNodeReader, q: Seq[Byte]) =
    queryNode(trie, q).right.toOption.map(_._2).flatMap(x => if (x == -1) None else Some(x))
}
