// package trie
//
// case class Node(
//     address: Long,
//     children: IndexedSeq[Long],
//     payload: Long
// ) {
//   assert(children.size == 256)
// }
// object Node {
//   def empty = Node(-1L, Array.fill(256)(-1L), -1L)
//   def apply(children: IndexedSeq[Long], payload: Long): Node = Node(-1L, children, payload)
// }
//
// class NodeReader(backing: Reader) {
//   val bufInt = Array.fill[Byte](4)(0)
//   val bufLongs = Array.fill[Byte](257 * 8)(0)
//
//   def read(i: Long): Option[Node] = {
//     if (backing.size >= i + 4) {
//       val recordSize = backing.readInt(i, bufInt)
//       val ar = Array.ofDim[Byte](recordSize)
//       backing.get(i + 4, ar)
//       val values = Conversions.fromArray(ar, 257)
//       val payload = values.head
//       val children = values.drop(1)
//       Some(Node(i, children.toArray, payload))
//     } else None
//   }
// }
//
// class NodeWriter(backing: Writer) extends NodeReader(backing) {
//   def write(n: Node) = {
//     val values = n.payload +: n.children
//     val raw = Conversions.toArray(values, bufLongs)
//     backing.writeInt(raw.size, n.address, bufInt)
//     backing.set(n.address + 4, raw)
//
//   }
//   def append(n: Node) = {
//     val m = n.copy(address = backing.size)
//     write(m)
//     m
//   }
// }
//
// object Trie {
//   def build(data: Iterator[(Seq[Byte], Long)], storage: NodeWriter): Unit = {
//     data.foreach {
//       case (key, value) =>
//         val root = storage.read(0).getOrElse {
//           val n = Node.empty
//           storage.append(n)
//         }
//         val lastNode = key.foldLeft(root) {
//           case (node, byte) =>
//             val nextAddr: Long = node.children(byte)
//             if (nextAddr == -1) {
//               val n = storage.append(Node.empty)
//               val m = node.copy(children = node.children.updated(byte, n.address))
//               storage.write(m)
//               n
//             } else storage.read(nextAddr).get
//         }
//         val nodeWithValue = lastNode.copy(payload = value)
//         if (nodeWithValue.address == -1) storage.append(nodeWithValue)
//         else storage.write(nodeWithValue)
//     }
//   }
//   def query(trie: NodeReader, q: Seq[Byte]): Option[Long] = {
//     def loop(node: Node, q: List[Byte]): Option[Node] = q match {
//       case Nil => Some(node)
//       case x :: xs => {
//         val nextAddr: Long = node.children(x)
//         if (nextAddr == -1) None
//         else {
//           val n = trie.read(nextAddr).get
//           loop(n, xs)
//         }
//       }
//     }
//     trie.read(0).flatMap { root =>
//       loop(root, q.toList)
//     }.map(_.payload).flatMap(x => if (x == -1L) None else Some(x))
//
//   }
// }
