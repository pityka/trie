// package trie
// /**
// * main record: |size,4|payload,8|children batch|prefix|
// * children batch: |list of 4 children,36(8x9)|next children batch address,8| = 44
// */
// class CANodeReader(backing: Reader) {
//   val bufInt = Array.fill[Byte](4)(0)
//   val bufLongs = Array.fill[Byte](257 * 8)(0)
//
//   def read(i: Long): Option[CNode] = {
//     if (backing.size >= i + 4) {
//       val recordSize = backing.readInt(i, bufInt)
//       val ar = Array.ofDim[Byte](recordSize)
//       backing.get(i + 4, ar)
//
//
//
//       Some(CNode(i, children.toArray, payload, prefix))
//     } else None
//   }
// }
//
// class CANodeWriter(backing: Writer) extends CNodeReader(backing) {
//   def write(n: CNode) = {
//     val values = n.payload +: n.children
//     val raw = Conversions.toArray(values, bufLongs)
//     backing.writeInt(raw.size + n.prefix.size, n.address, bufInt)
//     backing.set(n.address + 4, raw)
//     backing.set(n.address + 4 + raw.size, n.prefix.toArray)
//
//   }
//   def append(n: CNode) = {
//     val m = n.copy(address = backing.size)
//     write(m)
//     m
//   }
// }
