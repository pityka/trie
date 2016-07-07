package trie
import java.nio.{ ByteBuffer, ByteOrder }

// |recordsize,4|value,8|pointers|prefix|
// pointers: |size,1|elems,16|pointers,16*8|next,8| = 153
class CANodeReader(backing: Reader) extends CNodeReader {
  val bufInt = Array.fill[Byte](4)(0)
  val bufLong = Array.fill[Byte](8)(0)
  val GroupSize = 8
  val PointerRecordSize = 1 + GroupSize + 8 * GroupSize + 8

  def readPointers1(bb: ByteBuffer): Seq[(Byte, Long)] = {
    val elemSize = bb.get.toInt
    val elems: Seq[Byte] = 0 until GroupSize map (i => bb.get)
    val pointers = 0 until GroupSize map (i => bb.getLong)
    elems zip pointers take (elemSize)
  }

  def readPointers(bb: ByteBuffer): Stream[Seq[(Byte, Long)]] = {
    val children = readPointers1(bb)
    val next = bb.getLong
    if (next >= 0) {
      val ar = Array.ofDim[Byte](PointerRecordSize)
      backing.get(next, ar)
      val bb2 = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
      children #:: readPointers(bb2)
    } else Stream(children)
  }

  def read(i: Long): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i, bufInt)
      val ar = Array.ofDim[Byte](recordSize)
      backing.get(i + 4, ar)
      val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
      val payload = bb.getLong
      val children = {
        val pointers: Stream[Seq[(Byte, Long)]] = readPointers(bb)
        val ar = Array.fill(256)(-1L)
        pointers.flatten.foreach {
          case (b, l) =>
            ar(b) = l
        }
        ar
      }

      val prefix = ar.slice(8 + PointerRecordSize, ar.size)
      Some(CNode(i, children, payload, prefix))
    } else None
  }

  def readAddress(i: Long, b: Byte): Long = {
    val ar = Array.ofDim[Byte](PointerRecordSize)
    backing.get(i + 4 + 8, ar)
    val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
    val pointers: Stream[Seq[(Byte, Long)]] = readPointers(bb)
    pointers.find(_.exists(_._1 == b)).flatMap(_.find(_._1 == b).map(_._2)).getOrElse(-1L)
  }

  def readPartial(i: Long): PartialCNode = {
    val recordSize = backing.readInt(i, bufInt)
    val ar = Array.ofDim[Byte](recordSize - PointerRecordSize - 8)
    backing.get(i + 4 + 8 + PointerRecordSize, ar)
    PartialCNode(i, ar.toSeq)
  }
}

class CANodeWriter(backing: Writer) extends CANodeReader(backing) with CNodeWriter {

  def writePointers(bb: ByteBuffer, pointers: Seq[(Byte, Long)]): Unit = {
    val (first16, rest) = pointers.splitAt(GroupSize)
    bb.put(first16.size.toByte)
    0 until GroupSize foreach { i =>
      if (first16.size > i) bb.put(first16(i)._1) else bb.put(0.toByte)
    }
    0 until GroupSize foreach { i =>
      if (first16.size > i) bb.putLong(first16(i)._2) else bb.putLong(-1L)
    }
    if (rest.isEmpty) bb.putLong(-1L)
    else {
      val next = backing.size
      bb.putLong(next)
      val ar = Array.ofDim[Byte](PointerRecordSize)
      val bb2 = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
      writePointers(bb2, rest)
      backing.set(next, ar)
    }

  }

  def write(n: CNode) = {
    val size = n.prefix.size + 4 + 8 + PointerRecordSize
    val ar = Array.ofDim[Byte](size)
    val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(size - 4)
    bb.putLong(n.payload)
    writePointers(bb, n.children.zipWithIndex.filter(_._1 >= 0).map(x => x._2.toByte -> x._1))
    n.prefix.foreach(b => bb.put(b))
    backing.set(n.address, ar)
  }
  def updatePayload(old: CNode, n: Long) = {
    backing.writeLong(n, old.address + 4, bufLong)
  }
  def updateRoute(old: CNode, b: Byte, a: Long) = {
    def update(b: Byte, a: Long, idx: Long): Unit = {
      val ar = Array.ofDim[Byte](PointerRecordSize)
      backing.get(idx, ar)
      val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
      val elems = readPointers1(bb)
      val next = bb.getLong
      if (elems.map(_._1).contains(b)) {
        val idx2 = elems.zipWithIndex.find(_._1._1 == b).map(_._2).get
        backing.writeLong(a, idx + 1 + GroupSize + idx2 * 8, bufLong)
      } else {
        if (elems.size == GroupSize) {
          if (next >= 0) update(b, a, next)
          else {
            val nidx = backing.size
            val ar = Array.ofDim[Byte](PointerRecordSize)
            val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
            writePointers(bb, List(b -> a))
            backing.set(nidx, ar)
            backing.writeLong(nidx, idx + 1 + GroupSize + GroupSize * 8, bufLong)
            // update(b, a, backing.size)
          }
        } else {
          val size = elems.size
          backing.set(idx, Array((size + 1).toByte))
          backing.set(idx + 1 + elems.size, Array(b))
          backing.writeLong(a, idx + 1 + GroupSize + elems.size * 8, bufLong)
        }
      }
    }
    update(b, a, old.address + 4 + 8)
  }
  def append(n: CNode) = {
    val m = n.copy(address = backing.size)
    write(m)
    m
  }
}
