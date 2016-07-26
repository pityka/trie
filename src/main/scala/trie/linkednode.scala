package trie
import java.nio.{ ByteBuffer, ByteOrder }
import scala.collection.mutable.ArrayBuffer
// |recordsize,4|value,8|next pointer,8|prefix|
// pointers: |size,1|elems,8|pointers,8*8|next,8|
class CANodeReader(backing: Reader) extends CNodeReader {
  val bufInt = Array.fill[Byte](4)(0)
  val bufLong = Array.fill[Byte](8)(0)
  val GroupSize = 8
  val PointerRecordSize = 1 + GroupSize + 8 * GroupSize + 8
  val elemsBuffer: Array[Byte] = Array.ofDim[Byte](GroupSize)
  val elemsLongBuffer = Array.ofDim[Long](GroupSize)
  val prBuffer = Array.ofDim[Byte](PointerRecordSize)
  val pointerRecordBuffer = Array.ofDim[Byte](PointerRecordSize)
  var prefixBuffer = Array.ofDim[Byte](1000)

  def readPointers1(address: Long): Array[(Byte, Long)] = {
    if (address < 0) Array()
    else {
      val elemSize = backing.readByte(address)
      val r = Array.ofDim[(Byte, Long)](elemSize)
      var i = 0
      while (i < r.size) {
        r(i) = backing.readByte(address + 1 + i) -> backing.readLong(address + 1 + GroupSize + i * 8)
        i += 1
      }
      r
    }
  }
  def readPointer1(address: Long, b: Long): Long = {
    val elemSize = backing.readByte(address)
    val idx = {
      var i = 0
      var stop = false
      while (i < elemSize && !stop) {
        stop = b == backing.readByte(address + 1 + i)
        i += 1
      }
      if (stop) i - 1 else -1
    }
    if (idx < 0) -1L
    else {
      backing.readLong(address + 1 + GroupSize + idx * 8)
    }
  }

  def readPointer(i: Long, b: Byte): Long = {
    if (i < 0) -1L
    else {
      val l = readPointer1(i, b)
      val next = backing.readLong(i + PointerRecordSize - 8) //bb.getLong
      if (l >= 0) l
      else if (next < 0) -1L
      else readPointer(next, b)
    }
  }

  def readPointers(address: Long): Stream[Array[(Byte, Long)]] = {
    if (address < 0) Nil.toStream
    else {
      val children = readPointers1(address)
      val next = backing.readLong(address + PointerRecordSize - 8)
      if (next >= 0) {
        children #:: readPointers(next)
      } else Stream(children)
    }
  }

  val m1 = Array.fill(256)(-1L)
  def read(i: Long, l: Int): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i)
      val payload = backing.readLong(i + 4)
      val children = ArrayBuffer(readPointers(backing.readLong(i + 4 + 8)).flatten: _*)

      val prefix = backing.readBytes(i + 4 + 8 + 8, recordSize - 8 - 8)
      Some(CNode(i, children, payload, prefix))
    } else None
  }

  def readAddress(i: Long, b: Byte, l: Int): Long = {
    readPointer(backing.readLong(i + 4 + 8), b)
  }

  def readPayload(i: Long, l: Int): Long =
    backing.readLong(i + 4)

  def readPartial(i: Long, buffer: Array[Byte], offset: Int, l: Int): (Array[Byte], Int) =
    {
      val recordSize = backing.readInt(i)
      val bufferSize = recordSize - 16
      val buf2 = if (buffer.size >= offset + bufferSize) buffer
      else {
        val ar = Array.ofDim[Byte]((offset + bufferSize) * 2)
        System.arraycopy(buffer, 0, ar, 0, offset)
        ar
      }
      if (bufferSize > 0) {
        backing.readBytesInto(i + 4 + 8 + 8, buf2, offset, bufferSize)
      }
      (buf2, offset + bufferSize)
    }
}

class CANodeWriter(backing: Writer) extends CANodeReader(backing) with CNodeWriter {

  def writePointers(bb: ByteBuffer, start: Int, pointers: ArrayBuffer[(Byte, Long)], address: Long): Unit = {

    val s = if (pointers.size - start > GroupSize) GroupSize else pointers.size - start
    bb.put(s.toByte)
    0 until GroupSize foreach { i =>
      if (s > i) bb.put(pointers(i + start)._1) else bb.put(0.toByte)
    }
    0 until GroupSize foreach { i =>
      if (s > i) bb.putLong(pointers(i + start)._2) else bb.putLong(-1L)
    }
    if (pointers.size - start - s <= 0) {
      bb.putLong(-1L)
      backing.set(address, pointerRecordBuffer)
    } else {
      val next = math.max(backing.size, address + PointerRecordSize)
      bb.putLong(next)
      backing.set(address, pointerRecordBuffer)
      val bb2 = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
      writePointers(bb2, start + s, pointers, next)
    }

  }

  def write(n: CNode) = {
    val size = n.prefix.size + 4 + 8 + 8
    val ar = Array.ofDim[Byte](size)
    val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(size - 4)
    bb.putLong(n.payload)
    val ch = ArrayBuffer(n.children.toSeq: _*)
    val nextPointerBlock = if (ch.isEmpty) -1L else math.max(n.address + size, backing.size)
    bb.putLong(nextPointerBlock)
    n.prefix.foreach(b => bb.put(b))
    backing.set(n.address, ar)
    if (!ch.isEmpty) {
      val bb2 = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
      writePointers(bb2, 0, ch, nextPointerBlock)
    }
    // assert(read(n.address).get.children == n.children)
    // assert(read(n.address).get.prefix.deep == n.prefix.deep)
  }
  def updatePayload(old: Long, n: Long) = {
    backing.writeLong(n, old + 4)
  }
  def updateRoute(old: Long, b: Byte, a: Long) = {
    def update(b: Byte, a: Long, idx: Long, updateAddress: Long): Unit = {
      val elems = readPointers1(idx)

      if (elems.map(_._1).contains(b)) {
        val idx2 = elems.zipWithIndex.find(_._1._1 == b).map(_._2).get
        backing.writeLong(a, idx + 1 + GroupSize + idx2 * 8)
      } else {
        if (elems.size == GroupSize) {
          val next = backing.readLong(idx + PointerRecordSize - 8)
          if (next >= 0) update(b, a, next, idx + 1 + GroupSize + GroupSize * 8)
          else {
            val nidx = backing.size
            val bb = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
            writePointers(bb, 0, ArrayBuffer(b -> a), nidx)
            backing.set(nidx, pointerRecordBuffer)
            backing.writeLong(nidx, idx + 1 + GroupSize + GroupSize * 8)
          }
        } else {
          if (idx >= 0) {
            val size = elems.size
            backing.set(idx, Array((size + 1).toByte))
            backing.set(idx + 1 + elems.size, Array(b))
            backing.writeLong(a, idx + 1 + GroupSize + elems.size * 8)
          } else {
            val nidx = backing.size
            val bb = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
            writePointers(bb, 0, ArrayBuffer(b -> a), nidx)
            backing.set(nidx, pointerRecordBuffer)
            backing.writeLong(nidx, updateAddress)

          }
        }
      }
    }

    update(b, a, backing.readLong(old + 4 + 8), old + 4 + 8)
  }
  def append(n: CNode, level_unused: Int) = {
    val m = n.copy(address = backing.size)
    write(m)
    m
  }
}
