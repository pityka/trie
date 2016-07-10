package trie
import java.nio.{ ByteBuffer, ByteOrder }
import scala.collection.mutable.ArrayBuffer
// |recordsize,4|value,8|pointers|prefix|
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

  def readPointers1(bb: ByteBuffer): Array[(Byte, Long)] = {
    val elemSize = bb.get.toInt
    bb.get(elemsBuffer)

    bb.asLongBuffer.get(elemsLongBuffer)
    bb.position(bb.position + GroupSize * 8)
    val r = Array.ofDim[(Byte, Long)](elemSize)
    var i = 0
    while (i < r.size) {
      r(i) = elemsBuffer(i) -> elemsLongBuffer(i)
      i += 1
    }
    r
  }
  def readPointer1(bb: ByteBuffer, b: Long): Long = {
    val elemSize = bb.get.toInt

    bb.get(elemsBuffer, 0, elemSize)
    val idx = {
      var i = 0
      var stop = false
      while (i < elemSize && !stop) {
        stop = b == elemsBuffer(i)
        i += 1
      }
      if (stop) i - 1 else -1
    }
    bb.position(bb.position + GroupSize - elemSize)
    val ret = if (idx < 0) -1L
    else {
      val lb = bb.asLongBuffer
      lb.get(idx)
    }

    bb.position(bb.position + GroupSize * 8)
    ret
  }

  def readPointer(bb: ByteBuffer, b: Byte): Long = {
    val l = readPointer1(bb, b)
    val next = bb.getLong
    if (l >= 0) l
    else if (next < 0) -1L
    else {
      backing.get(next, pointerRecordBuffer)
      val bb2 = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
      readPointer(bb2, b)
    }
  }

  def readPointers(bb: ByteBuffer): Stream[Array[(Byte, Long)]] = {
    val children = readPointers1(bb)
    val next = bb.getLong
    if (next >= 0) {
      backing.get(next, pointerRecordBuffer)
      val bb2 = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
      children #:: readPointers(bb2)
    } else Stream(children)
  }
  val m1 = Array.fill(256)(-1L)
  def read(i: Long): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i)
      val ar = Array.ofDim[Byte](recordSize)
      backing.get(i + 4, ar)
      val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
      val payload = bb.getLong
      val children = {
        val pointers: Stream[Array[(Byte, Long)]] = readPointers(bb)
        val ar = java.util.Arrays.copyOf(m1, m1.length)
        pointers.foreach {
          _ foreach {
            case (b, l) =>
              ar(b) = l
          }
        }
        ar
      }

      val prefix = ar.slice(8 + PointerRecordSize, ar.size)
      Some(CNode(i, children, payload, prefix.toVector))
    } else None
  }

  def readAddress(i: Long, b: Byte): Long = {

    backing.get(i + 4 + 8, prBuffer)
    val bb = ByteBuffer.wrap(prBuffer).order(ByteOrder.LITTLE_ENDIAN)
    readPointer(bb, b)
  }

  def readPayload(i: Long): Long = {
    backing.readLong(i + 4)
  }

  def readPartial(i: Long, buffer: Array[Byte], offset: Int): (Long, Array[Byte], Int) =
    {
      val recordSize = backing.readInt(i)
      val bufferSize = recordSize - PointerRecordSize - 8
      val buf2 = if (buffer.size >= offset + bufferSize) buffer
      else {
        val ar = Array.ofDim[Byte]((offset + bufferSize) * 2)
        System.arraycopy(buffer, 0, ar, 0, offset)
        ar
      }
      backing.get(i + 4 + 8 + PointerRecordSize, buf2, offset, bufferSize)
      (i, buf2, offset + bufferSize)
    }
}

class CANodeWriter(backing: Writer) extends CANodeReader(backing) with CNodeWriter {

  def writePointers(bb: ByteBuffer, start: Int, pointers: ArrayBuffer[(Byte, Long)]): Unit = {
    // val (first16, rest) = pointers.splitAt(GroupSize)
    val s = if (pointers.size - start > GroupSize) GroupSize else pointers.size - start
    bb.put(s.toByte)
    0 until GroupSize foreach { i =>
      if (s > i) bb.put(pointers(i + start)._1) else bb.put(0.toByte)
    }
    0 until GroupSize foreach { i =>
      if (s > i) bb.putLong(pointers(i + start)._2) else bb.putLong(-1L)
    }
    if (pointers.size - start - s <= 0) bb.putLong(-1L)
    else {
      val next = backing.size
      bb.putLong(next)
      val bb2 = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
      writePointers(bb2, start + s, pointers)
      backing.set(next, pointerRecordBuffer)
    }

  }

  def write(n: CNode) = {
    val size = n.prefix.size + 4 + 8 + PointerRecordSize
    val ar = Array.ofDim[Byte](size)
    val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(size - 4)
    bb.putLong(n.payload)
    val ch = {
      val buf = scala.collection.mutable.ArrayBuffer[(Byte, Long)]()
      var i = 0
      while (i < 256) {
        val v = n.children(i)
        if (v >= 0) {
          buf.append(i.toByte -> v)
        }
        i += 1
      }
      buf
    }
    writePointers(bb, 0, ch)
    n.prefix.foreach(b => bb.put(b))
    backing.set(n.address, ar)
  }
  def updatePayload(old: CNode, n: Long) = {
    backing.writeLong(n, old.address + 4)
  }
  def updateRoute(old: CNode, b: Byte, a: Long) = {
    def update(b: Byte, a: Long, idx: Long): Unit = {
      backing.get(idx, pointerRecordBuffer)
      val bb = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
      val elems = readPointers1(bb)
      val next = bb.getLong
      if (elems.map(_._1).contains(b)) {
        val idx2 = elems.zipWithIndex.find(_._1._1 == b).map(_._2).get
        backing.writeLong(a, idx + 1 + GroupSize + idx2 * 8)
      } else {
        if (elems.size == GroupSize) {
          if (next >= 0) update(b, a, next)
          else {
            val nidx = backing.size
            val bb = ByteBuffer.wrap(pointerRecordBuffer).order(ByteOrder.LITTLE_ENDIAN)
            writePointers(bb, 0, ArrayBuffer(b -> a))
            backing.set(nidx, pointerRecordBuffer)
            backing.writeLong(nidx, idx + 1 + GroupSize + GroupSize * 8)
          }
        } else {
          val size = elems.size
          backing.set(idx, Array((size + 1).toByte))
          backing.set(idx + 1 + elems.size, Array(b))
          backing.writeLong(a, idx + 1 + GroupSize + elems.size * 8)
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
