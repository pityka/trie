
package trie
import java.nio.{ ByteBuffer, ByteOrder }
import scala.collection.mutable.ArrayBuffer
// node: |record size,4|payload,8|type,1|route1,1|route2,1|positive hash table pointer or route1,8|negative hash table pointer or route2 |prefix|
// hash: (b+128)/8. -128..127 -> 0..31
// hash table: |16*8 hash buckets for positive, 128|
// hash bucket: |pointers,8*8|

class CAHNodeReader(backing: Reader) extends CNodeReader {
  val bufInt = Array.fill[Byte](4)(0)
  val bufLong = Array.fill[Byte](8)(0)
  var prefixBuffer = Array.ofDim[Byte](1000)

  val ShortNode = 0.toByte
  val HashNode = 1.toByte

  def readPointersFromHashTable(address: Long): ArrayBuffer[(Byte, Long)] = {
    if (address < 0) ArrayBuffer()
    else {
      val ar = new scala.collection.immutable.VectorBuilder[(Byte, Long)]()
      var i = 0
      while (i < 16) {
        val bucket = backing.readLong(address + i * 8)
        if (bucket >= 0) {
          var j = 0
          while (j < 8) {
            val ad = backing.readLong(bucket + j * 8)
            if (ad >= 0) {
              ar += ((i * 8 + j).toByte -> ad)
            }
            j += 1
          }
        }
        i += 1
      }
      ArrayBuffer(ar.result: _*)
    }
  }

  def readPointer(i: Long, b: Byte): Long = {
    if (i < 0) -1L
    else {

      val h = b / 8
      // val t1 = System.nanoTime
      val bucket = backing.readLong(i + h * 8)
      // println(System.nanoTime - t1)
      if (bucket < 0) -1L
      else backing.readLong(bucket + (b % 8) * 8)

    }
  }

  def readPointers(address: Long): ArrayBuffer[(Byte, Long)] = {
    if (address < 0) ArrayBuffer()
    else {
      val children = readPointersFromHashTable(address)
      children
    }
  }

  def read(i: Long): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i)
      val payload = backing.readLong(i + 4)
      val tpe = backing.readByte(i + 12)
      val children: ArrayBuffer[(Byte, Long)] = if (tpe == ShortNode) {
        val b1 = backing.readByte(i + 4 + 8 + 1)
        val b2 = backing.readByte(i + 4 + 8 + 2)
        val p1 = backing.readLong(i + 4 + 8 + 3)
        val p2 = backing.readLong(i + 4 + 8 + 3 + 8)
        if (p1 < 0 && p2 < 0) ArrayBuffer()
        else if (p1 < 0) ArrayBuffer(b2 -> p2)
        else if (p2 < 0) ArrayBuffer(b1 -> p1)
        else ArrayBuffer(b1 -> p1, b2 -> p2)
      } else {
        val positives = readPointers(backing.readLong(i + 4 + 8 + 3))
        val negatives = readPointers(backing.readLong(i + 4 + 8 + 3 + 8))
          .map(x => (x._1 - 128).toByte -> x._2)
        positives ++ negatives
      }

      val prefix = backing.readBytes(i + 4 + 8 + 3 + 8 + 8, recordSize - 8 - 8 - 8 - 3)
      Some(CNode(i, children, payload, prefix))
    } else None
  }

  def readAddress(i: Long, b: Byte): Long = {
    if (backing.readByte(i + 4 + 8) == HashNode) {
      if (b >= 0) readPointer(backing.readLong(i + 4 + 8 + 3), b)
      else readPointer(backing.readLong(i + 4 + 8 + 3 + 8), (b + 128).toByte)
    } else {
      val b1 = backing.readByte(i + 4 + 8 + 1)
      val b2 = backing.readByte(i + 4 + 8 + 2)
      if (b1 == b) backing.readLong(i + 4 + 8 + 3)
      else if (b2 == b) backing.readLong(i + 4 + 8 + 3 + 8)
      else -1L
    }
  }

  def readPayload(i: Long): Long =
    backing.readLong(i + 4)

  def readPartial(i: Long, buffer: Array[Byte], offset: Int): (Long, Array[Byte], Int) =
    {
      val recordSize = backing.readInt(i)
      val bufferSize = recordSize - 8 - 8 - 8 - 3
      val buf2 = if (buffer.size >= offset + bufferSize) buffer
      else {
        val ar = Array.ofDim[Byte]((offset + bufferSize) * 2)
        System.arraycopy(buffer, 0, ar, 0, offset)
        ar
      }
      backing.readBytesInto(i + 4 + 8 + 3 + 8 + 8, buf2, offset, bufferSize)
      (i, buf2, offset + bufferSize)
    }
}

class CAHNodeWriter(backing: Writer) extends CAHNodeReader(backing) with CNodeWriter {

  val empty = Array.fill[Byte](16 * 8)(0)
  val bb = ByteBuffer.wrap(empty).order(ByteOrder.LITTLE_ENDIAN)
  0 until 16 foreach { i => bb.putLong(-1L) }

  def writeHashTable(pointers: ArrayBuffer[(Byte, Long)], address: Long): Unit = {

    backing.set(address, empty)

    pointers.foreach {
      case (b, l) =>
        val bucket = backing.readLong(address + (b / 8) * 8)
        if (bucket == -1L) {
          val newaddress = backing.size
          var i = 0
          while (i < 8) {
            if (b % 8 == i) backing.writeLong(l, newaddress + i * 8)
            else backing.writeLong(-1L, newaddress + i * 8)
            i += 1
          }
          backing.writeLong(newaddress, address + (b / 8) * 8)
        } else {
          backing.writeLong(l, bucket + (b % 8) * 8)
        }
    }

  }

  def write(n: CNode) = {
    val size = n.prefix.size + 4 + 8 + 8 + 8 + 3
    val ar = Array.ofDim[Byte](size)
    val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(size - 4)
    bb.putLong(n.payload)
    if (n.children.size <= 2) {
      bb.put(ShortNode)
      if (n.children.size > 0) {
        bb.put(n.children(0)._1)
      } else bb.put(0.toByte)
      if (n.children.size > 1) {
        bb.put(n.children(1)._1)
      } else bb.put(0.toByte)

      if (n.children.size > 0) {
        bb.putLong(n.children(0)._2)
      } else bb.putLong(-1L)
      if (n.children.size > 1) {
        bb.putLong(n.children(1)._2)
      } else bb.putLong(-1L)

    } else {
      bb.put(HashNode)
      bb.put(0.toByte)
      bb.put(0.toByte)
      val (pos, neg) = n.children.partition(_._1 >= 0)

      val hashTablePositive = if (pos.isEmpty) -1L else {
        val ad = math.max(n.address + size, backing.size)
        writeHashTable(pos, ad)
        ad
      }
      val hashTableNegative = if (neg.isEmpty) -1L else {
        val ad = math.max(n.address + size, backing.size)
        writeHashTable(neg.map(x => (x._1 + 128).toByte -> x._2), ad)
        ad
      }
      bb.putLong(hashTablePositive)
      bb.putLong(hashTableNegative)
    }

    n.prefix.foreach(b => bb.put(b))
    backing.set(n.address, ar)

    // assert(read(n.address).get.children.toList == n.children.toList, read(n.address).get.children.toString + " " + n.children)
    // assert(read(n.address).get.prefix.deep == n.prefix.deep)
  }
  def updatePayload(old: Long, n: Long) = {
    backing.writeLong(n, old + 4)
  }
  def updateRoute(old: Long, b: Byte, a: Long) = {
    def updateHashTable(b: Byte, a: Long, idx: Long, updateAddress: Long): Unit = {
      if (idx < 0) {
        val nidx = backing.size
        backing.writeLong(nidx, updateAddress)
        writeHashTable(ArrayBuffer(b -> a), nidx)
      } else {
        val bucket = backing.readLong(idx + (b / 8) * 8)

        if (bucket >= 0) {
          backing.writeLong(a, bucket + (b % 8) * 8)
        } else {
          val nidx = backing.size
          backing.writeLong(nidx, idx + (b / 8) * 8)
          var i = 0
          while (i < 8) {
            if (b % 8 == i) backing.writeLong(a, nidx + i * 8)
            else backing.writeLong(-1L, nidx + i * 8)
            i += 1
          }
        }
      }
    }
    val tpe = backing.readByte(old + 4 + 8)
    if (tpe == HashNode) {
      if (b >= 0) updateHashTable(b, a, backing.readLong(old + 4 + 8 + 3), old + 4 + 8 + 3)
      else updateHashTable((b + 128).toByte, a, backing.readLong(old + 4 + 8 + 3 + 8), old + 4 + 8 + 3 + 8)
    } else {
      val b1 = backing.readByte(old + 4 + 8 + 1)
      val b2 = backing.readByte(old + 4 + 8 + 2)
      val p1 = backing.readLong(old + 4 + 8 + 3)
      val p2 = backing.readLong(old + 4 + 8 + 3 + 8)
      if (b1 == b) backing.writeLong(a, old + 4 + 8 + 3)
      else if (b2 == b) backing.writeLong(a, old + 4 + 8 + 3 + 8)
      else if (p1 == -1L) {
        backing.writeByte(b, old + 4 + 8 + 1)
        backing.writeLong(a, old + 4 + 8 + 3)
      } else if (p2 == -1L) {
        backing.writeByte(b, old + 4 + 8 + 2)
        backing.writeLong(a, old + 4 + 8 + 3 + 8)
      } else {
        backing.writeByte(HashNode, old + 4 + 8)
        backing.writeByte(0, old + 4 + 8 + 1)
        backing.writeByte(0, old + 4 + 8 + 2)
        val children = ArrayBuffer(b1 -> p1, b2 -> p2, b -> a)
        val (pos, neg) = children.partition(_._1 >= 0)

        if (pos.isEmpty) {
          backing.writeLong(-1L, old + 4 + 8 + 3)
        } else {
          val ad = backing.size
          writeHashTable(pos, ad)
          backing.writeLong(ad, old + 4 + 8 + 3)
        }
        if (neg.isEmpty) {
          backing.writeLong(-1L, old + 4 + 8 + 3 + 8)
        } else {
          val ad = backing.size
          writeHashTable(neg.map(x => (x._1 + 128).toByte -> x._2), ad)
          backing.writeLong(ad, old + 4 + 8 + 3 + 8)
        }
      }
    }

  }
  def append(n: CNode) = {
    val m = n.copy(address = backing.size)
    write(m)
    m
  }
}
