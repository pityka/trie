
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
  val tableBuffer = Array.ofDim[Byte](128)
  val pointerBuffer = Array.ofDim[Byte](64)
  val nodeSmallBuffer = Array.ofDim[Byte](19)
  val buf128 = Array.ofDim[Byte](128)

  val ShortNode = 0.toByte
  val HashNode = 1.toByte

  def readPointersFromHashTable(address: Long): ArrayBuffer[(Byte, Long)] = {
    if (address < 0) ArrayBuffer()
    else {
      val ar = new scala.collection.immutable.VectorBuilder[(Byte, Long)]()
      var i = 0
      backing.readBytesInto(address, tableBuffer, 0, tableBuffer.size)
      val bb = ByteBuffer.wrap(tableBuffer).order(ByteOrder.LITTLE_ENDIAN)
      while (i < 16) {
        val bucket = bb.getLong
        if (bucket >= 0) {
          backing.readBytesInto(bucket, pointerBuffer, 0, pointerBuffer.size)
          val bb = ByteBuffer.wrap(pointerBuffer).order(ByteOrder.LITTLE_ENDIAN)
          var j = 0
          while (j < 8) {
            val ad = bb.getLong
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
      // val t2 = (System.nanoTime - t1)
      // try {
      //   val ar = Array.ofDim[Byte](64)
      //   val t3 = System.nanoTime
      //   backing.readBytesInto(i, ar, 0, ar.size)
      //   val t4 = System.nanoTime - t3
      //   val t5a = System.nanoTime
      //   backing.readLong(i)
      //   val t5 = System.nanoTime - t5a
      //   val t6a = System.nanoTime
      //   backing.readLong(i + 8)
      //   val t6 = System.nanoTime - t6a
      //   val t7a = System.nanoTime
      //   backing.readLong(i + 16)
      //   val t7 = System.nanoTime - t7a
      //   val t8a = System.nanoTime
      //   backing.readLong(i + 120)
      //   val t8 = System.nanoTime - t8a
      //   println((t2, t4, t5, t6, t7, t8))
      // } catch {
      //   case e: Exception => ()
      // }
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

  def read(i: Long, l: Int): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i)
      val ar = backing.readBytes(i + 4, recordSize)
      val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
      val payload = bb.getLong
      val tpe = bb.get
      val children: ArrayBuffer[(Byte, Long)] = if (tpe == ShortNode) {
        val b1 = bb.get
        val b2 = bb.get
        val p1 = bb.getLong
        val p2 = bb.getLong
        if (p1 < 0 && p2 < 0) ArrayBuffer()
        else if (p1 < 0) ArrayBuffer(b2 -> p2)
        else if (p2 < 0) ArrayBuffer(b1 -> p1)
        else ArrayBuffer(b1 -> p1, b2 -> p2)
      } else {
        val _b1 = bb.get
        val _b2 = bb.get
        val positives = readPointers(bb.getLong)
        val negatives = readPointers(bb.getLong)
          .map(x => (x._1 - 128).toByte -> x._2)
        positives ++ negatives
      }

      val prefix = Array.ofDim[Byte](recordSize - 8 - 8 - 8 - 3)
      bb.get(prefix, 0, prefix.size)
      Some(CNode(i, children, payload, prefix))
    } else None
  }

  def readAddress(i: Long, b: Byte, l: Int): Long = {
    backing.readBytesInto(i + 4 + 8, nodeSmallBuffer, 0, nodeSmallBuffer.size)
    val bb = ByteBuffer.wrap(nodeSmallBuffer).order(ByteOrder.LITTLE_ENDIAN)
    if (bb.get == HashNode) {
      bb.get //discard
      bb.get //discard
      if (b >= 0) {
        readPointer(bb.getLong, b)
      } else {
        bb.getLong // discard
        readPointer(bb.getLong, (b + 128).toByte)
      }
    } else {
      val b1 = bb.get
      val b2 = bb.get
      if (b1 == b) bb.getLong
      else if (b2 == b) {
        bb.getLong //discard
        bb.getLong
      } else -1L
    }
  }

  def readPayload(i: Long, l: Int): Long =
    backing.readLong(i + 4)

  def readPartial(i: Long, buffer: Array[Byte], offset: Int, l: Int): (Array[Byte], Int) =
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
      (buf2, offset + bufferSize)
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
          val bb = ByteBuffer.wrap(pointerBuffer).order(ByteOrder.LITTLE_ENDIAN)
          while (i < 8) {
            if (b % 8 == i) bb.putLong(l)
            else bb.putLong(-1L)
            i += 1
          }
          backing.set(newaddress, pointerBuffer)
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
          val bb = ByteBuffer.wrap(pointerBuffer).order(ByteOrder.LITTLE_ENDIAN)
          while (i < 8) {
            if (b % 8 == i) bb.putLong(a)
            else bb.putLong(-1L)
            i += 1
          }
          backing.set(nidx, pointerBuffer)
        }
      }
    }
    // val tpe = backing.readByte(old + 4 + 8)
    backing.readBytesInto(old + 4 + 8, nodeSmallBuffer, 0, nodeSmallBuffer.size)
    val bb = ByteBuffer.wrap(nodeSmallBuffer).order(ByteOrder.LITTLE_ENDIAN)
    val tpe = bb.get
    val b1 = bb.get
    val b2 = bb.get
    val p1 = bb.getLong
    val p2 = bb.getLong

    if (tpe == HashNode) {
      if (b >= 0) updateHashTable(b, a, p1, old + 4 + 8 + 3)
      else updateHashTable((b + 128).toByte, a, p2, old + 4 + 8 + 3 + 8)
    } else {

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
  def append(n: CNode, level_unused: Int) = {
    val m = n.copy(address = backing.size)
    write(m)
    m
  }
}
