
package trie
import java.nio.{ ByteBuffer, ByteOrder }
import scala.collection.mutable.ArrayBuffer
// node: |record size,4|payload,8|hash table pointer,8|prefix|
// hash: (b+128)/8. -128..127 -> 0..31
// hash table: |16*8 hash buckets for positive, 128|2nd half of hash table for negative, 8|
// hash bucket: |pointers,8*8|

class CAHNodeReader(backing: Reader) extends CNodeReader {
  val bufInt = Array.fill[Byte](4)(0)
  val bufLong = Array.fill[Byte](8)(0)
  var prefixBuffer = Array.ofDim[Byte](1000)

  def readPointersFromHashTable(address: Long): ArrayBuffer[(Byte, Long)] = {
    if (address < 0) ArrayBuffer()
    else {
      val ar = new ArrayBuffer[(Byte, Long)](16)
      var i = 0
      while (i < 16) {
        val bucket = backing.readLong(address + i * 8)
        if (bucket >= 0) {
          var j = 0
          while (j < 8) {
            val ad = backing.readLong(bucket + j * 8)
            if (ad >= 0) {
              ar.append((i * 8 + j).toByte -> ad)
            }
            j += 1
          }
        }
        i += 1
      }
      ar
    }
  }

  def readPointer(i: Long, b: Byte): Long = {
    if (i < 0) -1L
    else {
      if (b < 0) readPointer(backing.readLong(i + 128), (b + 128).toByte)
      else {
        val h = b / 8
        val bucket = backing.readLong(i + h * 8)
        if (bucket < 0) -1L
        else backing.readLong(bucket + (b % 8) * 8)
      }
    }
  }

  def readPointers(address: Long): ArrayBuffer[(Byte, Long)] = {
    if (address < 0) ArrayBuffer()
    else {
      val children = readPointersFromHashTable(address)
      val negatives = backing.readLong(address + 128)
      if (negatives >= 0) {
        children ++ readPointers(negatives).map(x => (x._1 - 128).toByte -> x._2)
      } else children
    }
  }

  def read(i: Long): Option[CNode] = {
    if (backing.size >= i + 4) {
      val recordSize = backing.readInt(i)
      val payload = backing.readLong(i + 4)
      val children = readPointers(backing.readLong(i + 4 + 8))

      val prefix = backing.readBytes(i + 4 + 8 + 8, recordSize - 8 - 8)
      Some(CNode(i, children, payload, prefix))
    } else None
  }

  def readAddress(i: Long, b: Byte): Long = {
    readPointer(backing.readLong(i + 4 + 8), b)
  }

  def readPayload(i: Long): Long =
    backing.readLong(i + 4)

  def readPartial(i: Long, buffer: Array[Byte], offset: Int): (Long, Array[Byte], Int) =
    {
      val recordSize = backing.readInt(i)
      val bufferSize = recordSize - 16
      val buf2 = if (buffer.size >= offset + bufferSize) buffer
      else {
        val ar = Array.ofDim[Byte]((offset + bufferSize) * 2)
        System.arraycopy(buffer, 0, ar, 0, offset)
        ar
      }
      backing.readBytesInto(i + 4 + 8 + 8, buf2, offset, bufferSize)
      (i, buf2, offset + bufferSize)
    }
}

class CAHNodeWriter(backing: Writer) extends CAHNodeReader(backing) with CNodeWriter {

  val empty = Array.fill[Byte](17 * 8)(0)
  val bb = ByteBuffer.wrap(empty).order(ByteOrder.LITTLE_ENDIAN)
  0 until 17 foreach { i => bb.putLong(-1L) }

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
    val size = n.prefix.size + 4 + 8 + 8
    val ar = Array.ofDim[Byte](size)
    val bb = ByteBuffer.wrap(ar).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(size - 4)
    bb.putLong(n.payload)
    val ch = ArrayBuffer(n.children.toSeq: _*)
    val hashTable = if (ch.isEmpty) -1L else math.max(n.address + size, backing.size)
    bb.putLong(hashTable)
    n.prefix.foreach(b => bb.put(b))
    backing.set(n.address, ar)
    if (!ch.isEmpty) {
      val (positives, negatives) = ch.partition(_._1 >= 0)
      writeHashTable(positives, hashTable)
      val negativeHashTable = if (negatives.isEmpty) -1L else backing.size
      backing.writeLong(negativeHashTable, hashTable + 128)
      if (!negatives.isEmpty) {
        writeHashTable(negatives.map(x => (x._1 + 128).toByte -> x._2), negativeHashTable)
      }
    }
    // assert(read(n.address).get.children.toList == n.children.toList, read(n.address).get.children.toString + " " + n.children)
    // assert(read(n.address).get.prefix.deep == n.prefix.deep)
  }
  def updatePayload(old: CNode, n: Long) = {
    backing.writeLong(n, old.address + 4)
  }
  def updateRoute(old: Long, b: Byte, a: Long) = {
    def update(b: Byte, a: Long, idx: Long, updateAddress: Long): Unit = {
      if (idx < 0) {
        val nidx = backing.size
        backing.writeLong(nidx, updateAddress)
        if (b >= 0) writeHashTable(ArrayBuffer(b -> a), nidx)
        else {
          writeHashTable(ArrayBuffer(), nidx)
          val nidx2 = backing.size
          backing.writeLong(nidx2, nidx + 128)
          writeHashTable(ArrayBuffer((b + 128).toByte -> a), nidx2)
        }
      } else {
        if (b < 0) {
          update((b + 128).toByte, a, backing.readLong(idx + 128), idx + 128)
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
    }
    update(b, a, backing.readLong(old + 4 + 8), old + 4 + 8)
  }
  def append(n: CNode) = {
    val m = n.copy(address = backing.size)
    write(m)
    m
  }
}
