package trie

import scala.collection.mutable.ArrayBuffer
import java.nio.{ ByteBuffer, ByteOrder }

// object Conversions {
//   def fromArray(buf: Array[Byte], len: Int): Seq[Long] = {
//     val bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
//     0 until len map { i =>
//       bb.getLong
//     }
//   }
//
//   def toArray(l: Seq[Long], buf: Array[Byte]) = {
//     val bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
//     l.foreach(l => bb.putLong(l))
//     buf
//   }
// }

// object ConversionsSnappy {
//   def fromArray(buf: Array[Byte], len: Int): Seq[Long] = {
//     val uncompressed = Snappy.uncompress(buf)
//     val bb = ByteBuffer.wrap(uncompressed).order(ByteOrder.LITTLE_ENDIAN)
//     0 until len map { i =>
//       bb.getLong
//     }
//   }
//
//   def toArray(l: Seq[Long], buf: Array[Byte]) = {
//     val bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
//     l.foreach(l => bb.putLong(l))
//     Snappy.compress(buf)
//   }
// }

trait Reader {
  def get(i: Long, buf: Array[Byte]): Unit
  def size: Long

  def readLongs(idx: Long, buf: Array[Byte]): Seq[Long] = {
    get(idx, buf)
    val bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
    0 until (buf.size / 8) map { i =>
      bb.getLong
    }
  }

  def readInt(idx: Long, buf: Array[Byte]) = {
    get(idx, buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def readLong(idx: Long, buf: Array[Byte]) = {
    get(idx, buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

}

trait Writer extends Reader {
  def set(i: Long, v: Array[Byte]): Unit

  def writeLongs(l: Seq[Long], idx: Long, buf: Array[Byte]) = {
    val bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
    l.foreach(l => bb.putLong(l))
    set(idx, buf)
  }

  def writeInt(l: Int, idx: Long, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putInt(l)
    set(idx, buf)
  }

  def writeLong(l: Long, idx: Long, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(l)
    set(idx, buf)
  }

}

class InMemoryStorage extends Reader with Writer {
  val backing = ArrayBuffer[Byte]()
  var size = 0L
  def set(i: Long, v: Array[Byte]) = {
    val ii = i.toInt
    if (backing.size <= ii + v.size) {
      backing.appendAll(ArrayBuffer.fill[Byte](ii - backing.size + v.size)(0))
    }
    v.zipWithIndex.foreach {
      case (b, i) =>
        backing(ii + i) = b
    }
    size = math.max(i.toInt + v.size, size)
  }
  def get(i: Long, buf: Array[Byte]) = {
    val tmp = backing.slice(i.toInt, buf.size + i.toInt).toArray
    System.arraycopy(tmp, 0, buf, 0, tmp.size)
  }
  override def toString = backing.toString
}
