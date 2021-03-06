package trie

import scala.collection.mutable.ArrayBuffer
import java.nio.{ ByteBuffer, ByteOrder }

trait Reader {

  def size: Long
  def readInt(idx: Long): Int
  def readLong(idx: Long): Long
  def readByte(idx: Long): Byte
  def readBytes(idx: Long, size: Int): Array[Byte]
  def readBytesInto(i: Long, buf: Array[Byte], start: Int, len: Int)
}

trait Writer extends Reader {
  def set(i: Long, v: Array[Byte]): Unit
  def close: Unit
  def writeLong(l: Long, idx: Long)
  def writeByte(b: Byte, idx: Long)
}

trait JReader extends Reader {

  def get(i: Long): Byte
  def get(i: Long, buf: Array[Byte], start: Int, len: Int)
  def get(i: Long, buf: Array[Byte]): Unit = get(i, buf, 0, buf.size)

  def size: Long

  val intBuffer = Array.ofDim[Byte](4)
  val longBuffer = Array.ofDim[Byte](8)

  def readBytes(idx: Long, size: Int) = {
    val ar = Array.ofDim[Byte](size)
    get(idx, ar)
    ar
  }

  def readInt(idx: Long) = {
    get(idx, intBuffer)
    ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def readByte(idx: Long) = get(idx)

  def readBytesInto(i: Long, buf: Array[Byte], start: Int, len: Int) = get(i, buf, start, len)

  def readLong(idx: Long) = {
    get(idx, longBuffer)
    ByteBuffer.wrap(longBuffer).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

}

trait JWriter extends JReader with Writer {
  def set(i: Long, v: Array[Byte]): Unit

  def close: Unit

  def writeLong(l: Long, idx: Long) = {
    val ar = ByteBuffer.wrap(longBuffer).order(ByteOrder.LITTLE_ENDIAN).putLong(l)
    set(idx, longBuffer)
  }

  def writeByte(b: Byte, idx: Long) = set(idx, Array(b))

}

class InMemoryStorage extends JReader with JWriter {
  val backing = ArrayBuffer[Byte]()
  var size = 0L
  def set(i: Long, v: Array[Byte]) = {
    val ii = i.toInt
    if (backing.size <= ii + v.size) {
      backing.appendAll(ArrayBuffer.fill[Byte](ii - backing.size + v.size)(0))
    }
    var j = 0
    while (j < v.size) {
      backing(i.toInt + j) = v(j)
      j += 1
    }
    size = math.max(i.toInt + v.size, size)
  }
  def get(i: Long): Byte = backing(i.toInt)
  def get(i: Long, buf: Array[Byte], s: Int, l: Int) = {
    var j = 0
    val ii = i.toInt
    while (j < l) {
      buf(s + j) = backing(ii + j)
      j += 1
    }
  }
  override def toString = backing.toString
  def close = ()
}
