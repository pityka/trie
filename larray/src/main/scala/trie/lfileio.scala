package trie

import java.nio.{ ByteBuffer, ByteOrder }
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption._
import java.io.File
import xerial.larray._
import xerial.larray.mmap._

class LFileWriter(f: File) extends LFileReader(f) with Writer {
  def set(i: Long, v: Array[Byte]) = {
    remap(i, v.size)
    var j = 0
    while (j < v.size) {
      map.putByte((i - mapStart) + j, v(j))
      j += 1
    }
    if (i + v.size > fileSize) {
      fileSize = i + v.size
    }
  }
  def writeLong(l: Long, address: Long) = {
    remap(address, 8)
    map.putLong(address - mapStart, l)
    fileSize += 8
  }
  def close = {
    map.flush
    val fc = FileChannel.open(f.toPath, READ, WRITE)
    fc.truncate(size)
    fc.close
  }
  override def toString = "LFileWriter(" + f + ")"
}
class LFileReader(f: File) extends Reader {

  /* this is a mmap > 4gb */
  var map = new MMapBuffer(f, MMapMode.READ_WRITE)

  var mapStart = 0L
  var mapSize = f.length.toLong
  var fileSize = f.length
  def size = fileSize
  def remap(i: Long, size: Int): Unit = {
    if (i < mapStart || i + size > mapStart + mapSize) {
      val size2 = math.max(size, 1024 * 1024 * 100)
      val (mStart, mSize) =
        if (f.length - i > Long.MaxValue.toLong) (i, Long.MaxValue)
        else {
          val end = math.max(i + size2, f.length)
          val s = math.max(0, end - Long.MaxValue)

          (s, end - s)
        }
      // println(mStart -> mSize)
      map.flush
      map.close
      map = new MMapBuffer(f, mStart, mSize, MMapMode.READ_WRITE)
      mapStart = mStart
      mapSize = mSize
    }
  }
  def readBytesInto(i: Long, buf: Array[Byte], s: Int, l: Int) = {
    remap(i, l)
    var j = 0
    while (j < l) {
      buf(s + j) = map.getByte(i - mapStart + j)
      j += 1
    }
  }
  def readByte(i: Long) = {
    remap(i, 1)
    map.getByte(i - mapStart)
  }
  def readInt(idx: Long): Int = {
    remap(idx, 4)
    map.getInt(idx - mapStart)
  }
  def readLong(idx: Long): Long = {
    remap(idx, 8)
    map.getLong(idx - mapStart)
  }
  def readBytes(idx: Long, size: Int): Array[Byte] = {
    val ar = Array.ofDim[Byte](size)
    readBytesInto(idx, ar, 0, size)
    ar
  }
  override def toString = "LFileReader(" + f + ")"
}
