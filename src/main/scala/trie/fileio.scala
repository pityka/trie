package trie

import java.nio.{ ByteBuffer, ByteOrder }
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption._
import java.io.File

object FileWriter {
  def open(f: File) = new FileWriter(FileChannel.open(f.toPath, READ, WRITE))
}

class FileWriter(fc: FileChannel) extends FileReader(fc) with JWriter {
  def set(i: Long, v: Array[Byte]) = {
    remap(i, v.size)

    map.position((i - mapStart).toInt)
    map.put(v)
    if (i + v.size > fileSize) {
      fileSize = i + v.size
    }
  }
  def close = {
    map.force
    fc.truncate(size)
    fc.close
  }
  override def toString = "FileWriter(" + fc + ")"
}

object FileReader {
  def open(f: File) = new FileReader(FileChannel.open(f.toPath, READ, WRITE))
}

class FileReader(fc: FileChannel) extends JReader {
  var map = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size.toLong)
  var mapStart = 0L
  var mapSize = fc.size.toLong
  var fileSize = fc.size
  def size = fileSize
  def remap(i: Long, size: Int): Unit = {
    if (i < mapStart || i + size > mapStart + mapSize) {
      val size2 = math.max(size, 1024 * 1024)
      val (mStart, mSize) =
        if (fc.size - i > Int.MaxValue.toLong) (i, Int.MaxValue.toLong)
        else {
          val end = math.max(i + size2, fc.size)
          val s = math.max(0, end - Int.MaxValue)

          (s, end - s)
        }
      // println(mStart -> mSize)
      map = fc.map(FileChannel.MapMode.READ_WRITE, mStart, mSize)
      mapStart = mStart
      mapSize = mSize
    }
  }

  def get(i: Long, buf: Array[Byte], s: Int, l: Int) = {
    remap(i, l)
    map.position((i - mapStart).toInt)
    map.get(buf, s, l)
  }
  def get(i: Long): Byte = {
    remap(i, 1)
    map.position((i - mapStart).toInt)
    map.get
  }
  override def toString = "FileReader(" + fc + ")"
}
