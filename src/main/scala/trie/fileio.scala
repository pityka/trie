package trie

import java.nio.{ ByteBuffer, ByteOrder }
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption._
import java.io.File

object FileWriter {
  def open(f: File) = new FileWriter(FileChannel.open(f.toPath, READ, WRITE))
}

class FileWriter(fc: FileChannel) extends FileReader(fc) with Writer {
  def set(i: Long, v: Array[Byte]) = {
    val bb = ByteBuffer.wrap(v)
    fc.write(bb, i)
    if (i + v.size > fileSize) {
      fileSize = i + v.size
    }
  }
  override def toString = "FileWriter(" + fc + ")"
}
class FileReader(fc: FileChannel) extends Reader {
  var map = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size.toLong)
  var mapStart = 0L
  var mapSize = fc.size.toLong
  var fileSize = fc.size
  def size = fileSize
  def get(i: Long, buf: Array[Byte]) = {
    if (i < mapStart || i + buf.size > mapStart + mapSize) {
      val size = if (fc.size - i > Int.MaxValue.toLong) Int.MaxValue.toLong
      else fc.size - i
      map = fc.map(FileChannel.MapMode.READ_ONLY, i, size)
      mapStart = i
      mapSize = size
    }
    map.position((i - mapStart).toInt)
    map.get(buf)
  }
  override def toString = "FileReader(" + fc + ")"
}
