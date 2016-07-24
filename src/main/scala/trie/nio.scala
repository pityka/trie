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
    fc.position(i)
    val bb = ByteBuffer.wrap(v)
    while (bb.hasRemaining()) {
      val c = fc.write(bb)
    }

    if (i + v.size > fileSize) {
      fileSize = i + v.size
    }
  }
  def close = {
    fc.force(false)
    fc.truncate(size)
    fc.close
  }
  override def toString = "FileWriter(" + fc + ")"
}

object FileReader {
  def open(f: File) = new FileReader(FileChannel.open(f.toPath, READ, WRITE))
}

class FileReader(fc: FileChannel) extends JReader {
  var fileSize = fc.size
  def size = fileSize

  def get(i: Long, buf: Array[Byte], s: Int, l: Int) = {
    fc.position(i)
    val bb = ByteBuffer.wrap(buf, s, l)
    var j = l
    while (j > 0) {
      val c = fc.read(bb)
      if (c >= 0) {
        j -= c
      } else throw new RuntimeException("EOF " + i + " " + l + " " + j)
    }

  }
  def get(i: Long): Byte = {
    fc.position(i)
    val bb = ByteBuffer.wrap(intBuffer, 0, 1)
    var j = 1
    while (j > 0) {
      val c = fc.read(bb)
      if (c >= 0) {
        j -= c
      } else throw new RuntimeException("EOF " + i + " " + " " + j)
    }
    intBuffer(0)
  }
  override def toString = "FileReader(" + fc + ")"
}
