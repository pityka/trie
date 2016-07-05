package trie

import java.io._

class FileWriter(f: File) extends FileReader(f) with Writer {
  val rf = new RandomAccessFile(f, "rw")
  def set(i: Long, v: Array[Byte]) = {
    rf.seek(i)
    rf.write(v)
  }
  override def toString = "FileWriter(" + f + ")"
}
class FileReader(f: File) extends Reader {
  val r = new RandomAccessFile(f, "r")
  def size = r.length
  def get(i: Long, buf: Array[Byte]) = {
    r.seek(i)
    r.readFully(buf)
  }
  override def toString = "FileReader(" + f + ")"
}
