package trie

import xerial.larray._

class InMemoryStorageLArray extends JReader with JWriter {
  var backing = LArray.ofDim[Byte](1024 * 1024)
  var size = 0L
  def set(i: Long, v: Array[Byte]) = {
    val ii = i
    if (backing.size <= ii + v.size) {
      val backing2 = LArray.ofDim[Byte]((ii + v.size) * 3)
      LArray.copy(backing, 0, backing2, 0, backing.size)
      backing = backing2
    }
    var j = 0
    while (j < v.size) {
      backing(i + j) = v(j)
      j += 1
    }
    size = math.max(i + v.size, size)
  }
  def get(i: Long): Byte = backing(i)
  def get(i: Long, buf: Array[Byte], s: Int, l: Int) = {
    val tmp = backing.view(i, l + i)
    tmp.copyToArray(buf, s, l)
  }
  override def toString = backing.toString
  def close = ()
}
