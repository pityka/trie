package trie

import scala.collection.mutable.ArrayBuffer
import java.nio.{ ByteBuffer, ByteOrder }

class BufferedCNodeReader(
    slow: CNodeReader,
    maxLevel: Int
) extends CNodeReader {

  val inmemory2 = new InMemoryStorage
  val fast = new CAHNodeWriter(inmemory2)

  val mmap = scala.collection.mutable.LongMap[Long]()

  def read(i: Long, level: Int): Option[CNode] = mmap.get(i) match {
    case None => {

      val r = slow.read(i)
      if (level <= maxLevel && r.isDefined) {
        val n = fast.append(r.get, level)
        mmap.update(i, n.address)

      }
      r
    }
    case Some(x) => fast.read(x, level).map(_.copy(address = i))
  }
  def readAddress(i: Long, b: Byte, l: Int): Long = mmap.get(i) match {
    case None => {
      if (l <= maxLevel) {
        val r = slow.read(i)
        if (r.isDefined) {
          val n = fast.append(r.get, l)
          mmap.update(i, n.address)
          fast.readAddress(n.address, b)
        } else slow.readAddress(i, b)
      } else slow.readAddress(i, b)
    }
    case Some(x) => fast.readAddress(x, b)
  }

  def readPayload(i: Long, l: Int): Long = mmap.get(i) match {
    case None => {
      {
        if (l <= maxLevel) {
          val r = slow.read(i)
          if (r.isDefined) {
            val n = fast.append(r.get, l)
            mmap.update(i, n.address)
            fast.readPayload(n.address)
          } else slow.readPayload(i)
        } else slow.readPayload(i)
      }

    }
    case Some(x) => fast.readPayload(x)
  }
  def readPartial(i: Long, buffer: Array[Byte], offset: Int, l: Int): (Array[Byte], Int) = mmap.get(i) match {
    case None => {
      {
        if (l <= maxLevel) {
          val r = slow.read(i)
          if (r.isDefined) {
            val n = fast.append(r.get, l)
            mmap.update(i, n.address)
            fast.readPartial(n.address, buffer, offset)
          } else slow.readPartial(i, buffer, offset)
        } else slow.readPartial(i, buffer, offset)
      }
    }

    case Some(x) => fast.readPartial(x, buffer, offset)
  }

  var prefixBuffer: Array[Byte] = Array.ofDim[Byte](1000)
}

class BufferedCNodeWriter(
    slow: CNodeWriter,
    maxLevel: Int
) extends BufferedCNodeReader(slow, maxLevel) with CNodeWriter {

  def write(n: CNode): Unit = {
    slow.write(n)

    mmap.get(n.address) match {
      case None => ()
      case Some(x) => {
        fast.write(n.copy(address = x))
      }
    }
  }

  def append(n: CNode, l: Int) = {
    val r = slow.append(n, l)

    if (l <= maxLevel) {
      val n2 = fast.append(n, l).address
      mmap.update(r.address, n2)
    }

    r
  }

  def updatePayload(old: Long, n: Long): Unit = {
    slow.updatePayload(old, n)
    mmap.get(old) match {
      case None => ()
      case Some(x) => fast.updatePayload(x, n)
    }
  }

  def updateRoute(old: Long, b: Byte, a: Long): Unit = {
    slow.updateRoute(old, b, a)
    mmap.get(old) match {
      case None => ()
      case Some(x) => fast.updateRoute(x, b, a)
    }
  }

}
