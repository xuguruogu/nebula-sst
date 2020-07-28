package com.tencent.sparkx.graphdb

import java.io.{File, IOException}
import java.nio.file.{Files, StandardCopyOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.text.SimpleDateFormat

import com.google.common.primitives.UnsignedLong
import com.vesoft.nebula.NebulaCodec
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.hadoop.io.WritableComparator
import org.rocksdb.RocksDB
import org.slf4j.LoggerFactory

object SstUtils {
  private val log = LoggerFactory.getLogger(getClass)

  private val libraryName = "nebula_codec"
  private val libraryPrefix = "libnebula_codec"
  private val librarySuffix = ".so"

  try {
    RocksDB.loadLibrary()
    log.info("RocksDB.loadLibrary success.")
  } catch {
    case e: Exception =>
      log.error("Cannot load NebulaCodec library! ", e)
  }

  try {
    System.loadLibrary(libraryName)
    log.info(s"System.loadLibrary(${libraryName}) success.")
  } catch {
    case _: UnsatisfiedLinkError =>
      try {
        loadNebulaLibraryFromJar()
        log.info(s"loadNebulaLibraryFromJar success.")
      } catch {
        case e: Exception =>
          log.error("Cannot load NebulaCodec library! ", e)
      }
  }

  @throws[IOException]
  private def loadNebulaLibraryFromJar(): Unit = {
    val path: String = "/libnebula_codec.so"
    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    if (!tempDir.exists && !tempDir.mkdir) {
      throw new IOException("Failed to create temp directory " + tempDir)
    }
    val temp = File.createTempFile(libraryPrefix, librarySuffix)
    if (!temp.exists) {
      throw new RuntimeException("File " + temp.getAbsolutePath + " does not exist.")
    }
    temp.deleteOnExit()
    try {
      val is = classOf[NebulaCodec].getResourceAsStream(path)
      try {
        if (is == null) {
          throw new RuntimeException(path + " was not found inside JAR.")
        }
        Files.copy(is, temp.toPath, StandardCopyOption.REPLACE_EXISTING)
      } finally {
        if (is != null) {
          is.close()
        }
      }
    }
    System.load(temp.getAbsolutePath)
  }

  private val PARTITION_ID_SIZE = 4
  private val VERTEX_ID_SIZE = 8
  private val TAG_ID_SIZE = 4
  private val TAG_VERSION_SIZE = 8
  private val EDGE_TYPE_SIZE = 4
  private val EDGE_RANKING_SIZE = 8
  private val EDGE_VERSION_SIZE = 8
  private val VERTEX_SIZE = PARTITION_ID_SIZE + VERTEX_ID_SIZE + TAG_ID_SIZE + TAG_VERSION_SIZE
  private val EDGE_SIZE = PARTITION_ID_SIZE + VERTEX_ID_SIZE + EDGE_TYPE_SIZE + EDGE_RANKING_SIZE + VERTEX_ID_SIZE + EDGE_VERSION_SIZE

  private val DATA_KEY_TYPE = 0x00000001
  private val TAG_MASK = 0xBFFFFFFF
  private val EDGE_MASK = 0x40000000

  val emptyValueEncoded: Array[Byte] = NebulaCodec.encode(new Array[AnyRef](0))

  def assertPropsMatch(props: Array[String], values: Array[AnyRef]): Unit = {
    if (props.length != values.length) {
      throw new RuntimeException(
        s"props in schema len ${props.length} != actually values len ${values.length}")
    }

    for (i <- values.indices) {
      val prop = props(i)
      val clazz = values(i).getClass.getName
      clazz match {
        case "java.lang.Boolean" =>
          if (!prop.equalsIgnoreCase("bool")) {
            throw new RuntimeException("Type Not match: " + prop + " vs " + clazz)
          }

        case "java.lang.Integer" =>
        case "java.lang.Long" =>
          if (!prop.equalsIgnoreCase("int") && !prop.equalsIgnoreCase("timestamp")) {
            throw new RuntimeException("Type Not match: " + prop + " vs " + clazz)
          }

        case "java.lang.Float" =>
          if (!prop.equalsIgnoreCase("float")) {
            throw new RuntimeException("Type Not match: " + prop + " vs " + clazz)
          }

        case "java.lang.Double" =>
          if (!prop.equalsIgnoreCase("double")) {
            throw new RuntimeException("Type Not match: " + prop + " vs " + clazz)
          }

        case "[B" =>
        case "java.lang.String" =>
          if (!prop.equalsIgnoreCase("string")) {
            throw new RuntimeException("Type Not match: " + prop + " vs " + clazz)
          }

        case _ =>
          throw new RuntimeException("Type Error : " + clazz)
      }
    }
  }

  def encodeProps(values: Any*): Array[Byte] = {
    val v: Array[AnyRef] = new Array[AnyRef](values.length)
    for (i <- 0 until values.length) {
      val value: Any = values(i)
      val clazz: Class[_] = value.getClass
      val name: String = clazz.getName
      name match {
        case "java.lang.Byte" =>
          v(i) = value.asInstanceOf[Byte].longValue.asInstanceOf[AnyRef]

        case "java.lang.Short" =>
          v(i) = value.asInstanceOf[Short].longValue().asInstanceOf[AnyRef]

        case "java.lang.Integer" =>
          v(i) = value.asInstanceOf[Integer].longValue().asInstanceOf[AnyRef]

        case "java.lang.Float" =>
          v(i) = value.asInstanceOf[Float].doubleValue().asInstanceOf[AnyRef]

        case "java.lang.Boolean" =>
        case "java.lang.Long" =>
        case "java.lang.Double" =>
        case "[B" =>
          v(i) = value.asInstanceOf[Array[Byte]].asInstanceOf[AnyRef]

        case "java.lang.String" =>
          v(i) = value.toString.getBytes.asInstanceOf[AnyRef]

        case _ =>
          throw new RuntimeException("Type Error : " + name)
      }
    }
    NebulaCodec.encode(v)
  }

  def checkAndEncodeProps(props: Array[String], values: Any*): Array[Byte] = {
    if (props.length != values.length) {
      throw new RuntimeException(
        s"props in schema len ${props.length} != actually values len ${values.length}")
    }
    val v: Array[AnyRef] = new Array[AnyRef](values.length)
    for (i <- 0 until values.length) {
      val prop: String = props(i)
      val value: Any = values(i)
      if (value == null) {
        prop match {
          case "bool" =>
            v(i) = false.asInstanceOf[AnyRef]

          case "int" =>
          case "timestamp" =>
            v(i) = 0L.asInstanceOf[AnyRef]

          case "double" =>
            v(i) = 0.0.asInstanceOf[AnyRef]

          case "string" =>
            v(i) = String.valueOf("").asInstanceOf[AnyRef]

          case _ =>
            throw new RuntimeException("Type Error : " + prop)
        }
      }
      else {
        val clazz: String = value.getClass.getName
        prop match {
          case "bool" =>
            clazz match {
              case "java.lang.Boolean" =>
                v(i) = value.asInstanceOf[Boolean].asInstanceOf[AnyRef]

              case "java.lang.Byte" =>
                v(i) = (value.asInstanceOf[Byte] != 0.asInstanceOf[Byte]).asInstanceOf[AnyRef]

              case "java.lang.Short" =>
                v(i) = (value.asInstanceOf[Short] != 0.asInstanceOf[Short]).asInstanceOf[AnyRef]

              case "java.lang.Integer" =>
                v(i) = (value.asInstanceOf[Integer] != 0.asInstanceOf[Integer]).asInstanceOf[AnyRef]

              case "java.lang.Long" =>
                v(i) = (value.asInstanceOf[Long] != 0L).asInstanceOf[AnyRef]

              case _ =>
                throw new RuntimeException("Type Miss Match : " + prop + "," + clazz)
            }

          case "int" =>
          case "timestamp" =>
            clazz match {
              case "java.lang.Boolean" =>
                v(i) = if (value.asInstanceOf[Boolean]) {
                  1L.asInstanceOf[AnyRef]
                } else {
                  0L.asInstanceOf[AnyRef]
                }

              case "java.lang.Byte" =>
                v(i) = value.asInstanceOf[Byte].longValue().asInstanceOf[AnyRef]

              case "java.lang.Short" =>
                v(i) = value.asInstanceOf[Short].longValue().asInstanceOf[AnyRef]

              case "java.lang.Integer" =>
                v(i) = value.asInstanceOf[Integer].longValue().asInstanceOf[AnyRef]

              case "java.lang.Long" =>
                v(i) = value.asInstanceOf[Long].asInstanceOf[AnyRef]

              case "[B" =>
                if (value.asInstanceOf[Array[Byte]].length == 0) {
                  v(i) = 0L.asInstanceOf[AnyRef]
                } else {
                  try {
                    v(i) = new String(value.asInstanceOf[Array[Byte]]).asInstanceOf[AnyRef]
                  } catch {
                    case e: NumberFormatException =>
                      v(i) = UnsignedLong.valueOf(
                        new String(value.asInstanceOf[Array[Byte]]))
                        .longValue.asInstanceOf[AnyRef]
                  }
                }

              case "java.lang.String" =>
                if (value.asInstanceOf[String].isEmpty) {
                  v(i) = 0L.asInstanceOf[AnyRef]
                } else {
                  try {
                    v(i) = value.asInstanceOf[String].toLong.asInstanceOf[AnyRef]
                  } catch {
                    case _: NumberFormatException =>
                      v(i) = UnsignedLong.valueOf(value.asInstanceOf[String])
                        .longValue.asInstanceOf[AnyRef]
                  }
                }

              case _ =>
                throw new RuntimeException("Type Miss Match : " + prop + "," + clazz)
            }

          case "double" =>
            clazz match {
              case "java.lang.Float" =>
                v(i) = value.asInstanceOf[Float].doubleValue().asInstanceOf[AnyRef]

              case "java.lang.Double" =>
                v(i) = value.asInstanceOf[Double].asInstanceOf[AnyRef]

              case "java.lang.Byte" =>
                v(i) = value.asInstanceOf[Byte].doubleValue().asInstanceOf[AnyRef]

              case "java.lang.Short" =>
                v(i) = value.asInstanceOf[Short].doubleValue().asInstanceOf[AnyRef]

              case "java.lang.Integer" =>
                v(i) = value.asInstanceOf[Integer].doubleValue().asInstanceOf[AnyRef]

              case "java.lang.Long" =>
                v(i) = value.asInstanceOf[Long].doubleValue().asInstanceOf[AnyRef]

              case "[B" =>
                if ((value.asInstanceOf[Array[Byte]]).length == 0) {
                  v(i) = 0.0.asInstanceOf[AnyRef]
                } else {
                  v(i) = new String(value.asInstanceOf[Array[Byte]]).toDouble.asInstanceOf[AnyRef]
                }

              case "java.lang.String" =>
                if (value.asInstanceOf[String].isEmpty) {
                  v(i) = 0.0.asInstanceOf[AnyRef]
                }
                else {
                  v(i) = value.asInstanceOf[String].toDouble.asInstanceOf[AnyRef]
                }

              case _ =>
                throw new RuntimeException("Type Miss Match : " + prop + "," + clazz)
            }

          case "string" =>
            clazz match {
              case "java.lang.Float" =>
              case "java.lang.Double" =>
              case "java.lang.Byte" =>
              case "java.lang.Short" =>
              case "java.lang.Integer" =>
              case "java.lang.Long" =>
              case "java.lang.String" =>
                v(i) = value.toString.getBytes.asInstanceOf[AnyRef]

              case "[B" =>
                v(i) = value.asInstanceOf[String].asInstanceOf[AnyRef]

              case _ =>
                throw new RuntimeException("Type Miss Match : " + prop + "," + clazz)
            }

          case _ =>
            throw new RuntimeException("Type Error : " + prop)
        }
      }
    }
    assertPropsMatch(props, v)
    encodeProps(v: _*)
  }

  def createEdgeKey(partitionId: Int, srcId: Long, edgeType: Int,
                    edgeRank: Long, dstId: Long, edgeVersion: Long)
  : Array[Byte] = {
    val buffer = ByteBuffer.allocate(EDGE_SIZE)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt((partitionId << 8) | DATA_KEY_TYPE)
    buffer.putLong(srcId)
    buffer.putInt(edgeType | EDGE_MASK)
    buffer.putLong(edgeRank)
    buffer.putLong(dstId)
    buffer.order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(edgeVersion)
    buffer.array
  }

  def createVertexKey(partitionId: Int, vertexId: Long, tagId: Int, tagVersion: Long)
  : Array[Byte] = {
    val buffer = ByteBuffer.allocate(VERTEX_SIZE)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt((partitionId << 8) | DATA_KEY_TYPE)
    buffer.putLong(vertexId)
    buffer.putInt(tagId & TAG_MASK)
    buffer.order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(tagVersion)
    buffer.array
  }

  def getPartition(vid: Long, partitions: Int): Int = {
    UnsignedLong.fromLongBits(vid).mod(UnsignedLong.fromLongBits(partitions)).intValue + 1
  }

  def getPartition(key: Array[Byte]): Int = {
    val offset = 0
    getIntL(key, offset) >> 8
  }

  @throws[IllegalArgumentException]
  def getTimeStamp(dateTime: String): Long = {
    val format = "yyyyMMddHHmmss"
    if (dateTime.length > format.length) {
      throw new IllegalArgumentException(
        s"dateTime ${dateTime} should not longer than ${format.length}")
    }
    val dateFormat = new SimpleDateFormat(
      format.substring(0, dateTime.length))
    val date = dateFormat.parse(dateTime)
    date.getTime / 1000
  }

  @throws[IllegalArgumentException]
  def getVersion(dateTime: String): Long = {
    val format = "yyyyMMddHHmmss"
    if (dateTime.length > format.length) {
      throw new IllegalArgumentException(
        s"dateTime ${dateTime} should not longer than ${format.length}")
    }
    val dateFormat = new SimpleDateFormat(format.substring(0, dateTime.length))
    val date = dateFormat.parse(dateTime)
    Long.MaxValue - date.getTime * 1000
  }

  def getSrcId(key: Array[Byte]): Long = {
    assert(key.length == EDGE_SIZE)
    getLongL(key, PARTITION_ID_SIZE)
  }

  def getVId(key: Array[Byte]): Long = {
    assert(key.length == EDGE_SIZE || key.length == VERTEX_SIZE)
    getLongL(key, PARTITION_ID_SIZE)
  }

  def getEdgeType(key: Array[Byte]): Int = {
    assert(key.length == EDGE_SIZE)
    val edgeType = getIntL(key, PARTITION_ID_SIZE + VERTEX_ID_SIZE)
    if (edgeType > 0) edgeType & ~EDGE_MASK
    else edgeType
  }

  def getRank(key: Array[Byte]): Long = {
    assert(key.length == EDGE_SIZE)
    getLongL(key, PARTITION_ID_SIZE + VERTEX_ID_SIZE + EDGE_TYPE_SIZE)
  }

  def getDstId(key: Array[Byte]): Long = {
    assert(key.length == EDGE_SIZE)
    getLongL(key, PARTITION_ID_SIZE + VERTEX_ID_SIZE
      + EDGE_TYPE_SIZE + EDGE_RANKING_SIZE)
  }

  def getVertexId(key: Array[Byte]): Long = {
    assert(key.length == VERTEX_SIZE)
    getLongL(key, PARTITION_ID_SIZE)
  }

  def getTagId(key: Array[Byte]): Int = {
    assert(key.length == VERTEX_SIZE)
    getIntL(key, PARTITION_ID_SIZE + VERTEX_ID_SIZE)
  }

  def getVersionBigEndian(key: Array[Byte]): Long = {
    val buffer = ByteBuffer.allocate(8)
      .order(ByteOrder.BIG_ENDIAN)
    val b1 = new Array[Byte](8)
    System.arraycopy(key, key.length - 8, b1, 0, 8)
    buffer.put(b1)
    buffer.flip
    buffer.getLong
  }

  def isVertex(key: Array[Byte]): Boolean = {
    key.length == VERTEX_SIZE
  }

  def isEdge(key: Array[Byte]): Boolean = {
    key.length == EDGE_SIZE
  }


  private def makeInt(b3: Byte, b2: Byte, b1: Byte, b0: Byte) = {
    ((b3) << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | (b0 & 0xff)
  }

  private def getIntL(bb: Array[Byte], bi: Int): Int = {
    makeInt(bb(bi + 3), bb(bi + 2), bb(bi + 1), bb(bi))
  }

  private def makeLong(b7: Byte, b6: Byte, b5: Byte, b4: Byte,
                       b3: Byte, b2: Byte, b1: Byte, b0: Byte) = {
    (b7.toLong << 56) | ((b6.toLong & 0xff) << 48) |
      ((b5.toLong & 0xff) << 40) | ((b4.toLong & 0xff) << 32) |
      ((b3.toLong & 0xff) << 24) | ((b2.toLong & 0xff) << 16) |
      ((b1.toLong & 0xff) << 8) | (b0.toLong & 0xff)
  }

  private def getLongL(bb: Array[Byte], bi: Int): Long = {
    makeLong(bb(bi + 7), bb(bi + 6), bb(bi + 5), bb(bi + 4),
      bb(bi + 3), bb(bi + 2), bb(bi + 1), bb(bi))
  }

  private def getLongB(bb: Array[Byte], bi: Int): Long = {
    makeLong(bb(bi), bb(bi + 1), bb(bi + 2), bb(bi + 3),
      bb(bi + 4), bb(bi + 5), bb(bi + 6), bb(bi + 7))
  }

  def hash(value: Array[Byte]): Long = {
    MurmurHash2.hash64(value, value.length, 0xc70f6907)
  }

  def hash(value: String): Long = {
    hash(value.getBytes)
  }

  def humanReadableByteSize(fileSize: Long): String = {
    if (fileSize <= 0) return "0 B"
    // kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta
    val units: Array[String] = Array("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    val digitGroup: Int = (Math.log10(fileSize) / Math.log10(1024)).toInt
    f"${fileSize / Math.pow(1024, digitGroup)}%3.3f ${units(digitGroup)}"
  }

  implicit val graphKeyEncodedOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    override def compare(a: Array[Byte], b: Array[Byte]): Int = {
      WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length)
    }
  }
}
