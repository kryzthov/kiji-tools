package tools

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.io.DatumReader
import org.apache.hadoop.fs.AvroFSInput
import org.apache.hadoop.fs.FileContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes.toStringBinary
import org.apache.hadoop.io.MapFile
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.util.ReflectionUtils.newInstance
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.file.FileReader

/**
 * Command-line tool to manipulate (inspect) files in Hadoop DFS.
 */
object FileTool {
  @Flag(name="format", usage="File format. One of 'seq', 'hfile', 'map'.")
  var format: String = null

  @Flag(name="path",
      usage="Path to the sequence file to dump.")
  var path: String = null

  private val conf = HBaseConfiguration.create()

  import ReflectionUtils.newInstance
  import Bytes.toStringBinary

  /**
   * Reads a sequence file of (song ID, # of song plays) into a map.
   *
   * @param path Path of the sequence file to read.
   * @param map Map to fill in with (song ID, # of song plays) entries.
   * @throws Exception on I/O error.
   */
  private def readSequenceFile(path: Path): Unit = {
    val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))
    try {
      Console.err.println("Key class: %s".format(reader.getKeyClassName))
      Console.err.println("Value class: %s".format(reader.getValueClassName))
      Console.err.println("Compression type: %s".format(reader.getCompressionType))
      Console.err.println("Compression codec: %s".format(reader.getCompressionCodec))
      if (!reader.getMetadata.getMetadata.isEmpty) {
        Console.err.println("Metadata:%n%s".format(reader.getMetadata.getMetadata))
      } else {
        Console.err.println("File '%s' has no metadata".format(path))
      }

      val key: Writable = newInstance(reader.getKeyClass, conf).asInstanceOf[Writable]
      val value: Writable = newInstance(reader.getValueClass, conf).asInstanceOf[Writable]
      while (true) {
        val position = reader.getPosition
        if (!reader.next(key, value)) {
          return
        }
        Console.out.println("position=%s\tkey=%s\tvalue=%s".format(position, key, value))
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Reads a Hadoop map file.
   *
   * @param path Path of the map file to read.
   */
  private def readMapFile(path: Path): Unit = {
    val reader = new MapFile.Reader(path, conf)
    try {
      val key: WritableComparable[_] =
          newInstance(reader.getKeyClass, conf).asInstanceOf[WritableComparable[_]]
      val value: Writable = newInstance(reader.getValueClass, conf).asInstanceOf[Writable]
      while (true) {
        if (!reader.next(key, value)) {
          return
        }
        Console.out.println("key=%s\tvalue=%s".format(key, value))
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Reads an HBase HFile.
   *
   * @param path Path to the HFile to read.
   */
  private def readHFile(path: Path): Unit = {
    val cacheConf = new CacheConfig(conf)
    val fs = FileSystem.get(conf)

    val status = fs.getFileStatus(path)
    val fileSize = status.getLen
    val istream = fs.open(path)
    val trailer = FixedFileTrailer.readFromStream(istream, fileSize)

    val closeIStream = true
    val reader: HFile.Reader =
        new HFileReaderV2(path, trailer, istream, fileSize, closeIStream, cacheConf)
    try {
      val cacheBlocks = false
      val positionalRead = false
      val scanner = reader.getScanner(cacheBlocks, positionalRead)

      var hasNext = scanner.seekTo()
      while (hasNext) {
        val keyValue = scanner.getKeyValue
        val rowKey = keyValue.getRow
        val family = keyValue.getFamily
        val qualifier = keyValue.getQualifier
        val timestamp = keyValue.getTimestamp
        val value = keyValue.getValue

        Console.out.println("row=%-30sfamily=%-10squalifer=%-10stimestamp=%s\tvalue=%s".format(
            toStringBinary(rowKey),
            toStringBinary(family),
            toStringBinary(qualifier),
            timestamp,
            toStringBinary(value)))

        hasNext = scanner.next()
      }
      // no need to close scanner
      // istream is closed by HFile.Reader
    } finally {
      reader.close()
    }
  }

  /**
   * Reads an Avro container file.
   *
   * @param path Path of the Avro container file to read.
   */
  def readAvroContainer(path: Path): Unit = {
    val context: FileContext = FileContext.getFileContext()
    val input: SeekableInput = new AvroFSInput(context, path)
    val datumReader: GenericDatumReader[_] = new GenericDatumReader()
    val reader: FileReader[_] = DataFileReader.openReader(input, datumReader)
    try {
      println("Schema:\n%s".format(reader.getSchema().toString(true)))
      var counter = 0
      while (reader.hasNext) {
        val rec = reader.next()
        println("entry #%d: %s".format(counter, rec))
        counter += 1
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Program entry point.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    val unparsed = FlagParser.init(this, args)
    require(unparsed.isEmpty, "Unparsed arguments: %s".format(unparsed))
    require((path != null) && !path.isEmpty, "Specify --path=...")

    val filePath = new Path(path)
    conf.set("fs.defaultFS", path)

    require((format != null) && !format.isEmpty, "Specify --format=...")
    format match {
      case "seq" => readSequenceFile(filePath)
      case "map" => readMapFile(filePath)
      case "hfile" => readHFile(filePath)
      case "avro" => readAvroContainer(filePath)
      case _ => sys.error("Unknown file format: %s".format(format))
    }
  }
}
