package tools

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.hbase.regionserver.StoreFile
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.ColumnRangeFilter


class Timing(
    val name: String = "Timing",
    val repeat: Int = 1
) {
  private final val Log = LoggerFactory.getLogger(classOf[Timing])

  private var totalNS = 0L
  private var nruns = 0

  def apply(op: => Unit): Unit = {
    for (n <- 0L until repeat) {
      println("%s: starting run #%d".format(name, nruns))
      val startNS = System.nanoTime
      op
      val stopNS = System.nanoTime
      val timeNS = stopNS - startNS
      totalNS += timeNS
      nruns += 1
      val subsecs = "%09d".format(timeNS % 1000000000)
      val secs = "%d".format(timeNS / 1000000000)
      println("%s: run #%d: %s.%s s".format(name, n, secs, subsecs))
    }
    println("%s: %s ns (%d runs)".format(name, totalNS / nruns, nruns))
  }
}

object Timing {
  def apply(name: String = "Timing", repeat: Int = 1): Timing = {
    return new Timing(name, repeat)
  }
}

object HBaseBench {
  private final val Log = LoggerFactory.getLogger("HBaseBench")

  @Flag(name="hbase")
  var hbase: String = "localhost:3434"

  @Flag(name="nversions")
  var nversions: Long = 1000

  @Flag(name="nqualifiers")
  var nqualifiers: Long = 1000

  @Flag(name="max-versions")
  var mMaxVersions: Int = 1000

  @Flag(name="do")
  var mDoFlag: String = null

  @Flag(name="hfile-path")
  var mHFilePathFlag: String = null

  @Flag(name="hfile-block-size-bytes")
  var mHFileBlockSizeBytesFlag: Int = 64 * 1024

  @Flag(name="hfile-compression-type")
  var mHFileCompressionTypeFlag: String = null

  @Flag(name="row-key")
  var mRowKeyFlag: String = "the-row-key"

  type Bytes = Array[Byte]

  implicit def stringToBytes(string: String): Bytes = {
    return string.getBytes("utf-8")
  }

  def bytesToString(bytes: Bytes): String = {
    return Bytes.toString(bytes)
  }

  class Bench {
    val conf = HBaseConfiguration.create()

    {
      val Array(host, port) = hbase.split(':')
      conf.set("hbase.zookeeper.quorum", host)
      conf.set("hbase.zookeeper.property.clientPort", port)
    }

    val tableName = "test_table"
    val rowKey = mRowKeyFlag
    val familyName = "family"

    // Create the table:
    {

      val admin = new HBaseAdmin(conf)
      val desc = new HTableDescriptor(tableName)
      val colDesc = new HColumnDescriptor(
          familyName,
          0,                                      // min versions
          HConstants.ALL_VERSIONS,                // max versions
          Compression.Algorithm.NONE.toString(),  // compression type
          false,                                  // in memory
          false,                                  // blockCacheEnabled
          64 * 1024,                              // blockSize
          HConstants.FOREVER,                     // TTL
          StoreFile.BloomType.NONE.toString(),    // bloom filter
          HConstants.REPLICATION_SCOPE_LOCAL      // scope
      )

      desc.addFamily(colDesc)
      val alreadyExists = !admin.listTables(tableName).isEmpty
      if (!alreadyExists) admin.createTable(desc)
    }

    val table = new HTable(conf, tableName)

    def put(): Unit = {
      Timing("put(%d × %d)".format(nqualifiers, nversions)) {

        var put: Put = new Put(rowKey)
        def flush(): Unit = {
          if (put.size > 0) {
            Log.info("Sending put with size {}", put.size())
            table.put(put)
            put = new Put(rowKey)
          }
        }

        def addKV(qualifier: Long, ts: Long): Unit = {
          put.add(familyName, "qualifier-%d".format(qualifier), ts, "value-%d".format(ts))
          if (put.size >= 1000000) flush()
        }

        for (qualifier <- 0L until nqualifiers) {
          for (ts <- 0L until nversions) {
            addKV(qualifier, ts)
          }
        }
        flush()
        Log.info("Flushing commits")
        table.flushCommits()
        Log.info("Commits flushed")
      }
    }

    def listQualifiers(): Unit = {
      Timing("get") {
        val limit = 1000
        var offset = 0
        var qualifier: Bytes = null

        var loop = true
        while (loop) {
          val get = new Get(rowKey)
              .addFamily(familyName)
              .setMaxVersions(1)
              .setFilter(
                  new FilterList(FilterList.Operator.MUST_PASS_ALL,
                      new ColumnRangeFilter(qualifier, false, null, false),
                      new ColumnPaginationFilter(limit, 0)))

              // .setFilter(new ColumnPaginationFilter(limit, offset))
          val result = table.get(get)
          Log.info("Result contains {} KeyValue(s).", result.size)
          if (result.isEmpty) {
            loop = false
          } else {
            offset += result.size
            qualifier = result.raw.last.getQualifier
          }
        }
        println()
      }
    }

    def get(): Unit = {
      val get = new Get(rowKey)
          .addFamily(familyName)
          .setMaxVersions(mMaxVersions)
          // .setFilter(new ColumnCountGetFilter(30))
          // .setFilter(new ColumnPaginationFilter(limit, offset))

      Timing("get") {
        val result = table.get(get)
        Log.info("Result contains {} KeyValue(s)", result.size)
      }
    }

    def getPage(): Unit = {
      Timing("get") {
        val limit = 1000
        var qualifier: Bytes = null

        var loop = true
        while (loop) {
          val qget = new Get(rowKey)
              .addFamily(familyName)
              .setMaxVersions(1)
              .setFilter(
                  new FilterList(FilterList.Operator.MUST_PASS_ALL,
                      new ColumnRangeFilter(qualifier, false, null, false),
                      new ColumnPaginationFilter(limit, 0)))

          Log.info("Fetching a page of qualifiers...")
          val qresult = table.get(qget)
          Log.info("Fetched {} qualifiers", qresult.size)

          if (qresult.isEmpty) {
            loop = false
          } else {
            val maxQualifier = qresult.raw.last.getQualifier
            val get = new Get(rowKey)
                .addFamily(familyName)
                .setMaxVersions(mMaxVersions)
                .setFilter(new ColumnRangeFilter(qualifier, false, maxQualifier, true))
//            for (kv <- qresult.raw)
//              get.addColumn(familyName, kv.getQualifier)

            Log.info("Fetching a page of cells...")
            val result = table.get(get)
            Log.info("Result contains {} KeyValue(s)", result.size)

            qualifier = maxQualifier
          }
        }
      }
    }

    def close(): Unit = {
      table.close()
    }

    def generateHFile(): Unit = {
      val fs: FileSystem = FileSystem.getLocal(conf)
      val path: Path = new Path(mHFilePathFlag)
      val compressionType = Compression.getCompressionAlgorithmByName(mHFileCompressionTypeFlag)
      val writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
          .createWriter(fs, path, mHFileBlockSizeBytesFlag, compressionType, KeyValue.KEY_COMPARATOR)

      var counter: Long = 0
      def log(qualifier: Long, timestamp: Long): Unit = {
        if (counter == 0) Log.info("q:{}, ts:{}", qualifier, timestamp)
        counter = (counter + 1) % 100000
      }

      for (qualifierId <- 0L until nqualifiers) {
        val qualifier = "q%09d".format(qualifierId)
        for (timestamp <- (nversions - 1).to(0L, -1)) {
          log(qualifierId, timestamp)
          val value = "value-%d".format(timestamp)
          val kv = new KeyValue(rowKey, familyName, qualifier, timestamp, value)
          writer.append(kv)
        }
      }

      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    FlagParser.init(this, args)
    val bench = new Bench()
    mDoFlag match {
      case "put" => bench.put()
      case "get" => bench.get()
      case "get-page" => bench.getPage()
      case "list-qualifiers" => bench.listQualifiers()
      case "generate-hfile" => bench.generateHFile()
    }
    bench.close()
  }
}