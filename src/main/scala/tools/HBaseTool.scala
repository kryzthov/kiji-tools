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

object HBaseTool {
  type Bytes = Array[Byte]

  implicit def toBytes(string: String): Bytes = {
    return string.getBytes("utf-8")
  }

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "3434")

    val tableName = "test_table"
    val familyName = "family"

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

    val table = new HTable(conf, tableName)

    if (!alreadyExists) {
      val put = new Put("rowkey1")
      for (qualifier <- 0 until 10) {
        for (ts <- 0 until 10)
          put.add("family", "qualifier-%d".format(qualifier), ts, "value-%d".format(ts))
      }
      table.put(put)
      table.flushCommits()
    }

    val limit = 8
    val offset = 5

    val get = new Get("rowkey1")
        .addFamily("family")
        .setMaxVersions(2)
        .setFilter(new ColumnCountGetFilter(30))
        // .setFilter(new ColumnPaginationFilter(limit, offset))

    val result = table.get(get)
    for (kv <- result.raw) println(kv)
    println(result.size)

    table.close()
  }
}