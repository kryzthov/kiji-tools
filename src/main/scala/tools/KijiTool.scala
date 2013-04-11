package tools

import java.io.PrintStream
import java.text.SimpleDateFormat
import java.util.Comparator
import java.util.Date
import java.util.{TreeMap => JTreeMap}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

import org.apache.hadoop.hbase.HConstants
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.impl.HBaseKijiRowData
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.TimestampComparator

class Tool(clArgs: Array[String]) {
  @Flag(name = "target", usage = "")
  var target: String = "kiji://.env/default"

  var unparsed: Seq[String] = FlagParser.init(this, clArgs).asScala

  val DateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS' 'z")

  class HtmlRowLayout(
      val layout: KijiTableLayout
  ) {
    val columns: Map[String, Int] = {
      val result = mutable.Map[String, Int]()
      var counter = 0
      for (family <- layout.getFamilies.asScala) {
        if (family.isGroupType) {
          for (column <- family.getColumns.asScala) {
            result += ("%s:%s".format(family.getName, column.getName) -> counter)
            counter += 1
          }
        } else if (family.isMapType) {
          result += (family.getName -> counter)
          counter += 1
        } else {
          sys.error("Family is neither group-type nor map-type.")
        }
      }
      result.toMap
    }

    private def formatSchema(schema: CellSchema): String = {
      return schema.getType match {
        case SchemaType.INLINE => schema.getValue
        case SchemaType.CLASS => schema.getValue
        case SchemaType.COUNTER => "counter"
      }
    }

    def write(out: PrintStream): Unit = {
      // Families
      out.println("<tr>")
      out.println("<th></th>")  // row/entity ID
      out.println("<th></th>")  // timestamp
      for (family <- layout.getFamilies.asScala) {
        if (family.isGroupType) {
          out.println("""<th colspan="%s">%s</th>""".format(
              family.getColumns.size,
              family.getName))
        } else if (family.isMapType) {
          out.println("<th>%s</th>".format(family.getName))
        } else {
          sys.error("Family is neither group-type nor map-type.")
        }
      }
      out.println("</tr>")

      // Columns
      out.println("<tr>")
      out.println("<th>row</th>")
      out.println("<th>timestamp</th>")
      for (family <- layout.getFamilies.asScala) {
        if (family.isGroupType) {
          for (column <- family.getColumns.asScala) {
            out.println("<th>%s</th>".format(column.getName))
          }
        } else if (family.isMapType) {
          out.println("<th></th>")
        } else {
          sys.error("Family is neither group-type nor map-type.")
        }
      }
      out.println("</tr>")

      // Types
      out.println("<tr>")
      out.println("<th></th>")  // row/entity ID
      out.println("<th></th>")  // timestamp
      for (family <- layout.getFamilies.asScala) {
        if (family.isGroupType) {
          for (column <- family.getColumns.asScala) {
            val schema = layout.getCellSchema(new KijiColumnName(family.getName, column.getName))
                out.println("<th>%s</th>".format(formatSchema(schema)))
          }
        } else if (family.isMapType) {
          val schema = layout.getCellSchema(new KijiColumnName(family.getName, null))
          out.println("<th>%s</th>".format(formatSchema(schema)))
        } else {
          sys.error("Family is neither group-type nor map-type.")
        }
      }
      out.println("</tr>")
    }
  }

  class HtmlRow(
      row: KijiRowData,
      header: HtmlRowLayout
  ) {
    val TSComparator = TimestampComparator.INSTANCE.asInstanceOf[Comparator[Long]]
    val rows = new JTreeMap[Long, Array[String]](TSComparator)

    def getRow(timestamp: Long): Array[String] = {
      val columns = rows.get(timestamp)
      if (columns != null) return columns
      val newColumns = new Array[String](header.columns.size)
      rows.put(timestamp, newColumns)
      return newColumns
    }

    def put(family: String, qualifier: String, timestamp: Long, content: String): Unit = {
      val columns = getRow(timestamp)
      header.columns.get(family) match {
        case Some(index) => {
          columns(index) = "%s: %s".format(qualifier, content)
        }
        case None => header.columns.get("%s:%s".format(family, qualifier)) match {
          case Some(index) => {
            columns(index) = content
          }
          case None => sys.error("Unknown column '%s:%s'.".format(family, qualifier))
        }
      }
    }

    {
      val fmap = row.asInstanceOf[HBaseKijiRowData].getMap()
      for ((family, qmap) <- fmap.asScala) {
        for ((qualifier, tseries) <- qmap.asScala) {
          for (timestamp <- tseries.keySet.asScala) {
            val value: Object = row.getValue(family, qualifier, timestamp)
            put(family, qualifier, timestamp, value.toString)
          }
        }
      }
    }

    def write(out: PrintStream): Unit = {
      var first = true
      for (entry <- rows.entrySet.iterator.asScala) {
        out.println("<tr>")
        if (first) {
          out.println("""<td rowspan="%d">""".format(rows.size))
          out.println(row.getEntityId.getComponents())
          out.println("</td>")
          first = false
        }

        val timestamp: Long = entry.getKey
        out.println("<td>%s</td>".format(DateFormat.format(new Date(timestamp))))

        for (content <- entry.getValue) {
          if (content == null) {
            out.println("<td></td>")
          } else {
            out.println("<td>%s</td>".format(content))
          }
        }

        out.println("</tr>")
      }
    }
  }

  def run(): Int = {
    val uri = KijiURI.newBuilder(target).build()
    val kiji = Kiji.Factory.open(uri)
    val table = kiji.openTable(uri.getTable)
    val reader = table.openTableReader()

    val families = table.getLayout.getFamilies.asScala

    val kdrb = KijiDataRequest.builder()
    for (family <- families) {
      kdrb.addColumns(ColumnsDef.create()
          .withMaxVersions(HConstants.ALL_VERSIONS)
          .addFamily(family.getName))
    }
    val kdr = kdrb.build()
    val scanner = reader.getScanner(kdr)

    println("""<!DOCTYPE html>""")
    println("""<link rel="stylesheet" href="file:///home/taton/main.css" />""")
    println("<table>")

    // Header
    val header = new HtmlRowLayout(table.getLayout)
    header.write(Console.out)

    // Rows
    for (row <- scanner.iterator.asScala) {
      new HtmlRow(row, header).write(Console.out)
    }
    println("</table>")

    reader.close()
    table.release()
    kiji.release()
    return 0
  }
}

object KijiTool {
  def main(args: Array[String]): Unit = {
    val tool = new Tool(args)
    sys.exit(tool.run())
  }
}
