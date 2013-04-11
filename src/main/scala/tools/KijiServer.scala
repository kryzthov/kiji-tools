package tools

import java.net.InetSocketAddress
import scala.collection.JavaConverters._
import org.eclipse.jetty.http.MimeTypes
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler
import org.eclipse.jetty.util.resource.Resource
import org.eclipse.jetty.websocket.WebSocket
import org.eclipse.jetty.websocket.WebSocketServlet
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.kiji.schema.KijiDataRequest.Column
import org.apache.hadoop.hbase.util.Bytes
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef

class KijiServer {
  val addr = new InetSocketAddress(8383)
  val server = new Server(addr)

  /** Web socket servlet. */
  object ScanWSServlet extends WebSocketServlet {
    override def doWebSocketConnect(request: HttpServletRequest, protocol: String): WebSocket = {
      println("doWebSocketConnect with protocol " + protocol)
      protocol match {
      case "scan" => return new ScanWebSocket()
      case _ => throw new Exception("Unsupported web socket protocol: '%s'.".format(protocol))
      }
    }
  }

  /** Web socket that handles a table scan. */
  class ScanWebSocket extends WebSocket.OnTextMessage {
    private var conn: WebSocket.Connection = null

    override def onOpen(conn: WebSocket.Connection): Unit = {
      require(this.conn == null)
      ScanWebSocket.this.conn = conn
      require(this.conn != null)
      println("ScanWebSocket opened")
    }

    override def onMessage(message: String): Unit = {
      require(ScanWebSocket.this.conn != null)

      val kijiURI = KijiURI.newBuilder(message).build()
      val kiji = Kiji.Factory.open(kijiURI)
      val table = kiji.openTable(kijiURI.getTable)
      val reader = table.openTableReader()
      val kdrb = KijiDataRequest.builder()
      val families = table.getLayout.getFamilies.asScala
      for (family <- families) {
        kdrb.addColumns(ColumnsDef.create().addFamily(family.getName))
      }
      val kdr = kdrb.build()
      val scanner = reader.getScanner(kdr)
      for (row <- scanner.iterator.asScala) {
        val eid = row.getEntityId.getComponents()

        for (family <- families) {
          if (family.isMapType) {
            val qmap = row.getValues(family.getName)

          } else if (family.isGroupType) {
            for (column <- family.getColumns.asScala) {
              val timeseries = row.getValues(family.getName, column.getName)
              conn.sendMessage(row.getEntityId.toShellString)
            }
          } else {
            sys.error("Weird family!")
          }
          conn.sendMessage(row.getEntityId.toShellString)
        }
      }
      conn.sendMessage("")
    }

    override def onClose(closeCode: Int, message: String): Unit = {
      require(ScanWebSocket.this.conn != null)
      println("onClose(code=%d, message='%s')".format(closeCode, message))
      conn = null
      println("ScanWebSocket closed")
    }
  }

  private object ScanHandler extends AbstractHandler {
    final val Target = "/scan"

    override def handle(
        target: String,
        baseRequest: Request,
        request: HttpServletRequest,
        response: HttpServletResponse
    ): Unit = {
      if (target != Target) return
      baseRequest.setHandled(true)

      val writer = response.getWriter()

      response.setContentType(MimeTypes.TEXT_HTML_UTF_8)
      val kijiURIHeader = response.getHeader("kiji-uri")
      if (kijiURIHeader == null) {
      }
      val kijiURI = KijiURI.newBuilder(kijiURIHeader).build()
      val kiji = Kiji.Factory.open(kijiURI)
      val table = kiji.openTable(kijiURI.getTable)
      val reader = table.openTableReader()
      val scanner = reader.getScanner(KijiDataRequest.builder().build())
      for (row <- scanner.iterator.asScala) {
        writer.write("Row: %s </br>\n".format(row.getEntityId))
      }
      reader.close()

      response.setStatus(HttpServletResponse.SC_OK)
    }
  }

  private object ErrorHandler extends AbstractHandler {
    override def handle(
        target: String,
        baseRequest: Request,
        request: HttpServletRequest,
        response: HttpServletResponse
    ): Unit = {
      baseRequest.setHandled(true)
      response.setContentType(MimeTypes.TEXT_HTML_UTF_8)
      val writer = response.getWriter()
      writer.write("Bad request: </br>")
      writer.write("Target: %s</br>".format(target))
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
  }

  def run(): Unit = {
    val handlers = new HandlerList()
    handlers.addHandler(ScanHandler)
    handlers.addHandler({
      val sch = new ServletContextHandler()
      sch.addServlet(new ServletHolder(ScanWSServlet), "/ws")
      sch
    })
    handlers.addHandler({
      val rh = new ResourceHandler()
      rh.getMimeTypes.addMimeMapping("html", MimeTypes.TEXT_HTML_UTF_8)
      rh.getMimeTypes.addMimeMapping("js", "application/javascript")
      rh.setBaseResource(Resource.newClassPathResource("html"))
      rh
    })
    handlers.addHandler(ErrorHandler)
    server.setHandler(handlers)

    server.start()
    server.join()
  }
}

object KijiServer {
  def main(args: Array[String]): Unit = {
    val server = new KijiServer()
    server.run()
  }
}