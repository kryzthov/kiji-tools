package tools

import java.net.InetSocketAddress
import scala.collection.JavaConverters.asScalaIteratorConverter
import org.apache.avro.ipc.HttpServer
import org.apache.avro.ipc.specific.SpecificResponder
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.slf4j.LoggerFactory
import tools.avro.ListReply
import tools.avro.ListRequest
import tools.avro.Service
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.LocatedFileStatus

/** Server address parser and matcher */
object ServerAddress {
  val ServerAddressRegex = """([^:]+):(\d+)""".r

  /**
   * Deconstructs an InetSocketAddress into a (host, port) tuple.
   *
   * @param addr Server address to deconstruct.
   * @return matched address as a (host, port) tuple.
   */
  def unapply(isa: InetSocketAddress): Option[(String, Int)] = {
    return Some(isa.getHostName -> isa.getPort)
  }

  /**
   * Parses a "host:port" specification into an InetSocketAddress.
   *
   * @param addr a "host:port" string. Port must be within [0..65535].
   * @return the InetSocketAddress specified in addr.
   */
  def apply(addr: String): InetSocketAddress = {
    addr match {
      case ServerAddressRegex(host, portStr) => {
        val port = Integer.parseInt(portStr)
        if ((port >= 0) && (port <= 65535)) {
          return new InetSocketAddress(host, port)
        } else {
          throw new IllegalArgumentException("Invalid port in server address: " + addr)
        }
      }
      case _ => throw new IllegalArgumentException("Invalid server address: " + addr)
    }
  }
}

/**
 * RPC interface for the tool service.
 *
 * @param address Address the server will listen on.
 */
class ToolServer(
    val address: InetSocketAddress
) {
  private final val Log = LoggerFactory.getLogger(classOf[ToolServer])

  /** Implements the Avro RPC service. */
  class ToolServiceImpl extends Service {
    override def list(req: ListRequest): ListReply = {
      val reply = ListReply.newBuilder()
          .setDirPath(req.getDirPath)
          .setPaths(new java.util.ArrayList[String])
      val dirPath = new Path(req.getDirPath)
      val conf = new Configuration()
      if (dirPath.toUri.getScheme != null)
        conf.set("fs.defaultFS", dirPath.toString)
      else
        conf.set("fs.defaultFS", "file:///")
      val fs = FileSystem.get(conf)
      val it = fs.listLocatedStatus(dirPath)
      while (it.hasNext) {
        val fileStatus: LocatedFileStatus = it.next()
        reply.getPaths().add(fileStatus.getPath.toString)
      }
      return reply.build()
    }
  }

  private val responder = new SpecificResponder(classOf[Service], new ToolServiceImpl())
  private lazy val server = {
//    val server = new NettyServer(
//        responder,
//        address,
//        new NioServerSocketChannelFactory(
//            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()),
//        new ExecutionHandler(Executors.newCachedThreadPool()))
    val server = new HttpServer(responder, address.getHostName, address.getPort)
    Log.info("Server listening on: %s:%d".format(address.getHostName, server.getPort))
    server
  }

  /** Starts the server. */
  def start(): Unit = {
    server.start()
  }

  /** Stops the server. */
  def stop(): Unit = {
    server.close()
  }

  /** @return the address the server is listening on. */
  def getAddress(): InetSocketAddress = {
    return new InetSocketAddress(address.getHostName, server.getPort)
  }
}

/** Server process entry point. */
object ToolServer {
  @Flag(name = "help",
      usage = "Display this help message.")
  var help: Boolean = false

  @Flag(name = "listen-on",
      usage = "Server listens on this interface.")
  var listenOn: String = "localhost:8486"

  /**
   * Creates and sets a task queue server up according to the command-line arguments.
   *
   * @param args Command-line arguments.
   * @return the new task queue server.
   */
  def createServer(args: Array[String]): ToolServer = {
    FlagParser.init(this, args)
    if (help) {
      FlagParser.printUsage(this, Console.out)
      System.exit(0)
    }

    return new ToolServer(address = ServerAddress(listenOn))
  }

  /** Task queue server main entry point. */
  def main(args: Array[String]): Unit = {
    val server = createServer(args)
    server.start()
  }
}
