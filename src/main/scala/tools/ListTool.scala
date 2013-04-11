package tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.kiji.common.flags.Flag

/** Generalized ls tool. */
object ListTool {
  @Flag(name="a")
  var all = false

  @Flag(name="l")
  var long = false

  def main(args: Array[String]): Unit = {
    for (arg <- args) {
      val conf = new Configuration()
      val path = new Path(arg)
      if (path.toUri.getScheme != null) {
        conf.set("fs.defaultFS", path.toString)
      }
      val fs = FileSystem.get(conf)
      val qpath = fs.makeQualified(path)
      val status = fs.getFileStatus(qpath)
      Console.out.print("%s%s\t%d bytes".format(
          qpath,
          if (status.isDirectory) "/" else "",
          status.getLen,
          status.getOwner,
          status.getGroup
          ))
    }
  }
}