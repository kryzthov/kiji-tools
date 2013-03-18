package tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object ListTool {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    for (arg <- args) {
      val path = fs.makeQualified(new Path(arg))
      println(path)
    }
  }
}