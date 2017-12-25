package com.onemarket.utils

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object FileUtil {

  def checkFileExists(path: String, conf: Configuration): Boolean = {
    val hdpPath = new Path(path)
    val fs = hdpPath.getFileSystem(conf)
    fs.exists(hdpPath)
  }
}