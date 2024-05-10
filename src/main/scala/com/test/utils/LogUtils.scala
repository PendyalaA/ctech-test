package com.test.utils

import org.apache.log4j.{FileAppender, PatternLayout}

object LogUtils {

  def getFileAppender(appenderName: String, filePath: String, pattern: String = "%d %-5p %C%M:%L - %m%n"): FileAppender={
    val fileAppender: FileAppender = new FileAppender()
    try{
      fileAppender.setName(appenderName)
      fileAppender.setFile(filePath)
      fileAppender.setLayout (
        new PatternLayout(pattern)
      )
      fileAppender.activateOptions()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
    fileAppender
  }

}
