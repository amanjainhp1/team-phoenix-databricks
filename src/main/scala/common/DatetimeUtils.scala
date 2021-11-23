// Databricks notebook source
// Retrieve timestamp and datestamp upon notebook startup
import java.util.Date
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat

class CurrentTime {
  val sdf = new SimpleDateFormat("yyyyMMdd")
  val currentTime = System.currentTimeMillis

  def getTimestamp() = currentTime/1000
  
  def getDatestamp() = sdf.format(new Date(currentTime))
}
