package main.scala

object main extends App {
  helloWorld("World")

  def helloWorld(string: String): Unit = {
    println("Hello, " + string + "!")
  }
}
