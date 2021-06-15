package example

object HelloWorld {
  def max(x: Int, y: Int): Int = {
    if (x > y) x
    else y
  }




  def main(args: Array[String]): Unit = {
    println(max(4,5))
    println("Hello, world!")
  }
}
