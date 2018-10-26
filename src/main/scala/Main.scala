object Modes {
  val STRUCTURED = "structured"
  val DSTREAM = "dstream"
}

object Main extends App {
  if (args.length > 0 && args(0) == Modes.DSTREAM) {
    println("Running DStream version")
    ImplDStreams.run()
  } else {
    println("Running Structured version")
    ImplStructured.run()
  }
}
