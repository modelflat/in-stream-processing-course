object Modes {
  val STRUCTURED = "structured"
  val DSTREAM = "dstream"
}

object Main extends App {
  if (args.length > 0 && args(0) == Modes.DSTREAM)
    ImplDStreams.run()
  else
    ImplStructured.run()
}
