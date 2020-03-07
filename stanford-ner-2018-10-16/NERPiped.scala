import edu.stanford.nlp.ie.NERClassifierCombiner
import edu.stanford.nlp.util._
import edu.stanford.nlp.util.logging.Redwood

object NERPiped extends App {

  /** A logger for this class */
  val log = Redwood.channels(classOf[NERClassifierCombiner])

  var serializedClassifiers = Seq(
    "classifiers/english.conll.4class.distsim.crf.ser.gz",
    "classifiers/english.muc.7class.distsim.crf.ser.gz",
  )

  if (args.length > 0) {
    serializedClassifiers = args(0).split(",")
  }

  // from NERClassifierCombiner
  StringUtils.logInvocationString(log, args)
  val props = StringUtils.argsToProperties(args: _*)

  val ncc = NERClassifierCombiner.createNERClassifierCombiner("ner", null, props)

  val example = Seq(
    "Good afternoon Rajat Raina, how are you today?",
    "I go to school at Stanford University, which is located in California.",
    "Do you know what it is like to be hungry?",
    "Yes, I was hungry on Sunday at Arkansas Town Hall."
  )

  example.foreach { str =>
    // This one is best for dealing with the output as a TSV (tab-separated column) file.
    // The first column gives entities, the second their classes, and the third the remaining text in a document
    System.out.print(ncc.classifyToString(str, "tabbedEntities", false))
  }
}
