import edu.stanford.nlp.ie.NERClassifierCombiner
import edu.stanford.nlp.util._
import edu.stanford.nlp.util.logging.Redwood
import spray.json._

import scala.io.StdIn

case class NamedEntity(
    name: String,
    entityType: String,
)

case class TaggedText(entries: Seq[NamedEntity])

case class AssociatedTags(
    curId: String,
    tags: TaggedText,
)

object NERJsonProtocol extends DefaultJsonProtocol {
  implicit val entityFormat = jsonFormat2(NamedEntity)
  implicit val taggedTextFormat = jsonFormat1(TaggedText)
  implicit val associatedTagsFormat = jsonFormat2(AssociatedTags)
}

object NERPiped extends App {

  import NERJsonProtocol._

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

  val ncc =
    NERClassifierCombiner.createNERClassifierCombiner("ner", null, props)

  val namedEntityPattern = """^\s*([^\s]+)\s+([A-Z]+).*$""".r

  private def processTextSegment(text: String): TaggedText = {
    // This one is best for dealing with the output as a TSV (tab-separated column) file.
    // The first column gives entities, the second their classes, and the third the remaining text
    // in a document
    val entries = ncc
      .classifyToString(text, "tabbedEntities", false)
      .split("\n")
      .filter(!_.startsWith("\t"))
      .map {
        case namedEntityPattern(entity, entityType) =>
          NamedEntity(entity, entityType)
        case x => throw new Exception(s"${x} was not a recognized entity type!")
      }
    TaggedText(entries.toSeq)
  }

  var line = ""
  var curId = ""
  var curText = ""

  while ({
    line = StdIn.readLine()
    line != null
  }) {
    line match {
      case "++++++++++++++++++++++++++++++++++++++++++++++++++" => {
        if (!curText.isEmpty) {
          val tagged = processTextSegment(curText)
          System.out.println(
            "++++++++++++++++++++++++++++++++++++++++++++++++++")
          System.out.println(AssociatedTags(curId, tagged).toJson)
        }
        curText = ""
        curId = StdIn.readLine()
      }
      case s => {
        curText += s"${s}\n"
      }
    }
  }
}
