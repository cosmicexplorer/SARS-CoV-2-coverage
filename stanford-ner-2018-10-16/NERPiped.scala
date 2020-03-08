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

object UUIDUtils {
  val validUUID = """[a-f0-9\-]{36}""".r
}

case class UUIDWrapper(uuid: String) {
  import UUIDUtils._

  uuid match {
    case validUUID() => ()
    case _ => throw new Exception(s"given uuid string $uuid was invalid!")
  }
}

case class AssociatedTags(
    uuid: UUIDWrapper,
    tags: TaggedText,
)

case class NewsArticle(
  uuid: UUIDWrapper,
  text: String
)

object NERJsonProtocol extends DefaultJsonProtocol {
  implicit val entityFormat = jsonFormat2(NamedEntity)
  implicit val taggedTextFormat = jsonFormat1(TaggedText)
  implicit val uuidFormat = jsonFormat1(UUIDWrapper)
  implicit val associatedTagsFormat = jsonFormat2(AssociatedTags)
  implicit val articleFormat = jsonFormat2(NewsArticle)
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
    // (quoted from NERDemo.java in the stanford NER download zip):
    // This one is best for dealing with the output as a TSV (tab-separated column) file.
    // The first column gives entities, the second their classes, and the third the remaining text
    // in a document.
    val entries = ncc
      .classifyToString(text, "tabbedEntities", false)
      .split("\n")
      .filter(!_.startsWith("\t"))
      .flatMap {
        case namedEntityPattern(entity, entityType) => Some(NamedEntity(entity, entityType))
        case _nonRecognizedText => None
      }
    TaggedText(entries.toSeq)
  }

  var line = ""

  while ({
    line = StdIn.readLine()
    line != null
  }) {
    val NewsArticle(uuid, text) = line.parseJson.convertTo[NewsArticle]
    val tagged = processTextSegment(text)
    System.out.println(AssociatedTags(uuid, tagged).toJson)
  }
}
