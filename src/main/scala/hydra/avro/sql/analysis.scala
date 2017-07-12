package hydra.avro.sql

package object analysis {

  type Resolver = (String, String) => Boolean

  val caseInsensitiveResolution = (a: String, b: String) => a.equalsIgnoreCase(b)
  val caseSensitiveResolution = (a: String, b: String) => a == b
}