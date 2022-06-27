name := "relation"

version := "1.0-SNAPSHOT"

description := "Relational database implementation"

javaHome := sys.env.get("JAVA_HOME") map file

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

//This forbids including Scala related libraries into the dependencies
autoScalaLibrary := false

//initialize := {
//  val _ = initialize.value // run the previous initialization
//  val required = "17"
//  val current  = sys.props("java.specification.version")
//  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
//}