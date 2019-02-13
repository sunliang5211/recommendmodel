name := "recommendmodel"

version := "1.0"

scalaVersion := "2.12.1"

//resolvers += Resolver.mavenLocal
//resolvers += "aliyun Maven Repository" at "http://maven.aliyun.com/nexus/content/groups/public"
//externalResolvers := Resolver.withDefaultResolvers(resolvers.value,mavenCentral=false)

val spark_code = "org.apache.spark" %% "spark-core" % "2.4.0"
val spark_mllib = "org.apache.spark" %% "spark-mllib" % "2.4.0"
val spark_sql = "org.apache.spark" %% "spark-sql" % "2.4.0"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies += spark_code,
    libraryDependencies += spark_mllib,
    libraryDependencies += spark_sql
  )
