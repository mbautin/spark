(defproject org.spark-project/ClojureSpark "1.0.0-SNAPSHOT"
  :description "Clojure API for Spark"
  :url "https://github.com/markhamstra/spark/tree/dev/cljspark"
  :repositories [["typesafe" "http://repo.typesafe.com/typesafe/releases/"]
                 ["sonatype" "http://oss.sonatype.org/content/groups/public/"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.scala-lang/scala-library "2.9.2"]
                 [org.spark-project/spark-core_2.9.2 "0.6.0-SNAPSHOT"]
                 [org.apache/mesos "0.9.0"]
                 [log4j/log4j "1.2.16"]
                 [org.slf4j/slf4j-log4j12 "1.6.1"]
                 [com.typesafe.akka/akka-actor "2.0.2"]
                 [com.typesafe.akka/akka-remote "2.0.2"]
                 [com.typesafe.akka/akka-slf4j "2.0.2"]
                 [de.javakaffee/kryo-serializers "0.9"]
                 [it.unimi.dsi/fastutil "6.4.4"]
                 [org.eclipse.jetty/jetty-server "7.5.3.v20111011"]
                 [com.ning/compress-lzf "0.8.4"]
                 [org.apache.hadoop/hadoop-core "0.20.205.0"]
                 [asm/asm-all "3.3.1"]]
  :min-lein-version "2.0.0"
  :aot [spark.api.clojure.FunctionFactory])

