(defproject org.spark-project/ClojureSpark "1.0.0-SNAPSHOT"
  :description "Clojure API for Spark"
  :url "https://github.com/markhamstra/spark/tree/dev/cljspark"
  :repositories [["typesafe" "http://repo.typesafe.com/typesafe/releases/"]
                 ["sonatype" "http://oss.sonatype.org/content/groups/public/"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.spark-project/spark-core_2.9.2 "0.6.0-SNAPSHOT"]
                 [org.apache.mesos/mesos "0.9.0-incubating"]]
  :min-lein-version "2.0.0"
  :aot [spark.api.clojure.FunctionFactory]
  :repl-options {
                 :init (use '(spark.api.clojure core))}
  )

