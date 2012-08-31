;; package spark.api.java
(ns spark.api
  (:import (spark.api.java JavaSparkContext)))

;; import spark.{Accumulator, AccumulatorParam, RDD, SparkContext}
;; import spark.SparkContext.IntAccumulatorParam
;; import spark.SparkContext.DoubleAccumulatorParam
;; import spark.broadcast.Broadcast

;; import scala.collection.JavaConversions._

;; import org.apache.hadoop.conf.Configuration
;; import org.apache.hadoop.mapred.InputFormat
;; import org.apache.hadoop.mapred.JobConf

;; import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}


;; import scala.collection.JavaConversions

(defn spark-context
  "Create a new Spark context."
  ([sc] ;; Scala SparkContext
     (JavaSparkContext. sc))
  ([^String master ^String framework-name]
     (JavaSparkContext. master framework-name))
  ([^String master ^String framework-name ^String spark-home & jars]
     (if (nil? jars)
       (JavaSparkContext. master framework-name spark-home "")
       (if (and (= 1 (count jars)) (coll? (first jars)))
         (JavaSparkContext. master framework-name spark-home (to-array jars))))))

;;   val env = sc.env
(defn env [jsc]
  (.env (.sc jsc)))

(defn parallelize
  "Parallelize a collection into a JavaRDD"
  ([jsc coll ^Integer num-slices]
     (.parallelize jsc (java.util.ArrayList. coll) num-slices))
  ([jsc coll]
     (.parallelize jsc (java.util.ArrayList. coll))))

(defn parallelize-pairs
  "Parallelize a collection of scala.Tuple2 into a JavaPairRDD"
  ([jsc pairs num-slices]
     (.parallelizePairs jsc (java.util.ArrayList pairs) num-slices))
  ([jsc pairs]
     (.parallelizePairs jsc (java.util.ArrayList. pairs))))

(defn parallelize-doubles
  "Parallelize a collection of Doubles into a JavaDoubleRDD"
  ([jsc ^doubles dbls ^Integer num-slices]
     (.parallelizeDoubles jsc (java.util.ArrayList. dbls) num-slices))
  ([jsc ^doubles dbls]
     (.parallelizeDoubles jsc (java.util.ArrayList. dbls))))

(defn text-file
  "Create an RDD from a text file."
  ([jsc ^String path min-splits]
     (.textFile jsc path (int min-splits)))
  ([jsc ^String path]
     (.textFile jsc path)))

(defn sequence-file
  "Create a JavaPairRDD from a Hadoop SequenceFile with the given key and value types."
  ([jsc path key-class value-class min-splits]
     (.sequenceFile jsc path key-class value-class (int min-splits)))
  ([jsc path key-class value-class]
     (.sequenceFile jsc path key-class value-class)))

(defn object-file
  "Load an RDD saved as a SequenceFile containing serialized objects with NullWritable keys and
BytesWritable values that contain a serialized partition.  This is still an experimental storage
format and may not be supported exactly as is in future Spark releases.  It will also be pretty
slow if you use the default serializer (Java serialization), though the nice thing about it is
that there's very littl effort required to save arbitrary objects."
  ([jsc path min-splits]
     (.objectFile jsc path (int min-splits)))
  ([jsc path]
     (.objectFile jsc path)))

(defn hadoop-RDD
  "Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
other necessary info (e.g. file name for filesystem-based dataset, table name for HyperTable,
etc.)"
  ([jsc conf input-format-class key-class value-class min-splits]
     (.hadoopRDD jsc conf input-format-class key-class value-class (int min-splits)))
  ([jsc conf input-format-class key-class value-class]
     (.hadoopRDD jsc conf input-format-class key-class value-class)))

(defn hadoop-file
  "Create a JavaPairRDD for a Hadoop-readable dataset from a Hadoop JobConf, giving its InputFormat
and any other necessary info (e.g. file name for a filesystem-based dataset, table name for
HyperTable, etc.)"
  ([jsc path input-format-class key-class value-class min-splits]
     (.hadoopFile jsc path input-format-class key-class value-class (int min-splits)))
  ([jsc path input-format-class key-class value-class]
     (.hadoopFile jsc path input-format-class key-class value-class)))

(defn new-api-hadoop-file [jsc path f-class k-class v-class conf]
  "Create a JavaPairRDD from a given Hadoop file with an arbitrary new API
InputFormat and extra configuration options to pass to the input format."
  (.newAPIHadoopFile jsc path f-class k-class v-class conf))

(defn new-api-hadoop-RDD [jsc conf f-class k-class v-class]
  "Create a JavaPairRDD from a given Hadoop file with an arbitrary new API
InputFormat and extra configuration options to pass to the input format."
  (.newAPIHadoopRDD jsc conf f-class k-class v-class))

(defn union [rdd & rdds]
  "Union of RDDs"
  (if (nil? rdds)
    (.union (JavaSparkContext/fromSparkContext (.context (first rdd))) (first rdd) (java.util.ArrayList. (rest rdd)))
    (if (coll? (first rdds))
      (.union (JavaSparkContext/fromSparkContext (.context rdd)) rdd (java.util.ArrayList. (first rdds)))
      (.union (JavaSparkContext/fromSparkContext (.context rdd)) rdd (java.util.ArrayList. rdds)))))

(defn int-accumulator [jsc initial-value]
  (.intAccumulator jsc initial-value))

(defn double-accumulator [jsc initial-value]
  (.doubleAccumulator jsc initial-value))

;;   def accumulator[T](initialValue: T, accumulatorParam: AccumulatorParam[T]): Accumulator[T] =
;;     sc.accumulator(initialValue)(accumulatorParam)

(defn broadcast [value jsc]
  (.broadcast (.sc jsc) value))

(defn stop [jsc]
  (.stop jsc))

(defn get-spark-home [jsc]
  (let [opt (.getSparkHome jsc)]
    (if (.isEmpty opt)
      nil
      (.get opt))))

(defn from-spark-context [sc]
  "Create JavaSparkContext from a SparkContext"
  (JavaSparkContext. sc))

(defn to-spark-context [jsc]
  "Get the SparkContext from a JavaSparkContext"
  (.sc jsc))
