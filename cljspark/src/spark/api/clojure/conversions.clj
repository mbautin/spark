(in-ns 'spark.api.clojure.core)

(defn- scala-tuple2-to-map
  "Convert a collection of Scala Tuple2 into a map where tuple2._1 is the key associated with value tuple2._2"
  [tuple2s map-constructor]
  (apply map-constructor (flatten (for [tuple2 tuple2s] (list (._1 tuple2) (._2 tuple2))))))

(defn scala-tuple2-to-hash-map
  "Convert a collection of Scala Tuple2 into a hash-map where tuple2._1 is the key associated with value tuple2._2"
  [tuple2s]
  (scala-tuple2-to-map tuple2s hash-map))

(defn scala-tuple2-to-array-map
  "Convert a collection of Scala Tuple2 into an array-map where tuple2._1 is the key associated with value tuple2._2"
  [tuple2s]
  (scala-tuple2-to-map tuple2s array-map))