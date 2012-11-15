(in-ns 'spark.api.clojure.core)

(defn scala-tuple2-to-map
  "Convert a collection of Scala Tuple2 into a map where tuple2._1 is the key associated with value tuple2._2"
  [tuple2s]
  (apply hash-map (flatten (for [tuple2 tuple2s] (vector (._1 tuple2) (._2 tuple2))))))