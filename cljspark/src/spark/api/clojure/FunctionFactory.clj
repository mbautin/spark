(ns spark.api.clojure.FunctionFactory
  (:import (spark.api.java.function Function))
  (:gen-class
   :name spark.api.clojure.FunctionFactory
   :methods [[sparkFunc [clojure.lang.IFn] spark.api.java.function.Function]]))

(defn -sparkFunc [_ f]
  (proxy [Function] []
    (call [n] (f n))))