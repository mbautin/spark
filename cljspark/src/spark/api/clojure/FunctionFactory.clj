(ns spark.api.clojure.FunctionFactory
  (:import (spark.api.java.function Function FlatMapFunction Function2))
  (:gen-class
   :name spark.api.clojure.FunctionFactory
   :methods [[sparkFunction [clojure.lang.IFn] spark.api.java.function.Function]
             [sparkFlatMapFunction [clojure.lang.IFn] spark.api.java.function.FlatMapFunction]
             [sparkFunction2 [clojure.lang.IFn] spark.api.java.function.Function2]]))

(defn -sparkFunction [_ f]
  (proxy [Function] []
    (call [a] (f a))))

(defn -sparkFlatMapFunction [_ f]
  (proxy [FlatMapFunction] []
    (call [a] (f a))))

(defn -sparkFunction2 [_ f]
  (proxy [Function2] []
    (call [a b] (f a b))))
