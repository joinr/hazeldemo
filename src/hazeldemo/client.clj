;;client may not need to spool up its own cluster...
;;we always create a cluster of one for now for convenience,
;;but that may be unnecessary or undesirable going forward.
(ns hazeldemo.client
  (:require [chazel.core :as ch]
            [hazeldemo.core :as core]
            [hazeldemo.utils :as u]
            #_[hazeldemo.worker :as work]
            [clojure.core.async :as a :refer
             [>! <! >!! <!! put! take! chan]]))

;;might make sense to have multiple clients at some point.
;;I think we can alleviate the burden of client lifecycling
;;a bit by retaining a persistent client and defaulting to it.

(defonce me (delay (ch/client-instance core/+config+)))
(def ^:dynamic *client* me)

;;helper wrapper to allow us to have delayed instantiation.
(defn get-client ^com.hazelcast.core.HazelcastInstance []
  (if (delay? *client*)
    (deref *client*)
    *client*))

(defmacro on-cluster [& body]
  `(binding [~'hazeldemo.core/*cluster* (~'hazeldemo.client/get-client)]
     ~@body))

;;use executor service implementation....see if this is faster,
;;examine downsides.
;;This is about 73x faster than the queue-based implementation,
;;around 50x slower than in-memory pmap (2570x slower than single-core map...).
;;The problem we have here, is that if f is an anonymous function, we fail.
;;Since the classname for anony functions are not consistent across the cluster,
;;we need a way to resolve them (where currently partial is used to make thunks).

;;a) we can detect if f is a function, and if it's anonymous.

;;b) if it's not, we can lift it into the function's qualified name
;;   an project that using util/as-function to have the clients
;;   resolve on their end and apply.

;;c) if it's anonymous, we can serialize it with nippy,x
;;   have the clients deserialize it and apply on their end.
;;   if we are chewing a bunch of tasks, maybe we don't want
;;   to constantly deserialize the function....

;;we define new versions of fmap that use nippy for serialization.
;;this wraps the input arg in a map with contents that are serialized
;;by nippy.  The function invocation then unpacks the contents and applies
;;the function to the arg, then packs the result.  This allows us to
;;bypass the problem of Boolean/FALSE serialization problems.
(defn fmap [f coll]
  (let [n    10
        rets (map (fn [x]
                    (let [in (u/pack x)]
                      (ch/ftask (partial u/packed-call f in)))) coll)
        step (fn step [[x & xs :as vs] fs]
               (lazy-seq
                (if-let [s (seq fs)]
                  (cons (u/unpack (deref x)) (step xs (rest s)))
                  (map (comp u/unpack deref) vs))))]
    (step rets (drop n rets))))

(defn fmap2
  ([n size f coll]
   (let [rets (map (fn [x]
                     (let [in (u/pack x)]
                       (ch/ftask (partial u/packed-call f in)))) (partition size coll))
         step (fn step [[x & xs :as vs] fs]
                (lazy-seq
                 (if-let [s (seq fs)]
                   (concat (u/unpack (deref x)) (step xs (rest s)))
                   (mapcat (comp u/unpack deref) vs))))]
     (step rets (drop n rets)))))

;;control plane for evaluation and load, cluster-wide by default.
(defn eval-all! [expr]
  (let [res (ch/ftask (partial eval expr) :members :all)]
    (doseq [[m v] res]
      (println [m @v]))))

(defn compile-all! [expr]
  (let [res (ch/ftask (partial apply hazeldemo.utils/compile* expr) :members :all)]
    (doseq [[m v] res]
      (println [m @v]))))
