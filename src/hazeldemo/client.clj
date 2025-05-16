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

;;we can define unordered-fmap.
;;the fix with this is to use callbacks to deliver promises.
;;We want work stealing with backpressure.
;;So let the cluster farm work, use a callback to post results locally
;;to the results queue.
;;Then pull them off.
;;In theory, we have a pmap implementation with backpressure.
;;Can use a standard core.async loop.

;;https://gist.github.com/stathissideris/8659706
(defn seq!!
  "Returns a (blocking!) lazy sequence read from a channel.  Throws on err values" 
  [c]
  (lazy-seq
   (when-let [v (a/<!! c)]
     (if (instance? Throwable v)
       (throw v)
       (cons v (seq!! c))))))

;; (defn producer->consumer!! [n out f jobs]
;;   (let [;jobs    (a/chan 10)
;;         done?   (atom 0)
;;         res     (a/chan n)
;;         workers (dotimes [i n]
;;                   (a/thread
;;                     (loop []
;;                       (if-let [nxt (a/<!! jobs)]
;;                         (let [res (f nxt)
;;                               _   (a/>!! out res)]
;;                           (recur))
;;                         (let [ndone (swap! done? inc)]
;;                           (when (= ndone n)
;;                             (do (a/close! out)
;;                                 (a/>!! res true))))))))]
;;     res))

;;unordered map across a cluster.
;;we use callbacks for backpressure.
(defn ufmap
  ([n f coll]
   (let [pending  (atom 0)
         consumed (atom nil)
         res (a/chan n)
         push-result (fn push-result [x]
                       (let [v (u/unpack x)
                             nxt (swap! pending unchecked-dec)]
                        (a/put! res v)
                        (when (and @consumed (zero? nxt))
                          (a/close! res))))
        ins     (a/chan n)
        _    (a/go-loop []
               (if-let [x (a/<! ins)]
                 (let [in (u/pack x)]
                   (swap! pending unchecked-inc) ;;meh
                   (ch/ftask (partial u/packed-call f in)
                     :callback {:on-response push-result
                                :on-failure  (fn [ex]
                                               (println [:bombed :closing])
                                               (a/close! res))})
                   (recur))
                 (reset! consumed true)))
        jobs (a/onto-chan!! ins coll)]
     (seq!! res)))
  ([f coll] (ufmap 100 f coll)))

;;control plane for evaluation and load, cluster-wide by default.
(defn eval-all! [expr]
  (let [res (ch/ftask (partial eval expr) :members :all)]
    (doseq [[m v] res]
      (println [m @v]))))

(defn compile-all! [expr]
  (let [res (ch/ftask (partial apply hazeldemo.utils/compile* expr) :members :all)]
    (doseq [[m v] res]
      (println [m @v]))))
