(ns hazeldemo.utils
  (:require [chazel.core :as ch]
            [clojure.core.async :as a :refer
             [>! <! >!! <!! put! take! chan]]))

;;shim around an iterable that implements collection
;;and delegates to the obj's iterator.  Mainly to
;;satisfy janky interface requirements in addAll.
;;This allows us to wrap eductions and pass them through,
;;very niche usage.
(defn ->itercoll ^java.util.Collection [^java.lang.Iterable obj]
  (if (instance? java.util.Collection obj)
    obj
    (reify java.util.Collection
      (iterator [this] (.iterator obj)))))

(defn memo-1 [f]
  (let [^java.util.concurrent.ConcurrentHashMap
        cache (java.util.concurrent.ConcurrentHashMap.)]
    (fn [k]
      (if-let [res (.get cache k)]
        res
        (let [res (f k)
              _  (.put cache k res)]
          res)))))

(defn unstring [k]
  (if (string? k)
    (keyword k)
    k))

(alter-var-root #'unstring memo-1)


(defprotocol IRemote
  (as-function [this] "coerces arg into an invokable function"))

(extend-protocol IRemote
  String
  (as-function [this]
    (ns-resolve *ns* (symbol this)))
  clojure.lang.Symbol
  (as-function [this]
    (ns-resolve *ns* (symbol this)))
  clojure.lang.Keyword
  (as-function [this]
    (ns-resolve *ns* (symbol (name this))))
  ;;maybe add in default Object impl...
  )

;;function name pattern

(def eval-regex  #"eval[0-9]+")

(defn fn->symbol [f]
  (let [[l r] (-> f
                  str
                  (clojure.string/split #"\$"))
        [r _] (clojure.string/split r #"\@")]
    (if (re-find eval-regex r)
      (throw (ex-info "cannot resolve anonymous functions for distributed mapping!"
                      {:in f}))
      ;;need to convert underscores in classname to dashes for symbol.
      (symbol l (clojure.string/replace r "_" "-")))))

;;get a function's namespace qualified symbol and cache the result.
(defn symbolize [f]
  (cond (symbol? f) f
        (keyword? f) (symbol (name f))
        (string? f)  (symbol f)
        (fn? f) (fn->symbol f)
        :else (throw (ex-info "unable to coerce to qualified symbol" {:in f}))))


(defn await-delivery [p  & {:keys [retries timeout wait-time]
                            :or {retries Long/MAX_VALUE
                                 timeout (* 1000 60 60)
                                 wait-time 100}}]
  (let [res (chan 1)]
    (a/go-loop [n 0
                t 0]
      (if (and (< n retries) (< t timeout))
        (if (realized? p)
          (do (>! res @p)
              (a/close! res))
          (do (<! (a/timeout wait-time))
              (recur (unchecked-inc n) (unchecked-add t wait-time))))
        (do (>! res (ex-info "timed-out" {:in p :retries n :waited t})))))
    res))

;;we can await many promises.  just have the go routines waiting for
;;them and scanning.  If one is delivered, we pop it onto a result channel.

(defn promises->channel [xs & {:keys [buffer-size retries timeout wait-time]
                               :or {retries Long/MAX_VALUE
                                    timeout (* 1000 60 60)
                                    wait-time 100}}]
  (let [waiters   (->> xs
                       (mapv #(await-delivery % :retries retries
                                             :timeout timeout :wait-time wait-time)))
        buffer-size (or buffer-size (count waiters))]
    (a/merge waiters buffer-size)))

;;just to test out how inefficient alts is...
;;we can plough through 50k without incident,
;;but we probably just want to push work to a queue
;;and then wire up a channel to that....
#_
(let [ps (repeatedly 50000 (fn [] (promise)))
      out (u/promises->channel ps)]
  (future (loop [acc ps]
            (Thread/sleep 2)
            (when-let [p (first acc)]
              (deliver p :done!)
              (recur (rest acc)))))
  (a/go-loop [n 0]
    (when-let [res (<! out)]
      (when (zero? (mod n 100))
        (println [:delivered n]))
      (recur (unchecked-inc n)))))

;;Given the niche use-case, we are "probably" fine
;;for going with the promise-based route, although
;;it would undoubtedly be more efficient to get
;;a queue to dump into.

;;we can possibly handle classes of functions where
;;we don't depend on ref types.  Maybe also no direct
;;linking.

;;In theory, we can pass an anonymous function and
;;store it on the cluster.  That function is naturally
;;hashed from the client side, so we can store it in a
;;map on the cluster.  Workers can get the function from
;;the map if they don't have it already during do-job.
;;We can then resolve the function as a cluster resource.
;;Workers pull the function when doing work.
