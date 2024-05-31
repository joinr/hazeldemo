(ns hazeldemo.utils
  (:require [chazel.core :as ch]
            [clojure.java.io]
            [clojure.core.async :as a :refer
             [>! <! >!! <!! put! take! chan]]
            [taoensso.nippy :as nippy]
            [com.rpl.nippy-serializable-fn]))

(defn compile*
  "Read one or more expresssions and compile them, as if by load-file without the need
   for a file."
  [expr & exprs]
  (let [txt (if (seq exprs)
              (reduce str (interpose "\n" (into [expr] exprs)))
              (str expr))]
    (with-open [rdr (clojure.java.io/reader (char-array txt))]
      (clojure.lang.Compiler/load ^java.io.Reader rdr))))

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
  (Class/forName "[B")
  (as-function [this]
    (nippy/thaw this))
  clojure.lang.AFn
  (as-function [this] this)
  String
  (as-function [this]
    (ns-resolve *ns* (symbol this)))
  clojure.lang.Symbol
  (as-function [this]
    (ns-resolve *ns* (symbol this)))
  clojure.lang.Keyword
  (as-function [this]
    (ns-resolve *ns* (symbol (name this)))))

;;function name pattern

;;eval pattern probably not necessary...
(def eval-regex  #"eval[0-9]+")
(def fn-regex #"fn__.+")

;;if it's a repl eval, it will have 3 entries.  an
;;inner class called eval something, then the function class.
(defn fn->symbol [f]
  (let [classes (-> f
                    str
                    (clojure.string/split #"\$"))
        l (classes 0)
        r (case (count classes)
            2 (classes 1)
            (classes 2))
        [r _] (clojure.string/split r #"\@")
        anon? (some identity (map #(or (re-find eval-regex %)
                                       (re-find fn-regex %)) classes))]
    ;;need to convert underscores in classname to dashes for symbol.
    (-> (symbol l (clojure.string/replace r "_" "-"))
        (with-meta {:anon anon?}))))

#_
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

;;we can also store known functions in a map on the cluster.
;;clients can fetch them 1x...

;;maybe the workflow is:
;;prior to mapping, if it's an anon function, serialize and store
;;on the cluster.

;;send a serialized representation of the function reference as a task
;;client gets the task, looks up the function, deserializes it
;; if it's a named function, we just resolve it.
;; if it's an anonymous function, we look resolve it from the cluster (maybe?),
;;   or we just check the payload in the record.

;;client retains a map of {symbol anon}, lookup the symbol at runtime.
;;if not known, deserialize the payload, cache the function, then
;;use cached fn going forward.  I think we don't care about
;;sending 1k over the network.  Maybe serializing matters for perf,
;;maybe not.

#_#_
(fnref myfn)
{:symbol-fn 'hazeldemo/myfn :body nil}

#_#_
(let [f (fn [x] (+ 1 x))] (class f))
hazeldemo.utils$eval21729$f__21730

#_#_
;;just assign a symbol name to it
(fnref (fn [x] (+ 2 3)))
{:symbol-fn 'hazeldemo.utils$eval21729$f__21730
 :body (nippy/freeze (fn [x] (+ 2 3)))}

#_
(let [f (fn ([x y z] (+ x y z)) ([x y] (+ x y))) fr (function-ref f)]
  (nippy/freeze-to-file "demo.bin" fr))

(def function-cache (atom {}))

(defn box ^objects [^objects arr x]
  (let [_ (aset arr 0 x)]
    arr))

(defn unbox [^objects xs]
  (aget xs 0))

(defrecord fnref [symbol-fn body ^objects f]
  IRemote
  (as-function [this]
    (let [fv (unbox f)]
      (if-not (identical? fv ::pending)
        fv
        (if-let [fv (@function-cache symbol-fn)]
          (do (aset f 0 fv)
              fv)
          (let [new-fn (or (some-> (resolve symbol-fn) var-get)
                           (nippy/thaw body))
                _  (swap! function-cache assoc symbol-fn new-fn)
                _  (aset f 0 new-fn)]
            new-fn)))))
  clojure.lang.IFn
  (invoke [this] ((as-function this)))
  (invoke [this arg0]
    ((as-function this) arg0))
  (invoke [this arg0 arg1]
    ((as-function this) arg0 arg1))
  (invoke [this arg0 arg1 arg2]
    ((as-function this) arg0 arg1 arg2))
  (invoke [this arg0 arg1 arg2 arg3]
    ((as-function this) arg0 arg1 arg2 arg3))
  (invoke [this arg0 arg1 arg2 arg3 arg4]
    ((as-function this) arg0 arg1 arg2 arg3 arg4))
  (invoke [this arg0 arg1 arg2 arg3 arg4 arg5]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5))
  (invoke [this arg0 arg1 arg2 arg3 arg4 arg5 arg6]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6))
  (invoke [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7))
  (invoke [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8))
  (invoke [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9))
  (invoke  [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10))
  (invoke  [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13 arg14))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14 arg15]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13 arg14 arg15))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14 arg15 arg16]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13 arg14 arg15 arg16))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14 arg15 arg16 arg17]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13 arg14 arg15 arg16 arg17))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14 arg15 arg16 arg17 arg18]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14 arg15 arg16 arg17 arg18 arg19]
    ((as-function this) arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
     arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19))
  (invoke
    [this arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12
     arg13 arg14 arg15 arg16 arg17 arg18 arg19 args]
    (apply this  (list* arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10
                        arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19 args)))
  (applyTo [this args]))

(defn function-ref [x]
  (cond (symbol? x)
        (let [fv   (resolve x)
              f    (var-get x)
              symb (fn->symbol f)]
          (->fnref symb nil (object-array [::pending])))
        (fn? x)
        (let [symb (fn->symbol x)
              _ (println [symb (meta symb)])]
          (->fnref (with-meta symb nil) (when (-> symb meta :anon)
                                          (nippy/freeze x)) (object-array [::pending])))
        :else (throw (ex-info "unknown function input" {:in x}))))


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
