;;client may not need to spool up its own cluster...
;;we always create a cluster of one for now for convenience,
;;but that may be unnecessary or undesirable going forward.
(ns hazeldemo.client
  (:require [chazel.core :as ch]
            [hazeldemo.core :as core]))

;;all of these are going to be client side I think...
(def ^:dynamic *pending* (ref {}))

(defn clear-pending! []
  (dosync (ref-set  *pending* {})))

;;this should probably be client side, since that's where the promise exists.
(def responder
  (ch/add-entry-listener core/results (core/->response-listener core/results *pending*)))

;;returns a promise.
(defn invoke
  ([source f args]
   (dosync
    (let [some-id (core/uuid)
          result  (promise)]
      (core/request-job! source {:id some-id :data {:type :invoke :args [f args]} :response some-id})
      (alter *pending* assoc some-id result)
      result)))
  ([f args] (invoke core/*cluster* f args)))


;;this works fine.
#_
(core/with-client [tmp]
  (let [log (core/get-object :log)
        id (ch/add-message-listener log (fn [msg] (println "I ALSO SEE YOU!" msg)))]
    (ch/publish log "HAHA")
    (Thread/sleep 100) ;;this is janky, we need a response or timeout to shutdown.
    (ch/remove-message-listener log id)))

;;equivalent
#_
(core/with-client [tmp]
  (let [l (core/get-object :log)]
    (core/with-message-listeners l
      [hello (fn [msg] (println ["HELLO FROM TEMPORARY!" msg]))]
      (ch/publish l "world")
      (Thread/sleep 100))))


;;basic workflow....
#_
(let [p   (invoke '+ [1 2 3])  ;invoke a function workers know.
      res (core/poll-queue!! core/do-job 1 core/jobs) ;drain the workqueue.
      ]
  @p)
;;["9a1a7f56-9030-443c-bff3-54b02f01d60f" #<Promise@2cfa5667: 6> nil]

;;works, p allows us to synchronize.
#_
(core/with-client [tmp]
  (let [jobs (core/get-object tmp :jobs)]
    (let [p   (invoke '+ [1 2 3])  ;invoke a function workers know.
          res (core/poll-queue!! core/do-job 1 jobs) ;drain the workqueue.
          ]
      @p)))

;;we probably want some lifecycles....
;;like tell the cluster to spawn a workgroup (e.g. some threads).

;;Alternately, we just have the number of threads exist independently during
;;server setup. E.g., if you invoke the server namespace, you spawn the hardware
;;threads at that time. e.g., (start-workers *cluster* 10) to spool up 10 worker threads that will pull work off the jobs queue.

;;basically workflow:
;;assume we have a fixed set of workers connected externally (e.g.
;;startup via a script, and invoke something within the worker ns.)
;;in the case of m4 we would just require m4 so it's classpathed,
;;and then provide fully-qualified symbols to the functions we
;;want to invoke.

;;from a client, we can submit work via invoke and get a promise back.
;;So to implement a distributed map, dmap, we would:
;;  for each item of work, reify it as an invocation job
;;  that can be called via `invoke` and capture the promise.
;;
;;  since we already have a global (currently) responder
;;  that listens for responses, delivers promises, and deletes
;;  the communcation medium (a distributed map entry),
;;  we don't have to handle ad-hoc listeners (still an option).
;;
;;  Living in promise land, we just have a multiplexed operation
;;  over one or more promises and we await their completion.

;;  Client has no idea how the work is being done, only delivery.

;;  maybe we always ensure there is at least 1 worker, supplied
;;  by the client? so that progress can be made....

;;  so we trivially port pmap into dmap (for distributed-map).
;;  This is a non-order-preserving mapping operation invoked
;;  from the client.  The difference here is that f needs to
;;  be a symbol or something that can be resolved to a symbol
;;  that the cluster resolve into an invocation.

;;we can probably defn a little wrapper to use reflection
;;to try to resolve functions as dfuncs and extract their
;;qualified symbol....

;;Don't really want to go down the reflection angle if not
;;necessary, but it could be generally useful.

;;use clojure.reflect, find member where name =
;;declaring class, that is ctr class, turn into
;;qualified symbol.

;; (defn symbolize [f]
;;   (if (fn? f)
;;     ))
;; (defn dmap [f xs]
;;   (doall (map (fn [x]
;;                 )))
;;   )



