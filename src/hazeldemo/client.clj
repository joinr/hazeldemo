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
