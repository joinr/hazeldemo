(ns hazeldemo.core
  (:require [chazel.core :as ch]))

(ch/cluster-of 1 :name "dev")

(def me (first (ch/all-instances)))


;;utils
;;=====
(defn my-objects
  "Scrapes all the distributed objects from the cluster."
  []
  (into {}
        (for [itm (ch/distributed-objects me)]
          [(keyword (.getName itm)) itm])))

;;looks for jobs topic
(defn my-object [k] (-> (my-objects) (get k)))

(defn destroy! [k]
  (when-let [obj (my-object k)]
    (.destroy obj)))
;;let's create a message topic...
;;it's possible we are a work node joining a pre-existing
;;cluster, so we want to link to the existing topic if it already exists.
;;I don't know what the behavior is if we create duplicate topics.
;;looks like maybe it gets nuked.

;;establish a couple of common topics:
;;jobs, results, and log
;;we probably want log to be limited or have something dumping
;;to disk.
;;So our basic paradigm will be:
;;we have a jobs queue.  We also have a work topic.
;;When we submit work to the queue, one or more items are pushed onto
;;the queue and we publish a notification to the work topic.
;;Workers are subscribed to the work topic.
;;Workers handle work topic notices by going to drain the queue.
;;They check for jobs on the queue, pull a job, submit a result,
;;and look to see if more work remains, repeating until the queue
;;is empty.

(def jobs
  (or (my-object :jobs)
      (ch/hz-queue :jobs)))

(def results
  (or (my-object :results)
      (ch/hz-queue :results)))

(def log
  (or (my-object :log)
      (ch/hz-reliable-topic :log)))

(ch/add-message-listener log (fn [msg] (println [:LOG msg])))

(defn ping [] (println "ping!"))

(def ^java.util.concurrent.TimeUnit ms java.util.concurrent.TimeUnit/MILLISECONDS)

;;we want to loop through the queue doing work and pushing results until
;;thte queue is empty.  take will block, poll will timeout.
(defn poll-queue!
  ([f timeout ^java.util.concurrent.BlockingQueue in ]
   (when-let [job (.poll in timeout ms)]
     (do (f job)
         (recur  f timeout in))))
  ([f in] (poll-queue! f 2000 in )))

(defn do-job [{:keys [job id args :as data]}]
  (case job
    :add  [id (apply + args)]
    :ping (ping)
    :log  (ch/publish log data)
    nil))

(def this-ns *ns*)

(defn interpret [args]
  (binding [*ns* this-ns]
    (eval (read-string args))))


#_
(defn invoke-all [& args]
  (.executeOnAllMembers exec-svc (Rtask. fun)))
