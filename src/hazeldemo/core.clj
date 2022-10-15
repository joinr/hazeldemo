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

;;let's create a message topic...
;;it's possible we are a work node joining a pre-existing
;;cluster, so we want to link to the existing topic if it already exists.
;;I don't know what the behavior is if we create duplicate topics.
;;looks like maybe it gets nuked.

;;establish a couple of common topics:
;;jobs, results, and log
;;we probably want log to be limited or have something dumping
;;to disk.
(def jobs
  (or (my-object :jobs)
      (ch/hz-reliable-topic :jobs)))

(def results
  (or (my-object :results)
      (ch/hz-reliable-topic :results)))

(def log
  (or (my-object :log)
      (ch/hz-reliable-topic :log)))

(defn ping [] (println "ping!"))

(defn do-job [{:keys [job id args]}]
  (case job
    :add [id (apply + args)]
    :ping (ping)
    nil))


(def this-ns *ns*)

(defn interpret [args]
  (binding [*ns* this-ns]
    (eval (read-string args))))


#_
(defn invoke-all [& args]
  (.executeOnAllMembers exec-svc (Rtask. fun)))
