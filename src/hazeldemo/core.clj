(ns hazeldemo.core
  (:require [chazel.core :as ch]))

(ch/cluster-of 1 :name "dev")

(def app1 (ch/hz-map :app1))
(defn ping []
  (println "ping!"))

(def this-ns *ns*)

(defn interpret [args]
  (binding [*ns* this-ns]
    (eval (read-string args))))

(ch/put! app1 :apple 42)

#_
(defn invoke-all [& args]
  (.executeOnAllMembers exec-svc (Rtask. fun)))
