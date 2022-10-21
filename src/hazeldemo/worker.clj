;;ns to define starting and stopping worker pools
;;that will operate on a shared message queue.
(ns hazeldemo.worker
  (:require [chazel.core :as ch]
            [hazeldemo.core :as core]
            [hazeldemo.utils :as u]
            [clojure.core.async :as a :refer
             [>! <! >!! <!! put! take! chan]]))

;;we want to listen to the arrived topic and if any jobs have arrived
;;and we are not working, go drain the queue.

;;for visibility, we want workers to say who they are when
;;logging (what worker, what cluster member).

(def workers (atom {}))

(defn worker-log [id msg]
  (core/log! [(str "worker:" id core/addr) msg]))

(defn do-noisy-job
  ([wid {:keys [id data response response-type] :as job}]
   (worker-log wid [:handling job])
   (let [{:keys [type args]} data
         res   (case (data :type)
                 :add    (apply + args)
                 :ping   (println "ping!")
                 :log    (ch/publish core/log args)
                 :invoke (let [[fname  params] args
                               _ (worker-log wid [:invoking fname params])]
                           (try (apply (u/as-function fname) params)
                                (catch Exception e e))))]
     (worker-log wid [:response-pre response])
     (when response ;;we can overload this to allow us to push to queues easily.
       (worker-log wid [:reponse response])
       (case response-type
         (nil :map) (do (worker-log wid [:setting-result response res])
                        (.set ^java.util.Map core/results response res))
         :queue     (do (worker-log wid [:queuing-result response res])
                        (.put (ch/hz-queue id core/*cluster*) res))
         (do (worker-log wid [:error-on job])
             (throw (ex-info "unknown response-type!" {:response-type response-type :in job})))))
     res)))

;;workers can listen for new work arrivals and go poll for work.
(defn ->worker
  ([handler timeout source]
   (let [wid         (count @workers)
         active      (atom false)
         in          (a/chan (a/dropping-buffer 1))
         workthread  (a/thread
                       (loop []
                         (if-let [res (<!! in)]
                           (when  @active
                             (do (worker-log wid "polling!")
                                 (core/poll-queue!! handler timeout source)
                                 (recur)))
                           (println [:empty-channel :closing-worker wid]))))
         listener    (ch/add-message-listener core/arrived
                       (fn [msg] (a/put! in true)))
         new-worker {:id wid :in in :active active :workthread workthread :listener listener}]
     (reset! active true)
     (a/put! in true)
     new-worker))
  ([timeout source] (->worker core/do-job timeout source))
  ([source]  (->worker core/do-job 500 source))
  ([]    (->worker core/do-job 500 core/jobs)))


;;we want an api function start-workers that will
;;setup a work queue handling responses on the cluster.

(defn spawn-workers! [n & {:keys [source timeout handler]
                           :or {source core/jobs
                                timeout 500
                                handler core/do-job}}]
  (doseq [_ (range n)]
    (let [w (->worker handler timeout source)
          _ (println [:spawning-worker (w :id)])]
      (swap! workers assoc (w :id) w))))

(defn spawn-noisy-workers! [n & {:keys [source timeout]
                           :or {source core/jobs
                                timeout 500}}]
  (doseq [_ (range n)]
    (let [wid (count @workers)
          handler (fn noisy-handle [job]
                    (do-noisy-job wid job))
          w (->worker handler timeout source)
          _ (println [:spawning-worker (w :id)])]
      (swap! workers assoc (w :id) w))))

(defn pause [id]
  (some-> @workers (get id) :active (reset! false)))

(defn resume [id]
  (some-> @workers (get id) :active (reset! true)))

(defn kill [id]
  (do (some-> @workers (get id) :in a/close!)
      (swap! workers dissoc id)))

(defn pause-all! []
  (doseq [id (keys @workers)] (pause id)))
(defn resume-all! []
  (doseq [id (keys @workers)] (resume id)))
(defn kill-all! []
  (doseq [id (keys @workers)] (kill id)))
