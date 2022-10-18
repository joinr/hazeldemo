(ns hazeldemo.core
  (:require [chazel.core :as ch]
            [hazeldemo.utils :as u]))

(defonce me (first (ch/cluster-of 1 :name "dev")))

(def ^:dynamic *cluster* me)

;;utils
;;=====
(defn get-objects
  "Scrapes all the distributed objects from the cluster."
  ([source]
   (into {}
         (for [itm (ch/distributed-objects source)]
           [(keyword (.getName itm)) itm])))
  ([] (get-objects *cluster*)))

(defn unstring [k]
  (if (string? k)
    (keyword k)
    k))
(alter-var-root #'unstring u/memo-1)

;;looks for jobs topic
(defn get-object
  ([source k] (-> source get-objects (get k)))
  ([k] (get-object *cluster* k)))

(defn destroy!
  ([source k]
   (when-let [obj (get-object source k)]
     (.destroy obj)))
  ([k] (destroy! *cluster* k)))

;;enables publish on client topics.
(extend-type com.hazelcast.client.impl.proxy.ClientReliableTopicProxy
  ch/ReliableTopic
  (add-reliable-listener [t f opts]
    (.addMessageListener t (ch/reliable-message-listener f opts)))
  ch/Topic
  (add-message-listener [t f]
    (.addMessageListener t (ch/message-listener f)))
  (remove-message-listener [t id]
    (.removeMessageListener t id))
  (publish [t msg]
    (.publish t msg))
  (local-stats [t]
    (.getLocalTopicStats t))
  (hz-name [t]
    (.getName t)))

;;basic client ops..
;;==================
(def +config+ {:cluster-name "dev"
               :hosts ["127.0.0.1"]})

;;note: when we get distributed objects, they are proxies that are
;;dependent on a client/cluster connection.  So they return eagerly,
;;but getting values from them is on-demand/lazy.  It is possible
;;to close the connection before using the proxy values.  So...
;;any result that returns a distributed object will error if the underlying
;;client connection has been shut down.  Ergo, do all your work on distributed
;;database objects eagerly.  Another option is to extend deref to
;;distributed datastructures.  This is useful if we want to do a
;;limited scope computation an then disconnect.
;;[What if a client isn't found?]
;; -we should throw if no client is detected.
(defmacro with-client
  [[client & [config]] & body]
  `(let [~client (ch/client-instance ~(case config (nil :default) +config+ config))]
     (binding [~'hazeldemo.core/*cluster* ~client]
       (let [res# ~@body]
         (ch/shutdown-client ~client)
         res#))))

;;temporary message listeners
(defmacro with-message-listeners [topic lbinds & body]
  (assert (even? (count lbinds)))
  (let [pairs (partition 2 lbinds)
        names (mapv first pairs)
        binds (reduce (fn [acc [l r]]
                        (conj acc l r)) []
               (for [[l r] pairs]
                 [l `(ch/add-message-listener ~topic ~r)]))]
    `(let [~@binds
           t#   ~topic
           res# (do ~@body)]
       (doseq [l# ~names]
         (ch/remove-message-listener t# l#))
       res#)))

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

;;message topic to indicate work arrived and the
;;job queue needs to be drained by workers.
(def arrived
  (or (get-object :arrived)
      (ch/hz-reliable-topic :arrived)))

(def jobs
  (or (get-object :jobs)
      (ch/hz-queue :jobs)))

(def results
  (or (get-object :results)
      (ch/hz-map :results)))

(def log
  (or (get-object :log)
      (ch/hz-reliable-topic :log)))

;;need a way to clear message listeners (this provides guid for clearing)
(defonce log-id (ch/add-message-listener log (fn [msg] (println [:LOG msg]))))

;;for our purposes, we can simulate promises with a map where
;;the "channel" is a guid key and an entry.
;;We can add an entry listener to determine when known entries show up/
;;are delivered.  When they appear we can then deliver the promise (or close
;;a channel if we want the core.async model...)
(def results
  (or (get-object :results)
      (ch/hz-map :results)))


;;The lifecycle of requesting an invocation and awaiting a response
;;is like Amit's implementation, instead we use the distributed map
;;to contain response values.

;;The lifecycle goes:
;;generate a unique id for the map to store the result.
;;request the job on the cluster:
;;   -queue the work, which will be picked up by workers
;;   -job includes optional response (if value desired)
;;   -worker does job, evals result, puts result in map if response.
;;   -caller is notified....
;;    -response topic?
;;    -entry listener?

;;If there's an entry listener, i guess it runs locally on the client.
;;So actual usage would be to invoke a client as a psuedo threadpool.
;;then client is used to add listeners.
;;Performance could degrade if we have thousands of listeners.
;;    --entry listener or topic handler delivers promise, deletes entry.


;;could consolidate entry listener on client, have a response map (I think
;;Ammit did something like this).

;;naively, it would have a map of (response-id -> promise)
;;Then we have a single listener:

;;results is needed....
(defn handle-response [^java.util.Map m pending k v ov]
  ;;if it's a pending response, we deliver it.
  (dosync
   (when-let [p (@pending k)]
       (do (deliver p v)
           (.remove m k)
           (alter pending dissoc k)))))

(defn ->response-listener
  ([m pending]
   (ch/entry-added-listener
    (fn [k v ov] (handle-response m pending k v ov)))))

;;so we need to plumb ->listener to results 1x and then it
;;acts on every entry.  Another option is to create many singleton maps
;;per-response (or temporary response queues).  Let's see how the map
;;entry route works for now.

;;In theory we have many promises in flight

(defn uuid [] (str (java.util.UUID/randomUUID)))
(defn request-job!
  ([source {:keys [id data response response-type] :as m}]
   (if-let [^java.util.concurrent.BlockingQueue jobs (get-object source :jobs)]
     (do (.put jobs m)
         (ch/publish arrived {:new-work id})
         response)
     (throw (ex-info "cannot find jobs queue on source" {:source source :args m}))))
  ([data] (request-job! *cluster* data)))


;;we can wrap this up in a convenience macro.
;;when you want to run a bunch of computations on the cluster...
;;create a client,
;;create a pending result queue (or inherit one),
;;submit work to the cluster,
;;await responses.
;;since responses are delivered as promises (could also use
;;channels), we just sift through them polling until all are delivered
;;or we timeout.

(def ^java.util.concurrent.TimeUnit ms java.util.concurrent.TimeUnit/MILLISECONDS)

;;we want to loop through the queue doing work and pushing results until
;;thte queue is empty.  take will block, poll will timeout.
(defn poll-queue!!
  ([f timeout ^java.util.concurrent.BlockingQueue in ]
   (if-let [job (.poll in timeout ms)]
     (do (f job)
         (recur  f timeout in))
     ::empty))
  ([f in] (poll-queue!! f 2000 in)))

;;our jobs are of the form
;;{:id str :data {:keys [type args response]} :response str}

;; {:id "87f8a1fb-74e8-4b3d-93d7-8eb87864518b",
;;  :data {:type :invoke,
;;         :args [clojure.core/+ [1 2]]},
;;  :response "87f8a1fb-74e8-4b3d-93d7-8eb87864518b"}

;;a dumb message handler.
(defn do-job [{:keys [id data response response-type] :as job}]
  (let [{:keys [type args]} data
        res   (case (data :type)
                :add    (apply + args)
                :ping   (println "ping!")
                :log    (ch/publish log args)
                :invoke (let [[fname  params] args]
                          (try (apply (u/as-function fname) params)
                               (catch Exception e e))))]
    (when response ;;we can overload this to allow us to push to queues easily.
      (case response-type
        (nil :map) (.set ^java.util.Map results response res)
        :queue     (.put (ch/hz-queue id *cluster*) res)
        (throw (ex-info "unknown response-type!" {:response-type response-type :in job}))))
    res))

;;hazeldemo.core> (do-job {:id "blah" :data {:type :invoke :args ["clojure.core/+" 1 2 3]}})
;;6

;;we want to listen to the arrived topic and if any jobs have arrived
;;and we are not working, go drain the queue.

(def work-state  (atom nil))
(defn await-jobs!!
  ([handler timeout in]
   (if-not @work-state ;;not working yet.
     (let [_   (reset! work-state ::working)
           res (poll-queue!! timeout handler in)
           _   (reset! work-state nil)]
       res)))
  ([timeout in] (await-jobs!! do-job timeout in))
  ([in]  (await-jobs!! do-job 500 in))
  ([]    (await-jobs!! do-job 500 jobs)))


;;we want an api function start-workers that will
;;setup a work queue handling responses on the cluster.
