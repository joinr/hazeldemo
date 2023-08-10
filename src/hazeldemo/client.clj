;;client may not need to spool up its own cluster...
;;we always create a cluster of one for now for convenience,
;;but that may be unnecessary or undesirable going forward.
(ns hazeldemo.client
  (:require [chazel.core :as ch]
            [hazeldemo.core :as core]
            [hazeldemo.utils :as u]
            [hazeldemo.worker :as work]
            [clojure.core.async :as a :refer
             [>! <! >!! <!! put! take! chan]]))

;;might make sense to have multiple clients at some point.
;;I think we can alleviate the burden of client lifecycling
;;a bit by retaining a persistent client and defaulting to it.

(defonce me (ch/client-instance core/+config+) )
(def ^:dynamic *client* me)

(defmacro on-cluster [& body]
  `(binding [~'hazeldemo.core/*cluster* *client*]
     ~@body))

;;all of these are going to be client side I think...
(def ^:dynamic *pending* (ref {}))

(defn clear-pending! []
  (dosync (ref-set  *pending* {})))

;;this should probably be client side, since that's where the promise exists.
;;results should be client-bound....
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

;;we can also have a separate invocation protocol.
;;instead of the promisory mechanism, work can be invoked on a work-queue.
;;In this case, the client needs to define the queue ahead of time, and then
;;the core/do-work function can infer the response (or we submit added data to guide
;;processing).

;;like invoke, except we submit jobs and indicate the result should be
;;enqueued onto the target.
(defn invoke-send
  ([source id f args]
   (core/request-job! source
                      {:id id :data {:type :invoke :args [f args]} :response id :response-type :queue}))
  ([id f args] (invoke-send *client* id f args)))

;;In this case, we get a direct abstraction between channels/queues.  I don't think
;;it makes sense for 1-off channels though because we end up with a lot of distributed
;;queues that may be unnecessary.  jobs are scheduled on a unified queue infrastructure.
;;Delivery should be specified by the job.

;;In this case, the entire listener requirement is side-stepped, since we have a handle
;;on the queue through the client, we now have an open channel to receive results.
;;The only question is indicating closure semantics, and if that is necessary for
;;the workers to do their job.  If the client decides to cancel work, then we need
;;to indicate it to the workers somehow.  Probably a map entry.  What if later
;;work is pending an the queue no longer exists (has been deleted by client)?
;;Work should be ignored.

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

;;basic workflow:
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

;;it would be nice to define cluster values, e.g.
;;if we put data onto the cluster, we can refer to that
;;data by name and let the cluster read.  Maybe we push
;;a bunch of state for coordination.

;;We just need a means to resolve it on the cluster or
;;at least codify cluster-local data....maybe a record
;;and protocol that the workers participate in.

;;Then we have some locality.

;;we will run into restrictions on what can be passed as an argument
;;to the function via serialization.  Things like atoms/refs and
;;other references may not be viable (maybe a solution is to clone
;;the reference to the cluster and let the cluster work with it internally
;;I dunno).

;;Interesting question: do we want to retain the client until
;;all promises are completed?  Should we manually close the client,
;;e.g. on user interaction?  Similar issue with readers/writers.
;;The client is a resource that has to be managed, since promise
;;delivery is tied to it.  The dumb way is to just wait for
;;everything to be delivered.  We can serially map and preserve
;;order etc.

;;Another option is to retain a client and reuse it.
;;If we already have one that meets the bill, with-client can
;;just re-use it.  Since our use-case is pretty simple for the
;;moment, we just have an ephemeral client.  Maybe we can
;;timeout or do some resource management, but it is probably
;;less important than holding resources like file locks and
;;readers (having a client occupies a port but that's it I think).

;;caveman way.
(defn dmap
  ([client f xs]
   (let [fsym (u/symbolize f)]
     (on-cluster client
                 (->> xs
                      (map (fn [x] (invoke fsym [x]))) ;;don't want intermediate vecs but meh...
                      (map deref)
                      (doall)))))
  ([f xs]
   (dmap *client* f xs)))

(comment
  (def xs (future (dmap inc (range 10))))
  (future-done? xs) ;;false if no workers are running.
  ;;coerces work to be executed (manually, normally workers
  ;;would be doing this in a thread)
  (core/poll-queue!! core/do-job 1 core/jobs)
  ;;should see the client disconnect after the seq realizes
  ;;and the future completes...
  (future-done? xs)
  )

;;another, better option, is to do the producer/consumer queue and deliver
;;results on a channel.
;;we can put results on a queue, and strap a client that pulls results from
;;the queue or times out.  We need a sentinel value to inidicate channel
;;closure, or we can store that information on the cluster somewhere (although
;;it seems like we want some identity).  Having the channel abstraction
;;opens up the entire cluster to core.async for us....

;;since go routines are lightweight, and the heavy lifting is being done
;;by the cluster - we are just awaiting a response, we can spool up
;;a go block for every pending promise and have an async wait with
;;timeout (and total timeout if we want...) as well as cleanup/cancellation
;;routines.  There can be a common channel to deliver results to.

;;note: the semantics for establishing a client are to return
;;nil if no cluster can be found...

;;TBD -> Povide a cluster-native channel abstraction that dumps to a queue,
;;likely with the map/listener implementation of promises used to indicate
;;channel closure or otherwise.  Or use some sentinel value to indicate
;;closure and rely on the client/consumer to remove the channel when it's done.

;;create or acquire a named queue on the cluster.
;;by default it will be unbounded.
;;if no name is supplied, make a uuid and register it as the queue.
;;add the name to the open-channels map.
;;the channel semantics queue-side are implemented using
;;the open-channels map to indicate whether queue can have items
;;pushed to it (closed items are fine)

;;When a queue is closed, we add the the sentinel :queue/closed
;;as the last item, then remove the queue from the open-channels map.

;;queues that are both closed and empty are deleted on access of the
;;sentinel (by the calling process)


;;start a go block that pulls items from the queue
;;it's possible that the local client has a reference to a datstructure
;;that does not exist anymore.  Need to deal with that.

(def +closed+ :queue/closed)
(defn acquire-queue [source id]
  (if-let [obj (core/get-object source id)]
    obj
    (let [q (ch/hz-queue id source)
          ^java.util.Map
          open-channels (or (core/get-object source :open-channels)
                            (ch/hz-map :open-channels source))
          _ (.put ^java.util.Map open-channels id true)]
      q)))

(let [stdout *out*]
  (defn log! [msg]
    (binding [*out* stdout]
      (println msg))))

;;acquire a channel that is fed from a blocking queue on the cluster.
;;WIP doesn't work all the time!
(defn cluster-channel-out
  ([source id xf]
   (let [^java.util.concurrent.BlockingQueue q      (acquire-queue source id)
         out    (if xf
                  (chan Long/MAX_VALUE xf) ;;temporary, want to have caller configure
                  (chan Long/MAX_VALUE))
         worker (future (loop []
                          (if-let [v (.take q)] ;;blocks.
                            (if-not (= v +closed+)
                              ;;put the value on the channel
                              (do (>!! out v)
                                  (recur))
                              ;;delete the queue, stop working.
                              (do (core/destroy! source id)
                                  (a/close! out))))))]
     ;;if we close out before worker, then we should stop the thread.
     out))
  ([source id] (cluster-channel-out source id nil)))

;;returns a clojure channel where puts trigger items being copied to the
;;remote queue on the cluster (simulating a distributed channel).
;;core.async semantics for closing the channel apply; if in is closed,
;;then the corresponding queue "channel" is closed as well. once
;;elements from the cluster are drained (from a corresponding out
;;channel), then the queue is deleted from the cluster.

;;WIP doesn't work all the time!
(defn cluster-channel-in
  ([source id xf]
   (let [^java.util.concurrent.BlockingQueue q      (acquire-queue source id)
         in  (if xf
               (a/chan Long/MAX_VALUE xf)
               (a/chan Long/MAX_VALUE))
         stdout *out*
         log (a/chan (a/dropping-buffer 1) (map (fn [x] (binding [*out* stdout]
                                                          (println [:log x])
                                                          x))))
         worker (future (loop []
                          (if-let [v (<!! in)] ;;blocks.
                            ;;put the value on queue.
                            (do (.put q v)
                                (recur))
                            ;;if in is closed, we stop pulling.
                            ;;queue is no longer open either, but
                            ;;may have elements remaining to be drained.
                            (let [_ (.put q +closed+)
                                  ^java.util.Map
                                  m (core/get-object source :open-channels)]
                              (.remove m id)))))]
     in))
  ([source id] (cluster-channel-in source id nil)))



;;working
(comment
  (def result-chan (dmap> inc (range 10)))
  ;;coerces work to be executed (manually, normally workers
  ;;would be doing this in a thread)
  (core/poll-queue!! core/do-job 1 core/jobs)
  (a/into [] result-chan))


;;with a worker...
(comment
  (def result-chan (dmap> inc (range 100)))
  ;;coerces work to be executed (manually, normally workers
  ;;would be doing this in a thread)
  (<!! (a/into [] result-chan))
  )

(comment
  (->> (range 100)
       (dmap! inc)))


#_
(->> (range 100)
     (map (fn [x] (read-string "(rand-int 100)")))
     (dmap! *client* clojure.core/eval)
     (a/into [])
     <!!)


;;let's create another way to do this for testing.
;;lower level, simpler.
(defn dmap-future
  "Maps f over xs, yielding a future where a vector of results will be
   delivered."
  ([source f xs]
  ;;create a new queue, no channels.
  ;;push jobs to the jobs queue.
  ;;tell workers to push results to the queue (already doing this).
  ;;loop and pull results from the queue.
  ;;when we get all the results from the queue, we close the queue and delete it.
   (let [id (str "queue" (core/uuid))
         fsym (u/symbolize f)
        ^java.util.concurrent.BlockingQueue
         new-queue (acquire-queue source id)
         jobs (core/get-object source :jobs)]
     (future
       (let [n (reduce (fn [acc x]
                         (core/request-job! source jobs
                              {:id id :data {:type :invoke :args [fsym [x]]} :response id :response-type :queue})
                         (unchecked-inc acc)) 0 xs)]
         (loop [n   n
                acc []]
           (if (pos? n)
             (let [x (.take new-queue)]
               (recur (unchecked-dec n)
                      (conj acc x)))
             (do (core/destroy! source id)
                 acc)))))))
  ([f xs] (dmap-future *client* f xs)))

(defn dmap>
  "Like dmap! but provides a channel where results may be consumed as they are produced."
  ([source f xs]
  ;;create a new queue, no channels.
  ;;push jobs to the jobs queue.
  ;;tell workers to push results to the queue (already doing this).
  ;;loop and pull results from the queue.
  ;;when we get all the results from the queue, we close the queue and delete it.
   (let [id (str "queue" (core/uuid))
         fsym (u/symbolize f)
        ^java.util.concurrent.BlockingQueue
        new-queue (acquire-queue source id)
        out  (a/chan Long/MAX_VALUE)]
     (a/thread
       (let [n (reduce (fn [acc x]
                         (core/request-job! source
                                            {:id id :data {:type :invoke :args [fsym [x]]} :response id :response-type :queue})
                         (unchecked-inc acc)) 0 xs)]
         (loop [n   n]
           (if (pos? n)
             (let [x (.take new-queue)
                   _ (a/put! out x)]
               (recur (unchecked-dec n)))
             (do (core/destroy! source id)
                 (a/close! out))))))
     out))
  ([f xs] (dmap> *client* f xs)))

(defn dmap!
  ([source f xs]
   (->> (dmap-future source f xs)
        deref))
  ([f xs] (dmap! *client* f xs)))


(defn drain!
  (^long [^java.util.concurrent.BlockingQueue q]
   (drain! q (java.util.ArrayList.)))
  (^long [^java.util.concurrent.BlockingQueue q ^java.util.ArrayList coll]
   (.drainTo q coll)))

(defn dmap-future-batch
  "Maps f over xs, yielding a future where a vector of results will be
   delivered."
  ([source f xs]
  ;;create a new queue, no channels.
  ;;push jobs to the jobs queue.
  ;;tell workers to push results to the queue (already doing this).
  ;;loop and pull results from the queue.
  ;;when we get all the results from the queue, we close the queue and delete it.
   (let [id (str "queue" (core/uuid))
         fsym (u/symbolize f)
        ^java.util.concurrent.BlockingQueue
         new-queue (acquire-queue source id)
         jobs (core/get-object source :jobs)]
     (future
       (let [total (->> xs
                    (eduction (map (fn [x]
                                     {:id id :data {:type :invoke :args [fsym [x]]}
                                      :response id :response-type :queue})))
                    (core/request-jobs! source jobs))]         ;;we repeatedly drainTo an intermediate collection until we get all the
         ;;results, or timeout trying.  Basically implement our own poll.
         (loop [n   total
                acc (java.util.ArrayList.)]
           (if (pos? n)
             (let [k (drain! new-queue acc)
                   _ (core/log! [:drained k])]
               (cond (zero? k)
                     (let [x (.take new-queue)]
                       (recur (unchecked-dec n) (doto acc (.add x))))
                     :else (recur (- n k) acc)))
             (do (core/destroy! source id)
                 acc)))))))
  ([f xs] (dmap-future-batch *client* f xs)))
;;possibly more elegant, using channels, no waiting on promises, some extra
;;copying though.  Might be able to eliminate extra copies if we
;;extend channel impl to the cluster queue directly...
;;allows incremental progress instead of waiting on all promises.


(defn dmap!!
  ([source f xs]
   (->> (dmap-future-batch source f xs)
        deref))
  ([f xs] (dmap!! *client* f xs)))
;;channel-based variants that failed stochastically.  Replaced with simpler
;;direct queue-managed options.
(comment
  (defn dmap>
  ([source f xs]
   (let [fsym (u/symbolize f)
         id   (keyword (str "queue-" (core/uuid))) ;;get-object was finnicky...
         responses (atom 0)
         stdout *out*
         out  (cluster-channel-out source id
               (map (fn [x] (swap! responses unchecked-inc)
                      x)))
         n    (reduce (fn [acc x]
                        (invoke-send source id fsym [x])
                        (unchecked-inc acc)) 0 xs)
         ;;when no more are remaining we should close by sending a close signal.
         _  (add-watch responses :close-chan
                       (fn closer [acc k v0 v1]
                         (when (= v1 n)
                           (let [^java.util.concurrent.BlockingQueue
                                 q (core/get-object source id)
                                 ^java.util.Map
                                 oc (core/get-object source :open-channels)]
                             (.put q +closed+)
                             (.remove oc id)))))]
     out))
  ([f xs] (dmap> *client* f xs)))

(defn dmap!
  ([source f xs]
   (->> (dmap> source f xs)
        (a/into [])
        (<!!)))
  ([f xs] (dmap! *client* f xs)))

)

;;simple remote eval:
(comment
  (def res (invoke 'eval ['(+ 2 3)]))
  )


;;use executor service implementation....see if this is faster,
;;examine downsides.
;;This is about 73x faster than the queue-based implementation,
;;around 50x slower than in-memory pmap (2570x slower than single-core map...).
(defn fmap [f coll]
  (let [n    10
        rets (map #(ch/ftask  (partial f %)) coll)
        step (fn step [[x & xs :as vs] fs]
               (lazy-seq
                (if-let [s (seq fs)]
                  (cons (deref x) (step xs (rest s)))
                  (map deref vs))))]
    (step rets (drop n rets))))

(defn fmap2
  ([n size f coll]
   (let [rets (map #(ch/ftask  (partial mapv f %)) (partition size coll))
         step (fn step [[x & xs :as vs] fs]
                (lazy-seq
                 (if-let [s (seq fs)]
                   (concat (deref x) (step xs (rest s)))
                   (mapcat deref vs))))]
     (step rets (drop n rets)))))


(defn eval-all! [expr]
  (let [res (ch/ftask (partial eval expr) :members :all)]
    (doseq [[m v] res]
      (println [m @v]))))

(defn compile-all! [expr]
  (let [res (ch/ftask (partial apply hazeldemo.utils/compile* expr) :members :all)]
    (doseq [[m v] res]
      (println [m @v]))))

;;playing with fmap
(comment
  (defn noisy-inc [n]
    (let [mem  (.. core/*cluster* getCluster getLocalMember str)]
      ))


  )
#_#_
(defmacro get-f! [src]
  `(if ~'f ~'f
       (let [func# (eval ~src)]
         (set! ~'f func#)
         func#)))

(defrecord psuedofn [^String src ^{:tag 'clojure.lang.IFn :volatile-mutable true} f]
  clojure.lang.IFn
  (invoke [this]
    ((get-f! 'src)))
  (invoke [this arg]
    ((get-f! 'src) arg)))

;;we also have issues with the client side bindings.  In some cases for legacy
;;control flow, we uses bindings for nested stuff in the api.

;;we want something like bound-fn, but with the semantics of resolving
;;symbols if necessary.
