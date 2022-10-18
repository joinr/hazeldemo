;;client may not need to spool up its own cluster...
;;we always create a cluster of one for now for convenience,
;;but that may be unnecessary or undesirable going forward.
(ns hazeldemo.client
  (:require [chazel.core :as ch]
            [hazeldemo.core :as core]
            [hazeldemo.utils :as u]
            [clojure.core.async :as a :refer
             [>! <! >!! <!! put! take! chan]]))

;;might make sense to have multiple clients at some point.
;;I think we can alleviate the burden of client lifecycling
;;a bit by retaining a persistent client and defaulting to it.

(def ^:dynamic *client* (ch/client-instance core/+config+))

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
