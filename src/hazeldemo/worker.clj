;;ns to define starting and stopping worker pools
;;that will operate on a shared message queue.
(ns hazeldemo.worker
  (:require [chazel.core :as ch]
            [hazeldemo.core :as core]))

