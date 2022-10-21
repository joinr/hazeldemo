(ns hazeldemo.config
  (:require [chazel.core :as ch])
  (:import [com.hazelcast.config Config]))

(defn ->aws [id]
  (let [cfg (Config.)]
    (.. cfg (setInstanceName id))
    (.. cfg getNetworkConfig getJoin getMulticastConfig (setEnabled false))
    (.. cfg getNetworkConfig getJoin getAwsConfig       (setEnabled true))
    cfg))

;;add in support for ad-hoc tcp-ip networks
(defn ->tcp-ip [id & {:keys [required members]}]
  (let [cfg (Config.)]
    (.. cfg (setInstanceName id))
    (.. cfg getNetworkConfig getJoin getMulticastConfig (setEnabled false))
    (let [tcp     (.. cfg getNetworkConfig getJoin getTcpIpConfig)]
      (.. tcp (setEnabled true))
      (when required (.. tcp (setRequiredMember required)))
      (doseq [member members]
        (.. tcp (addMember member)))
      cfg)))

(defn ->default [id]
  (let [cfg (Config.)]
    (.. cfg (setInstanceName id))
    cfg))

;;derive based on env var HAZELCAST
(defn new-instance [id]
  (if-let [env (get (System/getenv) "HAZELCAST")]
    (if (= env "AWS")
      (ch/new-instance (->aws id))
      (ch/new-instance (->default id)))
    (ch/new-instance (->default id))))
