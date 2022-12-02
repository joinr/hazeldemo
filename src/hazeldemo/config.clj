(ns hazeldemo.config
  (:require [chazel.core :as ch]
            [spork.util.io :as io])
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
      (doseq [member members] (.. tcp (addMember member)))
      cfg)))

(defn ->default [id]
  (let [cfg (Config.)]
    (.. cfg (setInstanceName id))
    cfg))

;;we would like to enable configuration via edn files and env vars.
;;our 2 common use-cases will be to have aws ec2 instances,
;;and a tcp-ip based setup with known ip addresses.
;;So we want to specify these options fairly easily in a local .edn file
;;as a clojure map.
(def +default-confg+
  {:id "dev"
   :join :multicast})

;;tcp
#_
{:id "dev"
 :join :tcp
 :members {:file/path some-file} | ["member1" "member2" ....]
 :required "some-member"
 :append-on-join? true|false}

;;want to allow member logging of ip's to a shared file.
;;add an option to append our IP to the members log, e.g.
;;if we have dynamic ip's.
;;opens up some interesting challenges that are out of scope.
;;we could have a sqlite db as well and just connect to it
;;to record info, but meh.  For no we will just use log files.
;;They are ephemeral and can be blasted if necessary.

;;for parsing, we can allow users to define a default config.
;;~/.chazel/chazel.edn, or a chazel.edn colocated in the
;;invoking directory (e.g. if running from a jar).

;;If an id is specified we can merge it, otherwise let
;;the user's id stand.  Allows peer-specified connection
;;configuration.

(defmulti parse-config (fn [m] (m :join)))

(defmethod parse-config :tcp [{:keys [id join members required]}]
    ;;members may be a vector of ip addresses or
    ;;a path to a registry of known members, line-delimited ip addresses.
    ;;registry
    (let [members     (cond (map? members)
                            (-> members :file/path slurp clojure.string/split-lines)
                        (vector? members) members
                        :else (throw (ex-info "expected a vector of string ips or map of {:file/path string}"
                                              {:in members})))]
  (->tcp-ip id :required required :members members)))

(defmethod parse-config :multicast [{:keys [id join multicast-port]
                                         :or {id "dev"}}]
  (let [res (->default id)]
    (when multicast-port
      (.. res getNetworkConfig getJoin getMulticastConfig (setMulticastPort multicast-port)))
    res))

(defmethod parse-config :aws  [{:keys [id]}]
  (->aws id))

;;places to look for config, in order.
(def default-paths ["hazelcast.edn" "~/.hazelcast/hazelcast.edn"])
(defn find-config! []
  (some (fn [path]
          (when (io/fexists? (io/file path))
            path)) default-paths))

(defn get-config!
  ([config-map] (parse-config config-map))
  ([] (-> (if-let [path (find-config!)]
            (do (println [:loading-config path])
                (-> (slurp (io/file path))
                    clojure.edn/read-string))
            (do (println [:no-config :using-default :multicast])
                {:id "dev"
                 :join :multicast}))
          get-config!)))


;;allow a couple of ways to do this:
;;look for hazelcast.edn,
;;or a global ~/.hazelcast/hazelcast.edn
;;loading the config from there.

;;If none is found, check env var HAZELCAST,
;;since we may set peers on AWS to indicate
;;an IAM aws connection.  This is trivially accomplished
;;with baked ENV vars and baked into the image.
;;Note: we could also define a hazelcast.edn and
;;set it up that way too.


;;derive based on env var HAZELCAST
(defn new-instance [id-or-map]
  (cond
    (map? id-or-map) ;;passed in maps override local config.
       (ch/new-instance (parse-config id-or-map))
    (string? id-or-map)
      (let [id id-or-map]
      ;;use env vars for cloud stuff by default.
        (if-let [env (get (System/getenv) "HAZELCAST")]
          (if (= env "AWS")
            (ch/new-instance (->aws id))
            (ch/new-instance (->default id)))
          (let [cfg (get-config!)]
            (do (.. cfg (setInstanceName id)) ;;merge id with local config.
                (ch/new-instance cfg)))))
        :else (throw (ex-info "unknown instance arg type!" {:in id-or-map}))))

;; <hazelcast>
;;     ...
;;     <network>
;;         <join>
;;             <auto-detection enabled="true" />
;;             <multicast enabled="false">
;;                 <multicast-group>224.2.2.3</multicast-group>
;;                 <multicast-port>54327</multicast-port>
;;                 <multicast-time-to-live>32</multicast-time-to-live>
;;                 <multicast-timeout-seconds>2</multicast-timeout-seconds>
;;                 <trusted-interfaces>
;;                     <interface>192.168.1.102</interface>
;;                 </trusted-interfaces>
;;             </multicast>
;;             <tcp-ip enabled="false">
;;                 <required-member>192.168.1.104</required-member>
;;                 <member>192.168.1.104</member>
;;                 <members>192.168.1.105,192.168.1.106</members>
;;             </tcp-ip>
;;             <aws enabled="false">
;;                 <access-key>my-access-key</access-key>
;;                 <secret-key>my-secret-key</secret-key>
;;                 <region>us-west-1</region>
;;                 <host-header>ec2.amazonaws.com</host-header>
;;                 <security-group-name>hazelcast-sg</security-group-name>
;;                 <tag-key>type</tag-key>
;;                 <tag-value>hz-members</tag-value>
;;             </aws>
;;             <discovery-strategies>
;;                 <discovery-strategy ... />
;;             </discovery-strategies>
;;         </join>
;;     </network>
;;     ...
;; </hazelcast>
