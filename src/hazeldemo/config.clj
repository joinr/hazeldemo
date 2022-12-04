(ns hazeldemo.config
  (:require [chazel.core :as ch]
            [spork.util.io :as io])
  (:import [com.hazelcast.config Config]))

;;helpers

;;we create a proxy around Config objects to
;;allow us to pack some meta data along for
;;post-creation/shutdown operations (things
;;like registering/registering from registries).
;;This implementation is janky since operations
;;like with-meta are technically mutable.  Internal
;;use only for now, so who cares.
(defn ->config []
  (let [md (atom {})]
    (proxy [Config clojure.lang.IObj] []
      (meta [] @md)
      (withMeta [m] (do (reset! md m) this)))))

(defn touch [path]
  (when-not (io/fexists? (io/file-path path))
    (println [:file/touching path])
    (io/hock (io/file-path path) ""))
  path)

(defn dir? [^java.io.File f]
  (.isDirectory f))

;;allow trivial meta in Config objects.

;;for now, let's assume this works for the simple case.
;;and that we aren't changing IP addresses within
;;a single session..
;;need to make this more robust.  currently possible to have
;;more than one interface (e.g. testing on wifi with an ethernet adapter).
;;inet should indicate gateway, find first interface where gateway
;;exists.
;;https://stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java/38342964#38342964
(defn my-ip []
  (let [socket (java.net.DatagramSocket.)]
    (.connect socket (java.net.InetAddress/getByName "8.8.8.8") 10002)
    (.. socket getLocalAddress getHostAddress)))

;;this will be really dumb, and we won't use file locks yet. Just maintain
;;a members directory, where each file is an ip address.  This should
;;allow concurrent write/creation of different atomic entries (files).
;;Clients can delete their file as well.  We just need to have the service
;;infer that a directory means you should scan the file names to get
;;addresses.

;;Alternately, we have the dumb manual method - store a fixed set of
;;IP addresses at a network location.  Clients only read from this,
;;it's the responsibility of the admin to manage the member list.

;;For visibility/debugging, we also push the computer name
;;as the file content, although we typically will only
;;really care about the ips.
(defn push-ip!   [members-dir]
  (io/hock (io/file-path members-dir (my-ip))
           (.getHostName (java.net.Inet4Address/getLocalHost))))

(defn remove-ip! [members-dir]
  (io/delete-file-recursively (io/file-path members-dir (my-ip))))

(defn get-members! [members-dir]
  (->> (io/list-files (io/file-path members-dir))
       (mapv io/fname)))

(defn parse-members [path]
  (let [target (io/file path)]
    (if (dir? target)
      (-> (get-members! target)
          (with-meta {:file/path path}))
      (->> path touch io/file slurp clojure.string/split-lines
           (filterv (complement clojure.string/blank?))))))

(defn ->aws [id]
  (let [cfg (->config) #_(Config.)]
    (.. cfg (setInstanceName id))
    (.. cfg getNetworkConfig getJoin getMulticastConfig (setEnabled false))
    (.. cfg getNetworkConfig getJoin getAwsConfig       (setEnabled true))
    cfg))

;;add in support for ad-hoc tcp-ip networks
(defn ->tcp-ip [id & {:keys [required members]}]
  (let [cfg (->config) #_(Config.)]
    (.. cfg (setInstanceName id))
    (.. cfg getNetworkConfig getJoin getMulticastConfig (setEnabled false))
    (let [tcp     (.. cfg getNetworkConfig getJoin getTcpIpConfig)]
      (.. tcp (setEnabled true))
      (when required (.. tcp (setRequiredMember required)))
      (doseq [member members] (.. tcp (addMember member)))
      cfg)))

(defn ->default [id]
  (let [cfg (->config) #_(Config.)]
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
 :register-on-join true|false}

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

;;if members is a file path, we want to get the current members.
;;it's possible there is no members file yet.  So our semantic are to
;;touch the file to ensure it exists, and then read it.
;;We also allow the submission of a directory instead of a file.
;;If a directory is supplied, the directory is inferred to be a
;;registry of all the active ips (one file, where the name is the ip, per
;;member).  This should allow concurrent access to the registry (just look up
;;the current children and return the file names).
(defmethod parse-config :tcp [{:keys [id join members required register-on-join]}]
    ;;members may be a vector of ip addresses or
    ;;a path to a registry of known members, line-delimited ip addresses.
    ;;registry
  (let [members     (cond
                      (map? members)   (parse-members (members :file/path))
                      (vector? members) members
                      :else (throw (ex-info "expected a vector of string ips or map of {:file/path string}"
                                            {:in members})))]
    (-> (->tcp-ip id :required required :members members)
        (with-meta {:member-registry (-> members meta :file/path)
                    :register-on-join register-on-join}))))

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
  (let [cfg (cond
              (map? id-or-map) ;;passed in maps override local config.
                (parse-config id-or-map)
              (string? id-or-map)
                (let [id id-or-map]
                  ;;use env vars for cloud stuff by default.
                  (if-let [env (get (System/getenv) "HAZELCAST")]
                    (if (= env "AWS")
                      (->aws id)
                      (->default id))
                    (let [cfg (get-config!)]
                      (do (.. cfg (setInstanceName id)) ;;merge id with local config.
                          cfg))))
                :else (throw (ex-info "unknown instance arg type!" {:in id-or-map})))
        res   (ch/new-instance cfg)
        m     (meta cfg)]
    (when-let [registry (and (m :register-on-join) (m :member-registry))]
      (push-ip! registry))
    res))

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
