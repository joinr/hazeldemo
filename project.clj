(defproject hazeldemo "0.1.3-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.5.648"]
                 [tolitius/chazel "0.1.23"]
                 [org.hface/hface-client "0.1.8"]
                 [com.hazelcast/hazelcast-aws "3.4"]
                 [spork "0.2.1.4-SNAPSHOT"
                  :exclusions [com.taoensso/nippy]]
                 [com.rpl/nippy-serializable-fns "0.4.2"
                  :exclusions [com.taoensso/nippy]]
                 [com.taoensso/nippy "2.15.3"]
                 ])
