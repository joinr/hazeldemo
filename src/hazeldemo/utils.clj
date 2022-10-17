(ns hazeldemo.utils
  (:require [chazel.core :as ch]))

(defprotocol IRemote
  (as-function [this] "coerces arg into an invokable function"))

(extend-protocol IRemote
  String
  (as-function [this]
    (ns-resolve *ns* (symbol this)))
  clojure.lang.Symbol
  (as-function [this]
    (ns-resolve *ns* (symbol this)))
  clojure.lang.Keyword
  (as-function [this]
    (ns-resolve *ns* (symbol (name this))))
  ;;maybe add in default Object impl...
  )
