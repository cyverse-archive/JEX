(ns jex.config
  (:use [clojure-commons.props])
  (:require [clojure-commons.clavin-client :as cl]
            [clojure.tools.logging :as log]))

(def jex-props (atom nil))

(defn listen-port
  "Returns the port to accept requests on."
  []
  (Integer/parseInt (get @jex-props "jex.app.listen-port")))

(defn configure []
  (let [zkprops (parse-properties "zkhosts.properties")
        zkurl   (get zkprops "zookeeper")]
    (cl/with-zk
      zkurl
      (when (not (cl/can-run?))
        (log/warn "THIS APPLICATION CANNOT RUN ON THIS MACHINE. SO SAYETH ZOOKEEPER.")
        (log/warn "THIS APPLICATION WILL NOT EXECUTE CORRECTLY.")
        (System/exit 1))
      
      (reset! jex-props (cl/properties "jex")))))