(ns jex.core
  (:gen-class)
  (:use compojure.core)
  (:use [ring.middleware
         params
         keyword-params
         nested-params
         multipart-params
         cookies
         session]
        [clojure-commons.props]
        [clojure-commons.error-codes]
        [clojure.java.classpath]
        [slingshot.slingshot :only [try+ throw+]])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as rsp-utils]
            [ring.adapter.jetty :as jetty]
            [clojure-commons.clavin-client :as cl]
            [jex.process :as jp]
            [jex.json-body :as jb]
            [clojure.java.io :as ds]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]))

(def jex-props (atom nil))

(defn listen-port
  "Returns the port to accept requests on."
  []
  (Integer/parseInt (get @jex-props "jex.app.listen-port")))

(defn do-submission
  "Handles a request on /. "
  [request]
  (let [body (:body request)]
    (log/info "Received job request:")
    (log/info (json/json-str body))
    
    (if (jp/validate-submission body)
      (let [[exit-code dag-id doc-id] (jp/submit body)]
        (cond
          (not= exit-code 0)
          (throw+ {:error_code "ERR_FAILED_NON_ZERO"})
          
          :else
          {:sub_id dag-id
           :osm_id doc-id}))
      (throw+ {:error_code "ERR_INVALID_JSON"}))))

(defroutes jex-routes
  (GET "/" [] "Welcome to the JEX.")
  
  (POST "/" request
        (trap "submit" do-submission request))
  
  (POST "/arg-preview" request
        (trap "arg-preview" jp/cmdline-preview (:body request)))
  
  (DELETE "/stop/:uuid" [uuid]
          (trap "stop" jp/stop-analysis uuid)))

(defn site-handler [routes]
  (-> routes
    jb/parse-json-body
    wrap-errors))

(defn -main
  [& args]
  (def zkprops (parse-properties "zkhosts.properties"))
  (def zkurl (get zkprops "zookeeper"))
  
  (cl/with-zk
    zkurl
    (when (not (cl/can-run?))
      (log/warn "THIS APPLICATION CANNOT RUN ON THIS MACHINE. SO SAYETH ZOOKEEPER.")
      (log/warn "THIS APPLICATION WILL NOT EXECUTE CORRECTLY.")
      (System/exit 1))
    
    (reset! jex-props (cl/properties "jex")))
  
  (jp/init @jex-props)
  (jetty/run-jetty (site-handler jex-routes) {:port (listen-port)}))
