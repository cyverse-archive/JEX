(ns jex.process
  (:use [jex.json-validator])
  (:require [jex.incoming-xforms :as ix]
            [jex.outgoing-xforms :as ox]
            [jex.dagify :as dagify]
            [clojure.tools.logging :as log]
            [clojure.java.shell :as sh]
            [clojure-commons.osm :as osm]
            [clojure.data.json :as json]))

(defn failure [reason]
  {:status "failure" :reason reason})

(defn success []
  {:status "success"})

(def props (atom nil))

(defn init
  [app-props]
  (reset! props app-props)
  (reset! ix/filetool-path (get @props "jex.app.filetool-path"))
  (reset! ix/icommands-path (get @props "jex.app.icommands-path"))
  (reset! ix/condor-log-path (get @props "jex.app.condor-log-path"))
  (reset! ix/nfs-base (get @props "jex.app.nfs-base"))
  (reset! ix/irods-base (get @props "jex.app.irods-base")))

(def validators
  [#(string? (:uuid %))
   #(string? (:username %))
   #(string? (:name %))
   #(string? (:workspace_id %))
   #(sequential? (:steps %))
   #(every? true? 
           (for [step (:steps %)]
             (every? true?
                     [(string? (:type step))
                      (map? (:config step))
                      (map? (:component step))
                      (string? (:location (:component step)))
                      (string? (:name (:component step)))])))])

(defn validate-submission
  "Validates a submission."
  [submit-map]
  (if (not (valid? submit-map validators))
    (failure "Bad JSON")
    (success)))

(defn condor-submit-dag
  [dag-path]
  (let [env {"PATH" (get @props "jex.env.path")
             "CONDOR_CONFIG" (get @props "jex.env.condor-config")}
        shellout (partial sh/sh :env env)]
    (sh/with-sh-env 
      env
      (sh/sh "condor_submit_dag" "-f" dag-path))))

(defn create-osm-record
  [osm-client]
  (let [notif-url (get @props "jex.osm.notification-url")
        doc-id (osm/save-object osm-client {})
        result (osm/add-callback osm-client doc-id "on_update" notif-url)]
    (log/warn result)
    doc-id))

(defn submit
  [submit-map]
  (let [osm-url    (get @props "jex.osm.url")
        osm-coll   (get @props "jex.osm.collection")
        notif-url  (get @props "jex.osm.notification-url")
        [dag-path updated-map] (-> submit-map ix/transform dagify/dagify)
        {cexit :exit cout :out cerr :err} (condor-submit-dag dag-path)
        dag-id     (last (re-find #"\d+ job\(s\) submitted to cluster (\d+)\." cout))
        output-map (ox/transform (assoc updated-map :dag_id dag-id))
        osm-client (osm/create osm-url osm-coll)
        doc-id     (create-osm-record osm-client)]
    
    (log/warn (str "Exit code of condor-submit-dag: " cexit))
    (log/info (str "condor-submit-dag stdout:"))
    (log/info cout)
    (log/info (str "condor-submit-dag stderr:"))
    (log/info cerr)
    (log/info "Output map:")
    (log/info (json/json-str output-map))
    (log/warn (str "Grabbed dag_id: " dag-id))
    
    ;Update the OSM doc with dag info, triggering notification.
    (if (not= cexit 0)
      (log/warn 
        (osm/update-object 
          osm-client 
          doc-id
          (assoc output-map :status "Failed"))) 
      (log/warn 
        (osm/update-object 
          osm-client 
          doc-id 
          output-map)))
    
    [cexit dag-id doc-id]))
