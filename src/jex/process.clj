(ns jex.process
  (:use [jex.json-validator])
  (:require [jex.incoming-xforms :as ix]
            [jex.outgoing-xforms :as ox]
            [jex.dagify :as dagify]
            [clojure.tools.logging :as log]
            [clojure.java.shell :as sh]
            [clojure-commons.osm :as osm]))

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
  (let [env {"PATH" (get props "jex.env.path")
             "CONDOR_CONFIG" (get props "jex.env.condor-config")}]
    (apply sh/sh :env env "condor_submit_dag" "-f" dag-path)))

(defn create-osm-record
  [osm-client]
  (let [notif-url (get props "jex.osm.notification-url")
        doc-id (osm/save-object osm-client {})
        result (osm/add-callback osm-client doc-id "on_update" notif-url)]
    (log/warn result)
    doc-id))

(defn submit
  [submit-map]
  (let [[dag-path updated-map] (-> submit-map ix/transform dagify/dagify)
        condor-ret (condor-submit-dag dag-path)]
    (log/warn condor-ret)))

;;(defn submit
;;  [submit-map]
;;  (let [xform-result (-> submit-map ix/transform dagify/dagify)
;;        dag-path     (first xform-result)
;;        updated-map  (last xform-result)
;;        outgoing-map (ox/transform updated-map)
;;        condor-ret   (condor-submit-dag dag-path)
;;        osm-client   (osm/create
;;                       (get props "jex.osm.url")
;;                       (get props "jex.osm.collection"))
;;        doc-id       (create-osm-record osm-client)]
;;    (log/warn "condor_submit_dag:")
;;    (log/warn (:out condor-ret))
;;    (log/warn (:err condor-ret))
;;    (log/warn (str "Exit code: " (:exit condor-ret)))
;;    
;;    (if (not= (:exit condor-ret) 0)
;;      (log/warn 
;;        (osm/update-object osm-client doc-id (assoc outgoing-map :status "Failed")))
;;      (log/warn 
;;        (osm/update-object osm-client doc-id (assoc outgoing-map :status "Submitted"))))))
