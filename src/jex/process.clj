(ns jex.process
  (:use [jex.json-validator]
        [slingshot.slingshot :only [throw+]])
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

;Converts a string to a boolean.
(def boolize #(boolean (Boolean. %)))

(defn init
  [app-props]
  (reset! props app-props)
  (reset! ix/filetool-path (get @props "jex.app.filetool-path"))
  (reset! ix/icommands-path (get @props "jex.app.icommands-path"))
  (reset! ix/condor-log-path (get @props "jex.app.condor-log-path"))
  (reset! ix/nfs-base (get @props "jex.app.nfs-base"))
  (reset! ix/irods-base (get @props "jex.app.irods-base"))
  (reset! ix/filter-files (get @props "jex.app.filter-files"))
  (reset! ix/run-on-nfs (or (boolize (get @props "jex.app.run-on-nfs")) false)))

(def validators
  [#(string? (:uuid %))
   #(string? (:username %))
   #(string? (:name %))
   #(sequential? (:steps %))
   #(every? true? 
           (for [step (:steps %)]
             (every? true?
                     [(map? (:config step))
                      (map? (:component step))
                      (string? (:location (:component step)))
                      (string? (:name (:component step)))])))])

(defn validate-submission
  "Validates a submission."
  [submit-map]
  (valid? submit-map validators))

(defn condor-submit
  "Submits a job to Condor. sub-path should be the path to a Condor submission file."
  [sub-path]
  (let [env {"PATH" (get @props "jex.env.path")
             "CONDOR_CONFIG" (get @props "jex.env.condor-config")}
        shellout (partial sh/sh :env env)]
    (sh/with-sh-env 
      env
      (sh/sh "condor_submit" sub-path))))

(defn create-osm-record
  "Creates a new record in the OSM, associates the notification-url with it as a
   callback, and returns the OSM document ID in a string."
  [osm-client]
  (let [notif-url (get @props "jex.osm.notification-url")
        doc-id (osm/save-object osm-client {})
        result (osm/add-callback osm-client doc-id "on_update" notif-url)]
    (log/warn result)
    doc-id))

(defn condor-rm
  "Stops a condor job."
  [sub-id]
  (let [env {"PATH" (get @props "jex.env.path")
             "CONDOR_CONFIG" (get @props "jex.env.condor-config")}
        shellout (partial sh/sh :env env)]
    (sh/with-sh-env
      env
      (sh/sh "condor_rm" sub-id))))

(defn missing-condor-id
  [uuid]
  (hash-map
   :error_code "ERR_MISSING_CONDOR_ID"
   :uuid uuid))

(defn get-job-sub-id
  [uuid]
  (let [osm-url    (get @props "jex.osm.url")
        osm-coll   (get @props "jex.osm.collection")
        osm-client (osm/create osm-url osm-coll)
        result (json/read-json (osm/query osm-client {:state.uuid uuid}))]
    (when-not result
      (throw+ (missing-condor-id uuid)))

    (when-not (contains? result :objects)
      (throw+ (missing-condor-id uuid)))

    (when-not (pos? (count (:objects result)))
      (throw+ (missing-condor-id uuid)))

    (when-not (contains? (first (:objects result)) :state)
      (throw+ (missing-condor-id uuid)))

    (when-not (contains? (:state (first (:objects result))) :sub_id)
      (throw+ (missing-condor-id uuid)))

    (get-in (first (:objects result)) [:state :sub_id])))


(defn stop-analysis
  "Calls condor_rm on the submission id associated with the provided analysis
   id."
  [uuid]
  (let [sub-id (get-job-sub-id uuid)]
    (log/debug (str "Grabbed Condor ID: " sub-id))
    (if sub-id
      (let [{:keys [exit out err]} (condor-rm sub-id)]
        (when-not (= exit 0)
          (log/debug (str "condor-rm status was: " exit))
          (throw+ {:error_code "ERR_FAILED_NON_ZERO" 
                   :sub_id sub-id
                   :out out
                   :err err})))
      (throw+ {:error_code "ERR_MISSING_CONDOR_ID" :uuid uuid}))))

(defn cmdline-preview
  "Accepts a map in the following format:
   {:params [
       {:name \"-t\"
        :value \"foo\"
        :order 0
   ]}

   Returns a map in the format:
   {:params \"-t foo\"}"
  [param-obj]
  (let [param? #(and (contains? %1 :name)
                     (contains? %1 :value)
                     (contains? %1 :order))] 
    (when-not (contains? param-obj :params)
      (throw+ {:error_code "ERR_INVALID_JSON"
               :message "Missing params key."}))
    
    (when-not (every? true? (map param? (:params param-obj)))
      (throw+ {:error_code "ERR_INVALID_JSON"
               :message "All objects must have 'name', 'value', and 'order' keys."})))
  
  (hash-map :params (ix/escape-params (ix/param-maps (:params param-obj)))))

(defn submit
  "Applies the incoming tranformations to the submitted request, submits the
   job to the Condor cluster, applies outgoing transformations, and dumps the
   resulting map to the OSM."
  [submit-map]
  (let [osm-url    (get @props "jex.osm.url")
        osm-coll   (get @props "jex.osm.collection")
        notif-url  (get @props "jex.osm.notification-url")
        [sub-path updated-map] (-> submit-map ix/transform dagify/dagify)
        {cexit :exit cout :out cerr :err} (condor-submit sub-path)
        sub-id     (last (re-find #"\d+ job\(s\) submitted to cluster (\d+)\." cout))
        output-map (ox/transform (assoc updated-map :sub_id sub-id))
        osm-client (osm/create osm-url osm-coll)
        doc-id     (create-osm-record osm-client)]
    
    (log/warn (str "Exit code of condor-submit: " cexit))
    (log/info (str "condor-submit-dag stdout:"))
    (log/info cout)
    (log/info (str "condor-submit-dag stderr:"))
    (log/info cerr)
    (log/info "Output map:")
    (log/info (json/json-str output-map))
    (log/warn (str "Grabbed dag_id: " sub-id))
    
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
    
    [cexit sub-id doc-id]))
