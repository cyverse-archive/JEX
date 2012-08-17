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

(defn condor-env
  []
  {"PATH" (get @props "jex.env.path")
   "CONDOR_CONFIG" (get @props "jex.env.condor-config")})

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
  (sh/with-sh-env (condor-env) (sh/sh "condor_rm" sub-id)))

(defn osm-url [] (get @props "jex.osm.url"))
(defn osm-coll [] (get @props "jex.osm.collection"))

(defn extract-state-from-result
  [result-map]
  (-> result-map json/read-json :objects first :state))

(defn query-for-analysis
  [uuid]
  (osm/query
   (osm/create (osm-url) (osm-coll))
   {:state.uuid uuid}))

(defn stop-analysis
  "Calls condor_rm on the submission id associated with the provided analysis
   id."
  [uuid]
  (if-let [sub-id (:sub_id (query-for-analysis uuid))]
    (let [{:keys [exit out err]} (condor-rm sub-id)]
      (when-not (= exit 0)
        (throw+ {:error_code "ERR_FAILED_NON_ZERO" 
                 :sub_id sub-id
                 :out out
                 :err err}))
      {:condor-id sub-id})))

(defn param?
  [param-map]
  (and
   (contains? param-map :name)
   (contains? param-map :value)
   (contains? param-map :order)))

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
  (when-not (contains? param-obj :params)
    (throw+ {:error_code "ERR_INVALID_JSON"
             :message "Missing params key."}))
  
  (when-not (every? true? (map param? (:params param-obj)))
    (throw+ {:error_code "ERR_INVALID_JSON"
             :message "Objects must have 'name', 'value', and 'order' keys."}))
  
  (hash-map :params (ix/escape-params (ix/param-maps (:params param-obj)))))

(defn log-submit-results
  [{:keys [exit out err]}]
  (log/warn (str "Exit code of condor-submit: " exit))
  (log/info (str "condor-submit-dag stdout:"))
  (log/info out)
  (log/info (str "condor-submit-dag stderr:"))
  (log/info err))

(defn parse-sub-id
  [cout]
  (last (re-find #"\d+ job\(s\) submitted to cluster (\d+)\." cout)))

(defn submission-id
  [{:keys [out]}]
  (let [sub-id (parse-sub-id out)]
    (log/warn (str "Grabbed dag_id: " sub-id))
    sub-id))

(defn xform-map-for-osm
  [updated-map sub-id]
  (ox/transform (assoc updated-map :sub_id sub-id)))

(defn push-failed-submit-to-osm
  [output-map]
  (let [osm-client (osm/create (osm-url) (osm-coll))
        doc-id     (create-osm-record osm-client)]
    (osm/update-object osm-client doc-id (assoc output-map :status "Failed"))
    doc-id))

(defn push-successful-submit-to-osm
  [output-map]
  (let [osm-client (osm/create (osm-url) (osm-coll))
        doc-id     (create-osm-record osm-client)]
    (osm/update-object osm-client doc-id output-map)
    doc-id))

(defn push-submission-info-to-osm
  [output-map {:keys [exit]}]
  (if (not= exit 0)
    (push-failed-submit-to-osm output-map)
    (push-successful-submit-to-osm output-map)))

(defn generate-submission
  [submit-map]
  (let [result (-> submit-map ix/transform dagify/dagify)]
    (log/info "Output map:")
    (log/info (json/json-str (last result)))
    result))

(defn condor-submit
  "Submits a job to Condor. sub-path should be the path to a Condor submission
   file."
  [sub-path]
  (let [result (sh/with-sh-env (condor-env) (sh/sh "condor_submit" sub-path))]
    (log-submit-results result)
    result))

(defn submit
  "Applies the incoming tranformations to the submitted request, submits the
   job to the Condor cluster, applies outgoing transformations, and dumps the
   resulting map to the OSM."
  [submit-map]
  (let [[sub-path updated-map] (generate-submission submit-map) 
        sub-result (condor-submit sub-path)
        sub-id     (submission-id sub-result)
        doc-id     (push-submission-info-to-osm
                    (xform-map-for-osm updated-map sub-id)
                    sub-result)]
    [(:exit sub-result) sub-id doc-id]))
