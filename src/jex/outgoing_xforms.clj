(ns jex.outgoing-xforms)

(defn filter-map
  [outgoing-map]
  (let [username (:username outgoing-map)]
    (-> outgoing-map
    (assoc :status "Submitted")
    (assoc :user username)
    (assoc :output_manifest [])
    (dissoc :username)
    (dissoc :dag)
    (dissoc :final-output-job)
    (dissoc :steps)
    (dissoc :all-input-jobs :all-output-jobs :imkdir-job))))

(defn transform
  "Applies some transformations to the condor-map. Basically used
   to clean stuff up before dumping information into the OSM."
  [outgoing-map]
  (-> outgoing-map
    filter-map))

