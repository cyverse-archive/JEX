(ns jex.outgoing-xforms
  (:use [clojure.string :as string]
        [clojure.tools.logging :as log]))

(defn filter-map
  [outgoing-map]
  (let [jobs (:nodes (:dag outgoing-map))
        username (:username outgoing-map)]
    (-> outgoing-map
    (assoc :jobs jobs)
    (assoc :status "Submitted")
    (assoc :user username)
    (assoc :output_manifest [])
    (dissoc :username)
    (dissoc :dag)
    (dissoc :final-output-job)
    (dissoc :steps)
    (dissoc :all-input-jobs :all-output-jobs :imkdir-job))))

(defn filter-steps
  [outgoing-map]
  (assoc outgoing-map 
         :jobs (apply merge (for [[jname job] (seq (:jobs outgoing-map))]
                              {jname (dissoc job :input-jobs :output-jobs)}))))

(defn transform
  [outgoing-map]
  (-> outgoing-map
    filter-map
    filter-steps))

