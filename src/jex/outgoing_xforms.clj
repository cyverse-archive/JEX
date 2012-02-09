(ns jex.outgoing-xforms
  (:use [clojure.string :as string]
        [clojure.tools.logging :as log]))

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
  [outgoing-map]
  (-> outgoing-map
    filter-map))

