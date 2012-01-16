(ns jex.outgoing-xforms
  (:use [clojure.string :as string]))

(defn transform
  [outgoing-map]
  (let [jobs (:nodes (:dag outgoing-map))
        username (:username outgoing-map)]
    (-> outgoing-map
      (assoc :jobs jobs)
      (assoc :status "Running")
      (assoc :user username)
      (assoc :output_manifest [])
      (dissoc :username)
      (dissoc :dag)
      (dissoc :all-input-jobs :all-output-jobs :imkdir-job))))

