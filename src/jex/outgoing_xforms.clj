(ns jex.outgoing-xforms
  (:use [clojure.string :as string]))

(defn transform
  [outgoing-map]
  (dissoc (assoc outgoing-map :jobs (:nodes (:dag outgoing-map))) :dag))

