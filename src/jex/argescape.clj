(ns jex.argescape
  (:require [clojure.string :as string])
  (:use [clojure-commons.file-utils :only (replace-re)]))

(defn single? [arg] (if (re-find #"'" arg) true false))
(defn double? [arg] (if (re-find #"\"" arg) true false))
(defn space? [arg] (if (re-find #"\s" arg) true false))

(defn double-single [arg] (replace-re #"'" "''" arg))
(defn double-double [arg] (replace-re #"\"" "\"\"" arg))
(defn wrap-single [arg] (str "'" arg "'"))
(defn wrap-double [arg] (str "\"" arg "\""))

(defn space-and-single
  [arg]
  (cond
    (and (space? arg) (single? arg))       (wrap-single (double-single arg))
    (and (space? arg) (not (single? arg))) (wrap-single arg)
    (and (not (space? arg)) (single? arg)) (wrap-single (double-single arg))
    :else arg))

(defn escape [arg] (space-and-single (double-double arg)))
(defn condorize [arg-list] (wrap-double (string/join " " (map escape arg-list))))

