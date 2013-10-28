(ns jex.json-validator
  (:require [cheshire.core :as cheshire]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.set :as set])
  (:use [clojure.test :only (function?)]))

(defn json?
  "Returns true if a string is JSON."
  [json-string]
  (if (try 
        (cheshire/decode json-string) 
        (catch Exception e false))
    true
    false))

(defn acceptable-output-folder?
  [username path-to-check]
  (log/warn "[jex] acceptable-output-folder" username path-to-check)
  (and (not (.startsWith path-to-check "/iplant/home/shared"))
       (.startsWith path-to-check (str "/iplant/home/" (first (string/split username #"\@"))))
       (not (.startsWith path-to-check "/iplant/trash"))))

(defn valid?
  [json-map validators]
  (every? true? (for [vd validators] (vd json-map))))
