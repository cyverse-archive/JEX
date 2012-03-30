(ns jex.argescape
  "Escapes a list of arguments so they can be used in a Condor .cmd file."
  (:require [clojure.string :as string]))

;Tests for single-quotes, double quotes, and spaces.
(defn single? [arg] (if (re-find #"'" arg) true false))
(defn double? [arg] (if (re-find #"\"" arg) true false))
(defn space? [arg] (if (re-find #"\s" arg) true false))

(defn double-single
  "Replaces all single quotes with two single quotes."
  [arg] 
  (.replaceAll (re-matcher #"'" arg) "''"))

(defn double-double
  "Replaces all double quotes with two double quotes."
  [arg] 
  (.replaceAll (re-matcher #"\"" arg) "\"\""))

(defn wrap-single
  "Wraps an argument in single quotes."
  [arg] 
  (str "'" arg "'"))

(defn wrap-double
  "Wraps an argument in double quotes."
  [arg] 
  (str "\"" arg "\""))

(defn space-and-single
  "If an argument has a space and a single quote, the the single
   quote must be doubled and the entire argument wrapped in single quotes.

   If an argument has a space but no single quotes, then the argument
   must be wrapped in single quotes.

   If an argument has a single quote but no spaces, then the single quote
   must be doubled and the entire argument wrapped in single quotes."
  [arg]
  (cond
    (and (space? arg) (single? arg))       (wrap-single (double-single arg))
    (and (space? arg) (not (single? arg))) (wrap-single arg)
    (and (not (space? arg)) (single? arg)) (wrap-single (double-single arg))
    :else arg))

(defn escape
  "Takes in an argument, doubles any double quotes in it and passes it to
   (space-and-single) to handle the weirdness with single quotes and spaces."
  [arg] 
  (space-and-single (double-double arg)))

(defn condorize
  "Takes in a list of arguments, (escape)s them, turns them into a string
   that is wrapped in double quotes."
  [arg-list] 
  (wrap-double (string/join " " (map escape arg-list))))

