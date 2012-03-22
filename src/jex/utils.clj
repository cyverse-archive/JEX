(ns jex.utils
  (:use [clojure.string :as string]))

(def replacer
  "Params: [regex replace-str str-to-modify]."
  #(.replaceAll (re-matcher %1 %3) %2))

(def replace-at
  "Replaces @ sign in a string. [replace-str str-to-modify" 
  (partial replacer #"@"))

(def at-underscore 
  "Replaces @ sign with _. [str-to-modify]"
  (partial replace-at "_"))

(def at-space
  "Replaces @ sign with empty string. [str-to-modify]"
  (partial replace-at ""))

(def replace-space 
  "Replaces space. [replace-str str-to-modify]" 
   (partial replacer #"\s"))

(def space-underscore 
  "Replaces _. [replace-str str-to-modify" 
  (partial replace-space "_"))

(def now-fmt 
  "Date format used in directory and file names."
  "yyyy-MM-dd-HH-mm-ss.SSS")

(def submission-fmt
  "Date format used by other services."
  "yyyy MMM dd HH:mm:ss")

(defn escape-input
  "Escapes the spaces in a string with backspaces. Useful
   for filetool jobs since it doesn't wrap the string in double
   quotes."
  [escape-string]
  (let [work-string (string/trim escape-string)]
    (string/replace work-string #"\s" "\\\\ ")))

(defn escape-space
  "Escapes the spaces in a string with backspaces and wraps the
   string in double quotes. Useful for non-filetool jobs."
  [escape-string]
  (let [work-string (string/trim escape-string)]
    (if (re-seq #"\s" work-string)
      (str "\"" (string/replace work-string #"\s" "\\\\ ") "\"")
      work-string)))

(defn parse-date
  "Translates date-str into the format specified by format-str."
  [format-str date-str]
  (. (java.text.SimpleDateFormat. format-str) parse date-str))

(defn fmt-date
  "Translates date-obj into the format specified by format-str."
  [format-str date-obj]
  (. (java.text.SimpleDateFormat. format-str) format date-obj))

(defn date
  "Returns the current date as a java.util.Date instance."
  [] 
  (java.util.Date.))

(defn pathize
  "Makes a string safe for inclusion in a path by replacing @ and spaces with
   underscores.."
  [p]
  (-> p at-underscore space-underscore))