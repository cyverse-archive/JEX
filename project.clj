(defproject jex "1.0.0-SNAPSHOT"
  :description "FIXME: write"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/data.json "0.1.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/java.classpath "0.1.0"]
                 [compojure "1.0.1"]
                 [ring/ring-jetty-adapter "1.0.1"]
                 [clj-http "0.2.5"]
                 [org.iplantc/clojure-commons "1.1.0-SNAPSHOT"]]
  :aot [jex.core]
  :main jex.core)
