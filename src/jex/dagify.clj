(ns jex.dagify
  (:use [clojure-commons.file-utils :as ut]
        [clojure.string :only (join)]
        [clojure.tools.logging :as log])
  (:import [java.io File]))

(defn dag-path
  [script-dir]
  (ut/path-join script-dir "logs" "iplantDag.dag"))

(defn dag-contents
  [script-sub-path dummy-sub-path dummy-post-path]
  (str
    "JOB iplant_script " script-sub-path "\n"
    "JOB dummy_script " dummy-sub-path "\n"
    "PARENT iplant_script CHILD dummy_script\n"
    "SCRIPT POST iplant_script /usr/local/bin/handle_error.sh $RETURN\n"
    "SCRIPT POST dummy_script " dummy-post-path "$RETURN\n"))

(defn script-submission
  [username uuid script-dir script-path local-log-dir]
  (let [output (ut/path-join script-dir "logs" "script-output.log")
        error  (ut/path-join script-dir "logs" "script-error.log")
        log    (ut/path-join local-log-dir "script-condor-log")]
    (str
      "universe = vanilla\n"
      "executable = /bin/bash\n" 
      "arguments = \"" script-path "\"\n"
      "output = " output "\n"
      "error = " error "\n"
      "log = " log "\n"
      "+IpcUuid = \"" uuid "\"\n"
      "+IpcUsername = \"" username "\"\n"
      "transfer_executables = False\n"
      "transfer_output_files = \n"
      "when_to_transfer_output = ON_EXIT\n"
      "notification = NEVER\n"
      "queue\n")))

(defn dummy-submission
  [username uuid script-dir script-path local-log-dir]
  (let [output (ut/path-join script-path "logs" "dummy-output.log")
        error  (ut/path-join script-path "logs" "dummy-error.log")
        log    (ut/path-join local-log-dir "dummy.log")]
    (str
      "universe = vanilla\n"
      "executable = /bin/bash\n"
      "arguments = \"" script-path "\"\n"
      "output = " output "\n"
      "error = " error "\n"
      "log = " log "\n"
      "+IpcUuid = \"" uuid "\"\n"
      "+IpcUsername = \"" username "\"\n"
      "transfer_executables = False\n"
      "transfer_output_files = \n"
      "when_to_transfer_output = ON_EXIT\n"
      "notification = NEVER\n"
      "queue\n")))

(defn dummy-script
  []
  "#!/bin/bash\n/bin/echo \"This is a dummy job to fill out the DAG.\"\n")

(defn dummy-script-post
  []
  "#!/bin/bash\nexit 0\n")

(defn jobs-in-order
  [analysis-map]
  (concat
    [(:imkdir-job analysis-map)]
    (:all-input-jobs analysis-map)
    (:steps analysis-map)
    [(:final-output-job analysis-map)]))

(defn script-line
  [job-def]
  (let [env    (:environment job-def)
        exec   (:executable job-def)
        args   (:arguments job-def)
        stderr (:stderr job-def)
        stdout (:stdout job-def)]
    (str env " " exec " " args " 1>" stdout " 2>" stderr)))

(defn script
  [analysis-map]
  (str 
    "#!/bin/bash\n" 
    (join "\n" (map script-line (jobs-in-order analysis-map))) 
    "\n"))

(defn dagify
  [analysis-map]
  (let [script-dir  (:working_dir analysis-map)
        dag-log-dir (ut/path-join script-dir "logs")
        output-dir  (:output_dir analysis-map)
        condor-log  (:condor-log-dir analysis-map)
        uuid        (:uuid analysis-map)
        username    (:username analysis-map)
        scriptname  (str username "-" uuid ".sh")
        scriptpath  (ut/path-join script-dir scriptname)
        scriptsub   (ut/path-join script-dir "logs" "iplant.cmd")
        dummysub    (ut/path-join script-dir "logs" "dummy.cmd")
        dummypath   (ut/path-join script-dir "dummy.sh")
        dummypost   (ut/path-join script-dir "dummy-post.sh")
        dagpath     (dag-path script-dir)
        local-logs  (ut/path-join condor-log "logs")]
    
    ;Create the directory the script and log files will go into.
    (log/info (str "Creating submission directories: " dag-log-dir))
    (if (not (.mkdirs (File. dag-log-dir)))
      (log/warn (str "Failed to create directory: " dag-log-dir)))
    
    ;Create the local log directory.
    (log/info (str "Creating the local log directory: " local-logs))
    (if (not (.mkdirs (File. local-logs)))
      (log/warn (str "Failed to create directory " local-logs)))
    
    ;Write out the script
    (spit scriptpath (script analysis-map))
    
    ;Write out the script submission
    (spit scriptsub (script-submission username uuid script-dir scriptpath local-logs))
    
    ;Write out the dummy script
    (spit dummypath (dummy-script))
    
    ;Write out the dummy submission
    (spit dummysub (dummy-submission username uuid script-dir dummypath local-logs))
    
    ;Write out the dummy post
    (spit dummypost (dummy-script-post))
    
    ;Write out the dag
    (spit dag-path (dag-contents scriptsub dummysub dummypost))
    
    [dag-path analysis-map]))
