(ns jex.dagify
  (:use [clojure-commons.file-utils :as ut]
        [clojure.string :only (join)]
        [clojure.tools.logging :as log])
  (:import [java.io File]))

(defn dag-path
  [script-dir]
  (ut/path-join script-dir "logs" "iplantDag.dag"))

(defn dag-contents
  [script-sub-path dummy-sub-path]
  (str
    "JOB iplant_script " script-sub-path "\n"
    "JOB dummy_script " dummy-sub-path "\n"
    "PARENT iplant_script CHILD dummy_script\n"
    "SCRIPT POST iplant_script /usr/local/bin/handle_error.sh $RETURN\n"))

(defn script-output [script-dir] (ut/path-join script-dir "logs" "script-output.log"))
(defn script-error [script-dir] (ut/path-join script-dir "logs" "script-error.log"))
(defn script-log [local-log-dir] (ut/path-join local-log-dir "script-condor-log"))

(defn script-submission
  [username uuid script-dir script-path local-log-dir]
  (let [output (script-output script-dir)
        error  (script-error script-dir)
        log    (script-log local-log-dir)]
    (str
      "universe = vanilla\n"
      "executable = /bin/bash\n" 
      "arguments = \"" script-path "\"\n"
      "output = " output "\n"
      "error = " error "\n"
      "log = " log "\n"
      "+IpcUuid = \"" uuid "\"\n"
      "+IpcJobId = \"generated_script\"\n"
      "+IpcUsername = \"" username "\"\n"
      "transfer_executables = False\n"
      "transfer_output_files = \n"
      "when_to_transfer_output = ON_EXIT\n"
      "notification = NEVER\n"
      "queue\n")))

(defn script-step
  [script-dir script-path local-log-dir]
  {:generated_script 
   {:executable "/bin/bash"
    :args script-path
    :output (script-output script-dir)
    :error (script-error script-dir)
    :log (script-log local-log-dir)}})

(defn dummy-output [script-dir] (ut/path-join script-dir "logs" "dummy-output.log"))
(defn dummy-error [script-dir] (ut/path-join script-dir "logs" "dummy-error.log"))
(defn dummy-log [local-log-dir] (ut/path-join local-log-dir "dummy.log"))

(defn dummy-submission
  [username uuid script-dir script-path local-log-dir]
  (let [output (dummy-output script-dir)
        error  (dummy-error script-dir)
        log    (dummy-log local-log-dir)]
    (str
      "universe = vanilla\n"
      "executable = /bin/bash\n"
      "arguments = \"" script-path "\"\n"
      "output = " output "\n"
      "error = " error "\n"
      "log = " log "\n"
      "+IpcUuid = \"" uuid "\"\n"
      "+IpcJobId = \"dummy_job\"\n"
      "+IpcUsername = \"" username "\"\n"
      "transfer_executables = False\n"
      "transfer_output_files = \n"
      "when_to_transfer_output = ON_EXIT\n"
      "notification = NEVER\n"
      "queue\n")))

(defn dummy-step
  [script-dir script-path local-log-dir]
  {:dummy_job
   {:executable "/bin/bash"
    :args script-path
    :output (dummy-output script-dir)
    :error (dummy-error script-dir)
    :log (dummy-log local-log-dir)}})

(defn dummy-script
  []
  "#!/bin/bash\n/bin/echo \"This is a dummy job to fill out the DAG.\"\n")

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
    "mkdir logs\n"
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
    
    ;Write out the dag
    (spit dagpath (dag-contents scriptsub dummysub))
    
    ;Dissoc all of the other steps, they're not needed any more. 
    ;Assoc the new dummy script and generated script.
    [dagpath (-> analysis-map
                (dissoc :steps)
                (assoc-in [:steps] (script-step script-dir scriptpath local-logs))
                (assoc-in [:steps] (dummy-step script-dir dummypath local-logs)))]))
