(ns jex.dagify
  (:use [clojure-commons.file-utils :as ut]
        [clojure.string :only (join)]
        [clojure.tools.logging :as log])
  (:import [java.io File]))

(defn script-output [script-dir] (ut/path-join script-dir "logs" "script-output.log"))
(defn script-error [script-dir] (ut/path-join script-dir "logs" "script-error.log"))
(defn script-log [local-log-dir] (ut/path-join local-log-dir "script-condor-log"))

(defn script-submission
  [username uuid script-dir script-path local-log-dir run-on-nfs]
  (let [output (script-output script-dir)
        error  (script-error script-dir)
        log    (script-log local-log-dir)]
    (str
      (if run-on-nfs
        (str "remote_initialdir = " (ut/dirname script-path) "\n"))
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
   {}})

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
    (str env " " exec " " args " 1>" stdout " 2>" stderr "\n"
         "if [ ! \"$?\" -eq \"0\" ]; then\n"
             "\tEXITSTATUS=1\n"
         "fi\n")))

(defn script
  [analysis-map]
  (str 
    "#!/bin/bash\n"
    "pushd ..\n"
    "EXITSTATUS=0\n"
    (join "\n" (map script-line (jobs-in-order analysis-map)))
    "popd\n"
    "exit $EXITSTATUS\n"))

(defn dagify
  [analysis-map]
  (let [script-dir  (:working_dir analysis-map)
        dag-log-dir (ut/path-join script-dir "logs")
        output-dir  (:output_dir analysis-map)
        condor-log  (:condor-log-dir analysis-map)
        uuid        (:uuid analysis-map)
        username    (:username analysis-map)
        run-on-nfs  (:run-on-nfs analysis-map)
        scriptname  "iplant.sh"
        scriptpath  (ut/path-join script-dir "logs" scriptname)
        scriptsub   (ut/path-join script-dir "logs" "iplant.cmd")
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
    (spit scriptsub (script-submission username uuid script-dir scriptpath local-logs run-on-nfs))
    
    ;Dissoc all of the other steps, they're not needed any more. 
    ;Assoc the new dummy script and generated script.
    [scriptsub (-> analysis-map
                  (dissoc :steps)
                  (assoc :executable "/bin/bash"
                         :args scriptpath
                         :status "Submitted"
                         :output (script-output script-dir)
                         :error (script-error script-dir)
                         :log (script-log local-logs)))]))
