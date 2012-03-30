(ns jex.dagify
  (:use [clojure-commons.file-utils :as ut]
        [clojure.string :only (join)]
        [clojure.tools.logging :as log])
  (:import [java.io File]))

(defn script-output
  "Returns the path to the log containing the Condor logging
   from out on the execution nodes."
  [script-dir] 
  (ut/path-join script-dir "logs" "script-output.log"))

(defn script-error 
  "Returns the path to the error log containing the Condor error
   logging from out on the execution nodes."
  [script-dir] 
  (ut/path-join script-dir "logs" "script-error.log"))

(defn script-log
  "Returns the path to the log containing the Condor logging
   from the submission node (contains the return value of the script)."
  [local-log-dir]
  (ut/path-join local-log-dir "script-condor-log"))

(defn script-submission
  "Generates the Condor submission file that will execute the generated
   shell script."
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
      "should_transfer_files = NO\n"
      "notification = NEVER\n"
      "queue\n")))

;(defn script-step
;  [script-dir script-path local-log-dir]
;  {:generated_script 
;   {}})

(defn jobs-in-order
  "Take in the submitted analysis (processed by jex.incoming-xforms),
   and returns a list of the job definitions in the order that they
   should be executed in the shell script."
  [analysis-map]
  (concat
    [(:imkdir-job analysis-map)]
    (:all-input-jobs analysis-map)
    (:steps analysis-map)
    [(:final-output-job analysis-map)]))

(defn script-line
  "Takes in a job definition and generates a section of the shell
   script that will be executed out on the Condor nodes. This also
   handles capturing the exit value of a command in the shell script."
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
  "Takes in an analysis map that has been processed by
   (jex.incoming-xforms/transform) and turns it into a shell script
   that will be run out on the Condor cluster. Needs refactoring."
  [analysis-map]
  (let [job-uuid (:uuid analysis-map)
        job-dir  (str "iplant-de-jobs/" job-uuid)
        run-on-nfs (:run-on-nfs analysis-map)]
    (str 
      "#!/bin/bash\n"
      (if (not run-on-nfs)
        (str "cd ~\n"))
      (if (not run-on-nfs)
        (str "mkdir -p " job-dir "\n"))
      (if (not run-on-nfs)
        (str "pushd " job-dir "\n")
        (str "pushd ..\n"))
      (if (not run-on-nfs) 
        "mkdir -p logs\n")
      "EXITSTATUS=0\n"
      (join "\n" (map script-line (jobs-in-order analysis-map)))
      "popd\n"
      (if (not run-on-nfs)
        (str "rm -r " job-dir "\n"))
      "exit $EXITSTATUS\n")))

(defn dagify
  "Takes in analysis map that's been processed by (jex.incoming-xforms/transform)
   and puts together the stuff needed to submit the job to the Condor cluster. That
   includes:

   * Creating a place on the NFS mount point where the script and the Condor logs
     will be written to out on the Condor cluster.

   * Creating the local log directory (where Condor logs job stuff to on the
     submission node).

   * Generates the shell script and writes it out to the NFS mount point.

   * Generates the Condor submission file and writes it out to the NFS mount point.

   * Removes entries from the analysis-map that aren't needed any more.

   Returns a vector containing two entries, the path to the Condor submission file
   and the new version of the analysis-map."
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
