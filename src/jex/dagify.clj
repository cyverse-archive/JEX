(ns jex.dagify
  (:use [clojure-commons.file-utils :as ut]
        [clojure.string :only (join)]
        [clojure.tools.logging :as log])
  (:import [java.io File]))

(defn- nodify
  [jobdef]
  {(:id jobdef) jobdef})

(defn gen-steps 
  [analysis-map] 
  (into [] (map nodify (:steps analysis-map))))

(defn gen-input-jobs 
  [analysis-map]
  (into 
    [] 
    (map 
      nodify 
      (flatten (map :input-jobs (:steps analysis-map))))))

(defn- edgy-ij
  [steps]
  (flatten
    (map 
      #(map (fn [i] [(:id i) (:id %)]) (:input-jobs %))
      steps)))

(defn gen-imkdir-edges
  [analysis-map]
  [[(:id (:imkdir-job analysis-map))
    (:id (first (:all-input-jobs analysis-map)))]])

(defn gen-input-edges
  [analysis-map]
   (into [] (for [part (partition 2 (edgy-ij (:steps analysis-map)))] [(first part) (last part)])))

(defn gen-shotgun-edges
  [analysis-map]
  [[(:id (last (:steps analysis-map))) ;The last step is the parent. 
    (:id (:final-output-job analysis-map))]] ) ;The final job is child.

(defn gen-step-edges
  [analysis-map]
  (let [parted-steps (partition 2 1 (:steps analysis-map))]
    (into [] (for [part parted-steps] [(:id (first part)) (:id (last part))]))))

(defn gen-edges
  [analysis-map]
  (concat
    (gen-imkdir-edges analysis-map)
    (gen-input-edges analysis-map)
    (gen-step-edges analysis-map)
    (gen-shotgun-edges analysis-map)))

(defn gen-dag
  [analysis-map]
  (let [dag         {:nodes (hash-map) :edges []}
        shotgun-job (nodify (:final-output-job analysis-map))
        imkdir-job  (nodify (:imkdir-job analysis-map))
        steps       (gen-steps analysis-map)
        input-jobs  (gen-input-jobs analysis-map)
        extra-jobs  [imkdir-job shotgun-job]
        all-jobs    (concat steps input-jobs extra-jobs)]
    {:nodes (apply merge all-jobs)
     :edges (gen-edges analysis-map)}))

(defn submission-filename [job-id] (str "ipc-" job-id ".cmd"))

(defn submission-path
  [working-dir job-id]
  (ut/path-join working-dir "logs" (submission-filename job-id)))

(defn dag-job-def
  [working-dir job-id]
  (str "JOB " job-id " " (submission-path working-dir job-id)))

(defn dag-edge
  [edge]
  (let [parent (first edge)
        child  (last edge)]
    (str "PARENT " parent " CHILD " child)))

(defn dag-script-post
  [working-dir output-dir job-id]
  (str "SCRIPT POST " 
       job-id 
       " /usr/local/bin/handle_error.sh " 
       working-dir 
       " " 
       output-dir 
       " $RETURN"))

(defn gen-dag-contents
  [working-dir output-dir dag]
  (let [wdir       working-dir
        odir       output-dir
        job-ids    (keys (:nodes dag))
        def-lines  (into [] (for [job-id job-ids] (dag-job-def wdir job-id))) 
        edge-lines (map dag-edge (:edges dag))
        scr-lines  (map (partial dag-script-post wdir odir) job-ids)
        all-lines  (concat def-lines edge-lines scr-lines)]
    (str (join "\n" all-lines) "\n")))

(defn dag-path
  [working-dir]
  (ut/path-join working-dir "logs" "iplantDag.dag"))

(defn env
  [job-def]
  (if (contains? job-def :environment)
    job-def
    (assoc job-def :environment "")))

(defn gen-submission
  [working-dir username analysis-uuid job-def]
  (let [verse     (str "universe = vanilla")
        initdir   (str "initialdir = " working-dir)
        rinitdir  (str "remote_initialdir = " working-dir)
        reqs      (str "requirements =")
        env       (str "environment = \"" (:environment job-def) "\"")
        exec      (str "executable = " (:executable job-def))
        output    (str "output = " (:stdout job-def))
        error     (str "error = " (:stderr job-def))
        log       (str "log = " (:log-file job-def))
        args      (str "arguments = " (:arguments job-def))
        uuid      (str "+IpcUuid = \"" analysis-uuid "\"")
        jobid     (str "+IpcJobId = \"" (:id job-def) "\"")
        user      (str "+IpcUsername = \"" username "\"")
        xfer-exec "transfer_executable = False"
        xfer-out  "transfer_output_files = "
        wxfer-out "when_to_transfer_output = ON_EXIT"
        notif     "notification = NEVER"
        queue     "queue\n"]
    (join 
    "\n"
    [verse
     initdir
     rinitdir
     reqs
     env
     exec
     output
     error
     log
     args
     uuid
     jobid
     user
     xfer-exec
     xfer-out
     wxfer-out
     notif
     queue])))

(defn dagify
  [analysis-map]
  (let [working-dir  (:working_dir analysis-map)
        output-dir   (:output_dir analysis-map)
        condor-log   (:condor-log-dir analysis-map)
        uuid         (:uuid analysis-map)
        username     (:username analysis-map)
        dag          (gen-dag (-> analysis-map env))
        dag-fpath    (dag-path working-dir)
        dag-contents (gen-dag-contents working-dir output-dir dag)]
    ;;Create the working dir and the log directory.
    (log/info (str "Creating submission directories: " (ut/dirname dag-fpath)))
    (if (not (.mkdirs (File. (ut/dirname dag-fpath))))
      (log/warn (str "Failed to create directory: " (ut/dirname dag-fpath)))) 
    
    ;;Make the local log directory
    (log/info (str "Creating the local log directory: " (ut/path-join condor-log "logs")))
    (if (not (.mkdirs (File. (ut/path-join condor-log "logs"))))
      (log/warn (str "Failed to create directory " (ut/path-join condor-log "logs"))))
    
    ;Write out the job submission files.
    (doseq [node-id    (keys (:nodes dag))]
      (let [node-def   (get (:nodes dag) node-id)
            node-fpath (submission-path working-dir node-id)
            node-sub   (gen-submission working-dir username uuid node-def)]
        (log/info (str "Writing out submission file: " node-fpath))
        (log/info node-sub)
        (spit node-fpath node-sub)))
    
    ;Write out dag file.
    (log/info (str "Writing out dag file:"))
    (log/info dag-contents)
    (spit dag-fpath dag-contents)
    
    ;return the dag filepath.
    [dag-fpath (assoc analysis-map :dag dag)]))
