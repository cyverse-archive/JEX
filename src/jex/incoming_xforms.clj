(ns jex.incoming-xforms
  (:require [clojure.string :as string]
            [jex.argescape :as ae]
            [clojure-commons.file-utils :as ut]))

(def filetool-path (atom ""))
(def icommands-path (atom ""))
(def condor-log-path (atom ""))
(def nfs-base (atom ""))
(def irods-base (atom ""))

(def replacer #(.replaceAll (re-matcher %1 %3) %2))
(def replace-at (partial replacer #"@"))
(def at-underscore (partial replace-at "_"))
(def at-space (partial replace-at ""))
(def replace-space (partial replacer #"\s"))
(def space-underscore (partial replace-space "_"))

(def now-fmt "yyyy-MM-dd-HH-mm-ss.SSS")
(def submission-fmt "yyyy MMM dd HH:mm:ss")

(defn parse-date
  [format-str date-str]
  (. (java.text.SimpleDateFormat. format-str) parse date-str))

(defn fmt-date
  [format-str date-obj]
  (. (java.text.SimpleDateFormat. format-str) format date-obj))

(defn date [] (java.util.Date.))

(defn filetool-env [] (str "PATH=" @icommands-path))

(defn analysis-dirname
  [analysis-name date-str]
  (str analysis-name "-" date-str))

(defn now-date
  ([condor-map]
    (now-date condor-map date))
  ([condor-map date-func]
    (assoc condor-map :now_date (fmt-date now-fmt (date-func)))))

(defn analysis-name
  [condor-map]
  (assoc condor-map :name (-> (:name condor-map) at-underscore space-underscore)))

(defn email
  [condor-map]
  (assoc condor-map :email (:username condor-map)))

(defn username
  [condor-map]
  (assoc condor-map :username (-> (:username condor-map) at-underscore space-underscore)))

(defn set-nfs-base [condor-map] (assoc condor-map :nfs_base @nfs-base))
(defn set-irods-base [condor-map] (assoc condor-map :irods_base @irods-base))

(defn submission-date
  [condor-map]
  (assoc condor-map :submission_date 
         (let [parsed-date  (parse-date now-fmt (:now_date condor-map))
               submission-str (.getTime (date))]
           submission-str)))

(defn output-dir
  [condor-map]
  (let [username     (:username condor-map)
        irods-base   (:irods_base condor-map)
        analysis-dir (analysis-dirname (:name condor-map) (:now_date condor-map))
        output-dir   (ut/add-trailing-slash (ut/path-join irods-base username "analyses" analysis-dir))]
    (assoc condor-map :output_dir output-dir)))

(defn working-dir
  [condor-map]
  (let [username     (:username condor-map)
        nfs-base     (:nfs_base condor-map)
        analysis-dir (analysis-dirname (:name condor-map) (:now_date condor-map))
        working-dir  (ut/add-trailing-slash (ut/path-join nfs-base username analysis-dir))]
    (assoc condor-map :working_dir working-dir)))

(defn condor-log-dir
  [condor-map]
  (let [username     (:username condor-map)
        analysis-dir (analysis-dirname 
                       (:name condor-map)
                       (:now_date condor-map))
        log-dir-path (ut/path-join @condor-log-path username analysis-dir)
        log-dir      (ut/add-trailing-slash log-dir-path)]
    (assoc condor-map :condor-log-dir log-dir)))

(defn step-executables
  [condor-map]
  (assoc condor-map :steps 
         (for [step (:steps condor-map)]
           (assoc step :executable
                  (let [comp (:component step)]
                    (ut/path-join (:location comp) (:name comp)))) )))

(defn step-files
  [condor-map]
  (assoc condor-map 
         :steps
         (for [step     (:steps condor-map)
               step-idx (range 0 (count (:steps condor-map)))]
           (let [working-dir (:working_dir condor-map)
                 condor-log  (:condor-log-dir condor-map)
                 def-stdout  (str "condor-stdout-" step-idx)
                 def-stderr  (str "condor-stderr-" step-idx)
                 def-log     (str "condor-log-" step-idx)
                 stdin       (if (contains? :stdin step)
                               (ut/path-join working-dir (:stdin step))
                               nil)
                 stdout      (if (contains? :stdout step)
                               (ut/path-join working-dir (:stdout step))
                               (ut/path-join working-dir "logs" def-stdout)) 
                 stderr      (if (contains? :stderr step)
                               (ut/path-join working-dir (:stderr step))
                               (ut/path-join working-dir "logs" def-stderr))
                 log-file    (if (contains? :log-file step)
                               (ut/path-join condor-log (:log-file step))
                               (ut/path-join condor-log "logs" def-log))]
             (assoc step 
                    :stdout stdout
                    :stderr stderr
                    :log-file log-file)))))

(defn step-ids
  [condor-map]
  (assoc condor-map :steps
         (for [step     (:steps condor-map)
               step-idx (range 0 (count (:steps condor-map)))]
           (assoc step 
                  :id (str "condor-" step-idx)
                  :type "condor"
                  :submission_date (:submission_date condor-map)
                  :status "Submitted"))))

(defn- param-maps
  [params]
  (for [param params]
    {:name  (:name param)
     :value (:value param)
     :order (:order param)}))

(defn- escape-params
  [params]
  (ae/condorize 
    (flatten 
      (map 
        (fn [p] [(:name p) (:value p)]) 
        (sort-by :order params)))))

(defn step-args
  [condor-map]
  (assoc condor-map :steps
         (for [step     (:steps condor-map)
               step-idx (range 0 (count (:steps condor-map)))]
           (assoc step :arguments
                  (escape-params (param-maps (:params (:config step))))))))

(defn input-jobs
  "Adds output job definitions to the incoming analysis map."
  [condor-map]
  (assoc condor-map :steps
         (for [step     (:steps condor-map)
               step-idx (range 0 (count (:steps condor-map)))]
           (assoc step :input-jobs 
                  (let [working-dir (:working_dir condor-map)
                        condor-log  (:condor-log-dir condor-map)
                        config      (:config step)
                        inputs      (:input config)]
                    (for [input     inputs
                          input-idx (range 0 (count (:input (:config step))))]
                      (let [source   (. (java.net.URI. (:value input)) getPath)
                            ij-id    (str "condor-" step-idx "-input-" input-idx)]
                        {:id              ij-id
                         :submission_date (:submission_date condor-map)
                         :type            "condor"
                         :status          "Submitted"
                         :retain          (:retain input)
                         :multi           (:multiplicity input)
                         :source          source
                         :executable      @filetool-path
                         :environment     (filetool-env)
                         :arguments       (ae/condorize ["-get" "-source" source])
                         :stdout          (ut/path-join working-dir "logs" (str ij-id "-stdout"))
                         :stderr          (ut/path-join working-dir "logs" (str ij-id "-stderr"))
                         :log-file        (ut/path-join condor-log "logs" (str ij-id "-log"))})))))))

(defn output-jobs
  "Adds output job definitions to the incoming analysis map.

   condor-map must have the following key-values before calling:
         :output_dir :working_dir

   The result of this function is a map in each step called :output-jobs
   with the following format:
       {:id String
        :source String
        :dest   String}
  "
  [condor-map]
  (assoc condor-map :steps
         (for [step     (:steps condor-map)
               step-idx (range 0 (count (:steps condor-map)))]
           (assoc step :output-jobs
                  (let [config  (:config step)
                        outputs (:output config)
                        outputs-len (count outputs)]
                    (for [output     outputs
                          output-idx (range 0 outputs-len)]
                      (let [working-dir (:working_dir condor-map)
                            source-base (:name output)]
                        (let [source (ut/path-join working-dir source-base)
                              dest   (:output_dir condor-map)]
                          {:id              (str "condor-" step-idx "-output-" output-idx)
                           :type            "condor"
                           :status          "Submitted"
                           :submission_date (:submission_date condor-map)
                           :retain          (:retain output)
                           :multi           (:multiplicity output)
                           :executable      @filetool-path
                           :arguments       (ae/condorize ["-source" source "-destination" dest])
                           :source          source
                           :dest            dest}))))))))

(defn all-input-jobs
  [condor-map]
  (assoc condor-map :all-input-jobs
         (apply concat (map :input-jobs (:steps condor-map)))))

(defn all-output-jobs 
  [condor-map]
  (assoc condor-map :all-output-jobs
         (apply concat (map :output-jobs (:steps condor-map)))))

(defn- input-coll [working-dir jdef]
  (let [multi (:multi jdef)
        fpath (ut/path-join working-dir (ut/basename (:source jdef)))]
    (if (= multi "collection") (ut/add-trailing-slash fpath) fpath)))

(defn- output-coll [working-dir jdef]
  (let [multi (:multi jdef)
        fpath (:source jdef)]
    (if (= multi "collection") (ut/add-trailing-slash fpath) fpath)))

(defn exclude-arg
  [working-dir inputs outputs]
  (let [input-fixer  (partial input-coll working-dir)
        output-fixer (partial output-coll working-dir)
        not-retain   (comp not :retain)
        input-paths  (map input-fixer (filter not-retain inputs))
        output-paths (map output-fixer (filter not-retain outputs))
        all-paths    (flatten (conj input-paths output-paths))]
    (if (> (count all-paths) 0) 
      (list "-exclude" (string/join "," all-paths)) 
      [])))

(defn imkdir-job
  [condor-map]
  (let [working-dir (:working_dir condor-map)
        output-dir  (:output_dir condor-map)
        condor-log  (:condor-log-dir condor-map)]
    (assoc condor-map :imkdir-job
           {:id "imkdir"
            :executable @filetool-path
            :environment (filetool-env)
            :stderr (ut/path-join working-dir "logs" "imkdir-stderr")
            :stdout (ut/path-join working-dir "logs" "imkdir-stdout")
            :log-file (ut/path-join condor-log "logs" "imkdir-log")
            :arguments (ae/condorize ["-mkdir" "-destination" output-dir])})))

(defn shotgun-job
  [condor-map]
  (let [working-dir  (:working_dir condor-map)
        output-dir   (:output_dir condor-map)
        condor-log   (:condor-log-dir condor-map)
        cinput-jobs  (:all-input-jobs condor-map)
        coutput-jobs (:all-output-jobs condor-map)]
    (assoc condor-map :final-output-job
         {:id          "output-last"
          :executable  @filetool-path
          :environment (filetool-env)
          :stderr      (ut/path-join working-dir "logs" "output-last-stderr")
          :stdout      (ut/path-join working-dir "logs" "output-last-stdout")
          :log-file    (ut/path-join condor-log "logs" "output-last-log")
          :arguments   (ae/condorize  
                         (flatten ["-source"      (:working_dir condor-map) 
                                   "-destination" (:output_dir condor-map)
                                   (exclude-arg working-dir cinput-jobs coutput-jobs)]))})))

(defn rm-step-component
  [condor-map]
  (assoc condor-map :steps
         (for [step (:steps condor-map)]
           (dissoc step :component))))

(defn rm-step-config
  [condor-map]
  (assoc condor-map :steps
         (for [step (:steps condor-map)]
           (dissoc step :config))))

(defn transform
  "Transeforms the condor-map that's passed in into something more useable."
  [condor-map]
  (-> condor-map
    set-irods-base
    set-nfs-base
    now-date
    analysis-name
    email
    username
    submission-date
    condor-log-dir
    output-dir
    working-dir
    step-executables
    step-files
    step-ids
    step-args
    input-jobs
    output-jobs
    all-input-jobs
    all-output-jobs
    imkdir-job
    shotgun-job
    rm-step-component
    rm-step-config))
