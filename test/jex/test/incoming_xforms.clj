(ns jex.test.incoming-xforms
  (:use [jex.incoming-xforms] :reload)
  (:use [midje.sweet]))

(fact
 (replacer #"ll" "oo" "fll") => "foo"
 (replacer #"\s" "_" "foo oo") => "foo_oo")

(fact
 (replace-at "A" "foo@bar") => "fooAbar"
 (replace-at "A" "foobar@") => "foobarA"
 (replace-at "A" "@foobar") => "Afoobar")

(fact
 (at-underscore "foo@bar") => "foo_bar"
 (at-underscore "foobar@") => "foobar_"
 (at-underscore "@foobar") => "_foobar")

(fact
 (replace-space "^" "foo bar") => "foo^bar"
 (replace-space "^" "foobar ") => "foobar^"
 (replace-space "^" " foobar") => "^foobar")

(fact
 (space-underscore "foo bar") => "foo_bar"
 (space-underscore "foobar ") => "foobar_"
 (space-underscore " foobar") => "_foobar")

(fact
 (fmt-date now-fmt (java.util.Date. 0)) => "1969-12-31-17-00-00.000")

(fact
 (now-date {}) => #(contains? % :now_date)
 (now-date {} #(java.util.Date. 0)) => {:now_date "1969-12-31-17-00-00.000"})

(fact
 (pathize "/foo bar/baz@blippy/") => "/foo_bar/baz_blippy/")

(reset! run-on-nfs false)
(reset! nfs-base "/tmp/nfs-base")
(reset! irods-base "/tmp/irods-base")
(reset! condor-log-path "/tmp/condor-log-path")

(defn epoch-func [] (java.util.Date. 0))

(fact
 (analysis-attrs {:username "wregglej"} epoch-func) => {:run-on-nfs  @run-on-nfs
                                                        :type "analysis"
                                                        :username "wregglej"
                                                        :nfs_base @nfs-base
                                                        :irods_base  @irods-base
                                                        :submission_date 0}
 (analysis-attrs {:username "wr @lej"} epoch-func) => {:run-on-nfs @run-on-nfs
                                                       :type "analysis"
                                                       :username "wr__lej"
                                                       :nfs_base @nfs-base
                                                       :irods_base @irods-base
                                                       :submission_date 0}
 (analysis-attrs {:username "wregglej" :type "foo"} epoch-func) =>
 {:run-on-nfs @run-on-nfs
  :type "foo"
  :username "wregglej"
  :nfs_base @nfs-base
  :irods_base @irods-base
  :submission_date 0})

(def out-dir-test-0
  {:irods_base @irods-base
   :username "wregglej"
   :name "out-dir-test-0"
   :now_date "1969-12-31-17-00-00.000"})

(def out-dir-test-1
  {:irods_base @irods-base
   :username "wregglej"
   :output_dir ""
   :create_output_subdir true
   :name "out-dir-test-1"
   :now_date "1969-12-31-17-00-00.000"})

(def out-dir-test-2
  {:irods_base @irods-base
   :username "wregglej"
   :now_date "1969-12-31-17-00-00.000"
   :name "out-dir-test-2"
   :create_output_subdir false
   :output_dir ""})

(def out-dir-test-3
  {:irods_base @irods-base
   :username "wregglej"
   :now_date "1969-12-31-17-00-00.000"
   :name "out-dir-test-3"
   :create_output_subdir true
   :output_dir "/my/output-dir/"})

(def out-dir-test-4
  (assoc out-dir-test-3
    :create_output_subdir false
    :name "out-dir-test-4"))

(fact
 (output-directory out-dir-test-0) =>
 "/tmp/irods-base/wregglej/analyses/out-dir-test-0-1969-12-31-17-00-00.000"

 (output-directory out-dir-test-1) =>
 "/tmp/irods-base/wregglej/analyses/out-dir-test-1-1969-12-31-17-00-00.000"

 (output-directory out-dir-test-2) =>
 "/tmp/irods-base/wregglej/analyses/out-dir-test-2-1969-12-31-17-00-00.000"

 (output-directory out-dir-test-3) =>
 "/my/output-dir/out-dir-test-3-1969-12-31-17-00-00.000"

 (output-directory out-dir-test-4) =>
 "/my/output-dir")

(def context-dirs-map
  {:username "wregglej"
   :nfs_base @nfs-base
   :irods_base @irods-base
   :name "context-dirs-map"
   :create_output_subdir true
   :output_dir "/my/output-dir"
   :now_date "1969-12-31-17-00-00.000"})

(fact
 (context-dirs context-dirs-map) =>
 (merge
  context-dirs-map
  {:output_dir
   (output-directory context-dirs-map)
   
   :working_dir
   "/tmp/nfs-base/wregglej/context-dirs-map-1969-12-31-17-00-00.000/"
   
   :condor-log-dir
   "/tmp/condor-log-path/wregglej/context-dirs-map-1969-12-31-17-00-00.000/"}))

(def p
  [{:name "-n0" :value "v0" :order "0" :foo "bar"}
   {:name "-n1" :value "v1" :order "1" :bar "a"}
   {:name "-n2" :value "v2" :order "2" :bees "no!"}
   {:name "-n3" :value "v3" :order "3" :bears "oh my"}])

(fact
 (param-maps p) => [{:name "-n0" :value "v0" :order "0"}
                    {:name "-n1" :value "v1" :order "1"}
                    {:name "-n2" :value "v2" :order "2"}
                    {:name "-n3" :value "v3" :order "3"}])

(fact
 (naively-quote "foo 'bar' baz") => "'foo '\\''bar'\\'' baz'"
 (naively-quote "''foo''") => "''\\'''\\''foo'\\'''\\'''")

(fact
 (quote-value "foo 'bar' baz") => "'foo '\\''bar'\\'' baz'"
 (quote-value "''foo''") => "\\'''\\''foo'\\'''\\'")

(def fancy-params
  [{:name "-n1" :value "foo 'bar' baz" :order 5}
   {:name "-n2" :value "''foo''" :order 4}
   {:name "-n3" :value "nargle" :order 3}])

(fact
 (escape-params fancy-params) =>
 "-n3 'nargle' -n2 \\'''\\''foo'\\'''\\' -n1 'foo '\\''bar'\\'' baz'")

(fact
 (format-env-variables {:foo "bar" :baz "blippy"}) =>
 "foo=\"bar\" baz=\"blippy\"")

(def step-map
  {:component
   {:location "/usr/local/bin/"
    :name "footastic"}
   :config
   {:params p}
   :stdin "/test/stdin"
   :stdout "/test/stdout"
   :stderr "/test/stderr"
   :environment {:foo "bar" :baz "blippy"}})

(fact
 (executable step-map) => "/usr/local/bin/footastic")

(fact
 (arguments step-map) => "-n0 'v0' -n1 'v1' -n2 'v2' -n3 'v3'")

(fact
 (stdin step-map) => "'/test/stdin'")

(fact
 (stdout step-map 0) => "'/test/stdout'")

(fact
 (stderr step-map 0) => "'/test/stderr'")

(fact
 (environment step-map) => "foo=\"bar\" baz=\"blippy\"")

(fact
 (log-file {:log-file "log-file"} 0 "/tmp/logs") => "/tmp/logs/log-file"
 (log-file {} 0 "/tmp/logs") => "/tmp/logs/logs/condor-log-0")

(fact
 (step-iterator-vec {:steps [{} {} {}]}) => [[0 {}] [1 {}] [2 {}]])

(def step-map1
  {:component
   {:location "/usr/local/bin"
    :name "footastic1"}
   :config {:params p}
   :stdin "/test/stdin1"
   :stdout "/test/stdout1"
   :stderr "/test/stderr1"
   :environment {:PATH "/usr/local/bin"}
   :log-file "log-file1"})

(def condor-map
  {:steps [step-map step-map1]
   :submission_date 0
   :condor-log-dir "/tmp"})

(fact
 (process-steps condor-map) =>
 (sequence
  [
   {:id "condor-0"
    :type "condor"
    :submission_date 0
    :status "Submitted"
    :environment "foo=\"bar\" baz=\"blippy\""
    :executable "/usr/local/bin/footastic"
    :arguments "-n0 'v0' -n1 'v1' -n2 'v2' -n3 'v3'"
    :stdout "'/test/stdout'"
    :stderr "'/test/stderr'"
    :stdin "/test/stdin"
    :log-file "/tmp/logs/condor-log-0"
    :component
    {:location "/usr/local/bin/"
     :name "footastic"}
    :config
    {:params p}}
   {:id "condor-1"
    :type "condor"
    :submission_date 0
    :status "Submitted"
    :environment "PATH=\"/usr/local/bin\""
    :executable "/usr/local/bin/footastic1"
    :arguments "-n0 'v0' -n1 'v1' -n2 'v2' -n3 'v3'"
    :stdout "'/test/stdout1'"
    :stderr "'/test/stderr1'"
    :stdin "/test/stdin1"
    :log-file "/tmp/log-file1"
    :component
    {:location "/usr/local/bin"
     :name "footastic1"}
    :config {:params p}}]))

(fact
 (steps condor-map) =>
 (assoc condor-map
   :steps (sequence
           [
            {:id "condor-0"
             :type "condor"
             :submission_date 0
             :status "Submitted"
             :environment "foo=\"bar\" baz=\"blippy\""
             :executable "/usr/local/bin/footastic"
             :arguments "-n0 'v0' -n1 'v1' -n2 'v2' -n3 'v3'"
             :stdout "'/test/stdout'"
             :stderr "'/test/stderr'"
             :stdin "/test/stdin"
             :log-file "/tmp/logs/condor-log-0"
             :component
             {:location "/usr/local/bin/"
              :name "footastic"}
             :config
             {:params p}}
            {:id "condor-1"
             :type "condor"
             :submission_date 0
             :status "Submitted"
             :environment "PATH=\"/usr/local/bin\""
             :executable "/usr/local/bin/footastic1"
             :arguments "-n0 'v0' -n1 'v1' -n2 'v2' -n3 'v3'"
             :stdout "'/test/stdout1'"
             :stderr "'/test/stderr1'"
             :stdin "/test/stdin1"
             :log-file "/tmp/log-file1"
             :component
             {:location "/usr/local/bin"
              :name "footastic1"}
             :config {:params p}}])))

(fact
 (handle-source-path "/tmp/foo" "collection") => "/tmp/foo/"
 (handle-source-path "/tmp/foo" "single") => "/tmp/foo"
 (handle-source-path "/tmp/foo" "") => "/tmp/foo"
 (handle-source-path "/tmp/foo" nil) => "/tmp/foo")

(fact
 (input-id-str 0 0) => "condor-0-input-0"
 (input-id-str 0 1) => "condor-0-input-1")

(fact
 (input-stdout 0 0) => "logs/condor-0-input-0-stdout"
 (input-stdout 0 1) => "logs/condor-0-input-1-stdout")

(fact
 (input-stderr 0 0) => "logs/condor-0-input-0-stderr"
 (input-stderr 0 1) => "logs/condor-0-input-1-stderr")

(fact
 (input-log-file "/tmp" 0 0) => "/tmp/logs/condor-0-input-0-log"
 (input-log-file "/tmp" 0 1) => "/tmp/logs/condor-0-input-1-log")

(fact
 (input-arguments "foo" "/tmp/foo" {:multiplicity "collection"}) =>
 "get --user foo --source '/tmp/foo/'"

 (input-arguments "foo" "/tmp/foo" {:multiplicity "single"}) =>
 "get --user foo --source '/tmp/foo'"

 (input-arguments "foo" "/tmp/foo" {:multiplicity ""}) =>
 "get --user foo --source '/tmp/foo'"

 (input-arguments "foo" "/tmp/foo" {}) =>
 "get --user foo --source '/tmp/foo'")

(fact
 (input-iterator-vec {:config {:input [{:step 1} {:step 2} {:step 3}]}}) =>
 [[0 {:step 1}] [1 {:step 2}] [2 {:step 3}]])

(reset! filetool-path "/usr/local/bin/filetool")
(reset! icommands-path "/usr/local/bin")

(def input-condor-map
  {:submission_date 0
   :username "foo"
   :condor-log-dir "/tmp"
   :steps
   [{:config
     {:input
      [{:retain true
        :multiplicity "collection"
        :value "/tmp/source"}
       {:retain false
        :multiplicity "single"
        :value "/tmp/source1"}]}}]})

(fact
 (process-step-inputs
  input-condor-map
  [0
   {:config
    {:input
     [{:retain true
       :multiplicity "collection"
       :value "/tmp/source"}
      {:retain false
       :multiplicity "single"
       :value "/tmp/source1"}]}}]) =>
      (sequence
       [{:id "condor-0-input-0"
         :submission_date 0
         :type "condor"
         :status "Submitted"
         :retain true
         :multi "collection"
         :source "/tmp/source"
         :executable "/usr/local/bin/filetool"
         :environment "PATH=/usr/local/bin"
         :arguments "get --user foo --source '/tmp/source/'"
         :stdout "logs/condor-0-input-0-stdout"
         :stderr "logs/condor-0-input-0-stderr"
         :log-file "/tmp/logs/condor-0-input-0-log"}
        {:id "condor-0-input-1"
         :submission_date 0
         :type "condor"
         :status "Submitted"
         :retain false
         :multi "single"
         :source "/tmp/source1"
         :executable "/usr/local/bin/filetool"
         :environment "PATH=/usr/local/bin"
         :arguments "get --user foo --source '/tmp/source1'"
         :stdout "logs/condor-0-input-1-stdout"
         :stderr "logs/condor-0-input-1-stderr"
         :log-file "/tmp/logs/condor-0-input-1-log"}]))

(fact
 (process-inputs input-condor-map) =>
 (sequence
  [{:input-jobs
    (sequence
     [{:id "condor-0-input-0"
       :submission_date 0
       :type "condor"
       :status "Submitted"
       :retain true
       :multi "collection"
       :source "/tmp/source"
       :executable "/usr/local/bin/filetool"
       :environment "PATH=/usr/local/bin"
       :arguments "get --user foo --source '/tmp/source/'"
       :stdout "logs/condor-0-input-0-stdout"
       :stderr "logs/condor-0-input-0-stderr"
       :log-file "/tmp/logs/condor-0-input-0-log"}
      {:id "condor-0-input-1"
       :submission_date 0
       :type "condor"
       :status "Submitted"
       :retain false
       :multi "single"
       :source "/tmp/source1"
       :executable "/usr/local/bin/filetool"
       :environment "PATH=/usr/local/bin"
       :arguments "get --user foo --source '/tmp/source1'"
       :stdout "logs/condor-0-input-1-stdout"
       :stderr "logs/condor-0-input-1-stderr"
       :log-file "/tmp/logs/condor-0-input-1-log"}])
    :config
    {:input
     [{:retain true
       :multiplicity "collection"
       :value "/tmp/source"}
      {:retain false
       :multiplicity "single"
       :value "/tmp/source1"}]}}]))

(fact
 (input-jobs input-condor-map) =>
 {:submission_date 0
  :username "foo"
  :condor-log-dir "/tmp"
  :steps
  [{:input-jobs
    (sequence
     [{:id "condor-0-input-0"
       :submission_date 0
       :type "condor"
       :status "Submitted"
       :retain true
       :multi "collection"
       :source "/tmp/source"
       :executable "/usr/local/bin/filetool"
       :environment "PATH=/usr/local/bin"
       :arguments "get --user foo --source '/tmp/source/'"
       :stdout "logs/condor-0-input-0-stdout"
       :stderr "logs/condor-0-input-0-stderr"
       :log-file "/tmp/logs/condor-0-input-0-log"}
      {:id "condor-0-input-1"
       :submission_date 0
       :type "condor"
       :status "Submitted"
       :retain false
       :multi "single"
       :source "/tmp/source1"
       :executable "/usr/local/bin/filetool"
       :environment "PATH=/usr/local/bin"
       :arguments "get --user foo --source '/tmp/source1'"
       :stdout "logs/condor-0-input-1-stdout"
       :stderr "logs/condor-0-input-1-stderr"
       :log-file "/tmp/logs/condor-0-input-1-log"}])
    :config
    {:input
     [{:retain true
       :multiplicity "collection"
       :value "/tmp/source"}
      {:retain false
       :multiplicity "single"
       :value "/tmp/source1"}]}}]})

(fact
 (output-arguments "foo" "/tmp/source" "/tmp/dest") =>
 "put --user foo --source '/tmp/source' --destination '/tmp/dest'")

(fact
 (output-id-str 0 0) => "condor-0-output-0"
 (output-id-str 0 1) => "condor-0-output-1")

(fact
 (output-iterator-vec {:config {:output [{:step 0} {:step 1} {:step 2}]}}) =>
 [[0 {:step 0}] [1 {:step 1}] [2 {:step 2}]])

(def output-condor-map
  {:submission_date 0
   :username "foo"
   :condor-log-dir "/tmp"
   :output_dir "/tmp/output-dir"
   :steps
   [{:config
     {:output
      [{:retain true
        :multiplicity "collection"
        :name "/tmp/source"}
       {:retain false
        :multiplicity "single"
        :name "/tmp/source1"}]}}]})

(fact
 (process-step-outputs
  output-condor-map
  [0
   {:config
    {:output
     [{:retain true
       :multiplicity "collection"
       :name "/tmp/source"}
      {:retain false
       :multiplicity "single"
       :name "/tmp/source1"}]}}]) =>
       (sequence
        [{:id "condor-0-output-0"
          :submission_date 0
          :type "condor"
          :status "Submitted"
          :retain true
          :multi "collection"
          :source "/tmp/source"
          :executable "/usr/local/bin/filetool"
          :environment "PATH=/usr/local/bin"
          :arguments "put --user foo --source '/tmp/source' --destination '/tmp/output-dir'"
          :dest "/tmp/output-dir"}
         {:id "condor-0-output-1"
          :submission_date 0
          :type "condor"
          :status "Submitted"
          :retain false
          :multi "single"
          :source "/tmp/source1"
          :executable "/usr/local/bin/filetool"
          :environment "PATH=/usr/local/bin"
          :arguments "put --user foo --source '/tmp/source1' --destination '/tmp/output-dir'"
          :dest "/tmp/output-dir"}]))