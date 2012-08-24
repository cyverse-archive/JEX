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

