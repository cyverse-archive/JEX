(ns jex.test.dagify
  (:use [jex.dagify] :reload)
  (:use midje.sweet))

(fact
 (script-output "/tmp/script-dir") => "/tmp/script-dir/logs/script-output.log")

(fact
 (script-error "/tmp/script-dir") => "/tmp/script-dir/logs/script-error.log")

(fact
 (script-log "/tmp/log-dir") => "/tmp/log-dir/script-condor-log")

(fact
 (script-submission
  "testuser"
  "testuuid"
  "/tmp/script-dir"
  "/tmp/script-path"
  "/tmp/log-dir") =>
  (str
   "universe = vanilla\n"
   "executable = /bin/bash\n"
   "arguments = \"/tmp/script-path\"\n"
   "output = /tmp/script-dir/logs/script-output.log\n"
   "error = /tmp/script-dir/logs/script-error.log\n"
   "log = /tmp/log-dir/script-condor-log\n"
   "+IpcUuid = \"testuuid\"\n"
   "+IpcJobId = \"generated_script\"\n"
   "+IpcUsername = \"testuser\"\n"
   "should_transfer_files = NO\n"
   "notification = NEVER\n"
   "queue\n"))

(fact
 (jobs-in-order
  {:imkdir-job "imkdir"
   :all-input-jobs ["all-input-jobs"]
   :steps ["steps"]
   :final-output-job "final-output-job"}) =>
   ["imkdir"
    "all-input-jobs"
    "steps"
    "final-output-job"])

(fact
 (script-line
  {:environment "environment"
   :executable "executable"
   :arguments "arguments"
   :stderr "stderr"
   :stdout "stdout"}) =>
   (str
    "environment executable arguments 1> stdout 2> stderr\n"
    "if [ ! \"$?\" -eq \"0\" ]; then\n"
    "\tEXITSTATUS=1\n"
    "fi\n"))

(def test-analysis
  {:imkdir-job
   {:environment "imkdir-env"
    :executable "imkdir-exec"
    :arguments "imkdir-args"
    :stderr "imkdir-stderr"
    :stdout "imkdir-stdout"}
   :final-output-job
   {:environment "final-job-env"
    :executable "final-job-exec"
    :arguments "final-job-args"
    :stderr "final-job-stderr"
    :stdout "final-job-stdout"}
   :all-input-jobs
   [{:environment "input-1-env"
     :executable "input-1-exec"
     :arguments "input-1-args"
     :stderr "input-1-stderr"
     :stdout "input-1-stdout"}
    {:environment "input-2-env"
     :executable "input-2-exec"
     :arguments "input-2-args"
     :stderr "input-2-stderr"
     :stdout "input-2-stdout"}]
   :steps
   [{:environment "step-1-env"
     :executable "step-1-exec"
     :arguments "step-1-args"
     :stderr "step-1-stderr"
     :stdout "step-1-stdout"}
    {:environment "step-2-env"
     :executable "step-2-exec"
     :arguments "step-2-args"
     :stderr "step-2-stderr"
     :stdout "step-2-stdout"}]
   :uuid "testuuid"
   :username "testuser"})

(fact
 (script test-analysis) =>
 (str
  "#!/bin/bash\n"
  "cd ~\n"
  "mkdir -p iplant-de-jobs/testuser/testuuid\n"
  "pushd iplant-de-jobs/testuser/testuuid\n"
  "mkdir -p logs\n"
  "EXITSTATUS=0\n"
  "imkdir-env imkdir-exec imkdir-args 1> imkdir-stdout 2> imkdir-stderr\n"
  "if [ ! \"$?\" -eq \"0\" ]; then\n"
  "\tEXITSTATUS=1\n"
  "fi\n\n"
  "input-1-env input-1-exec input-1-args 1> input-1-stdout 2> input-1-stderr\n"
  "if [ ! \"$?\" -eq \"0\" ]; then\n"
  "\tEXITSTATUS=1\n"
  "fi\n\n"
  "input-2-env input-2-exec input-2-args 1> input-2-stdout 2> input-2-stderr\n"
  "if [ ! \"$?\" -eq \"0\" ]; then\n"
  "\tEXITSTATUS=1\n"
  "fi\n\n"
  "step-1-env step-1-exec step-1-args 1> step-1-stdout 2> step-1-stderr\n"
  "if [ ! \"$?\" -eq \"0\" ]; then\n"
  "\tEXITSTATUS=1\n"
  "fi\n\n"
  "step-2-env step-2-exec step-2-args 1> step-2-stdout 2> step-2-stderr\n"
  "if [ ! \"$?\" -eq \"0\" ]; then\n"
  "\tEXITSTATUS=1\n"
  "fi\n\n"
  "final-job-env final-job-exec final-job-args 1> final-job-stdout 2> final-job-stderr\n"
  "if [ ! \"$?\" -eq \"0\" ]; then\n"
  "\tEXITSTATUS=1\n"
  "fi\n"
  "popd\n"
  "rm -r iplant-de-jobs/testuser/testuuid\n"
  "exit $EXITSTATUS\n"))