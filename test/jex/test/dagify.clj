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