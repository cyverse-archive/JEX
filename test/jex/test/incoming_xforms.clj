(ns jex.test.incoming-xforms
  (:use [jex.incoming-xforms] :reload)
  (:use [midje.sweet]))

(fact
 (replacer #"ll" "oo" "fll") => "foo"
 (replacer #"\s" "_" "foo oo") => "foo_oo")