(ns jex.test.json-validator
  (:use [jex.json-validator] :reload)
  (:use [clojure.test]))

(deftest test-json
  (is (json? "{\"foo\" : \"bar\"}") )
  (is (not (json? "dfadsfa"))))

(deftest test-valid
  (let [example {"foo" 1 "bar" {"baz" 2 "blippy" {"blop" 3}}}]
    (is (valid? example [#(integer? (get % "foo"))]))
    (is (valid? example [#(integer? (get % "foo"))
                         #(map? (get % "bar"))]))
    (is (valid? example [#(integer? (get % "foo"))
                         #(map? (get % "bar"))
                         #(integer? (get (get % "bar") "baz"))
                         #(map? (get (get % "bar") "blippy"))
                         #(integer? (get (get (get % "bar") "blippy") "blop"))]))
    (is (not (valid? example [#(integer? (get % "foo"))
                              #(map? (get % "bar"))
                              #(integer? (get (get % "bar") "baz"))
                              #(map? (get (get % "bar") "blippy"))
                              #(string? (get (get (get % "bar") "blippy") "blop"))])))))



