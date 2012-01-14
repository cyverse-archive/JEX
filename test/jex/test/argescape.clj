(ns jex.test.argescape
  (:use [jex.argescape] :reload)
  (:use [clojure.test]))

(deftest test-single?
  (is (single? "'"))
  (is (not (single? "")))
  (is (not (single? "aabb"))))

(deftest test-double?
  (is (double? "\""))
  (is (not (double? "aaabbbb")))
  (is (not (double? "")))
  (is (double? "aaa\"bbb")))

(deftest test-space?
  (is (space? " "))
  (is (not (space? "")))
  (is (space? "aaa bbb ccc"))
  (is (not (space? "aaabbbccc"))))

(deftest test-double-single
  (is (= (double-single "'") "''"))
  (is (= (double-single "aa'bb") "aa''bb"))
  (is (= (double-single "aabb") "aabb"))
  (is (= (double-single "aa bb") "aa bb")))

(deftest test-double-double
  (is (= (double-double "\"") "\"\""))
  (is (= (double-double "aa\"bb") "aa\"\"bb"))
  (is (= (double-double "\"aa bb\"") "\"\"aa bb\"\""))
  (is (= (double-double "aabbcc") "aabbcc"))
  (is (= (double-double "aa bb cc") "aa bb cc")))

(deftest test-wrap-single
  (is (= (wrap-single "a") "'a'"))
  (is (= (wrap-single "\"a\"") "'\"a\"'"))
  (is (= (wrap-single "aa \"bb\" cc") "'aa \"bb\" cc'")))

(deftest test-wrap-double
  (is (= (wrap-double "a") "\"a\""))
  (is (= (wrap-double "a b") "\"a b\""))
  (is (= (wrap-double "a 'b' c") "\"a 'b' c\"")))

(deftest test-space-and-single
  (is (= (space-and-single "ab") "ab"))
  (is (= (space-and-single "\"ab\"") "\"ab\""))
  (is (= (space-and-single "a b") "'a b'"))
  (is (= (space-and-single "a'b'c") "'a''b''c'"))
  (is (= (space-and-single "'a'b'c'") "'''a''b''c'''"))
  (is (= (space-and-single "a  'b'  c'") "'a  ''b''  c'''"))
  (is (= (space-and-single "a  \"'b'\"  c") "'a  \"''b''\"  c'")))

(deftest test-escape
  (is (= (escape "ab") "ab"))
  (is (= (escape "\"ab\"") "\"\"ab\"\""))
  (is (= (escape "a b") "'a b'"))
  (is (= (escape "a'b'c") "'a''b''c'"))
  (is (= (escape "'a'b'c'") "'''a''b''c'''"))
  (is (= (escape "a  'b'  c'") "'a  ''b''  c'''"))
  (is (= (escape "a  \"'b'\"  c") "'a  \"\"''b''\"\"  c'")))

(deftest test-condorize
  (is (= (condorize ["a" "b" "c"]) "\"a b c\""))
  (is (= (condorize ["a b" "c" "d"]) "\"'a b' c d\""))
  (is (= (condorize ["'a'" "\"b\"" "'c'"]) "\"'''a''' \"\"b\"\" '''c'''\""))
  (is (= (condorize ["one" "\"two\"" "spacey 'quoted' argument"]) "\"one \"\"two\"\" 'spacey ''quoted'' argument'\"")))
