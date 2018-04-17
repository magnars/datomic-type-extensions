(ns datomic-type-extensions.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [datomic-type-extensions.query :as sut]))

(def attr-types
  {:user/created-at :java.time/instant
   :user/updated-at :java.time/instant
   :client/id :keyword-backed-by-string})

(deftest deserialization-pattern
  (testing "a single value"
    (testing "- serialized"
      (is (= :java.time/instant
             (sut/deserialization-pattern
              '[:find ?v . :where [?e :user/created-at ?v]]
              attr-types))))

    (testing "- not serialized"
      (is (nil? (sut/deserialization-pattern
                 '[:find ?v . :where [?e :user/name ?v]]
                 attr-types))))

    (testing "- entity id"
      (is (nil? (sut/deserialization-pattern
                 '[:find ?e . :where [?e :user/created-at ?v]]
                 attr-types)))))

  (testing "vector"
    (is (= {:type :vector
            :pattern :java.time/instant}
           (sut/deserialization-pattern
            '[:find [?v ...] :where [?e :user/created-at ?v]]
            attr-types))))

  (testing "sets of tuples"
    (is (= {:type :set
            :pattern {:type :tuple
                      :entries [nil :java.time/instant]}}
           (sut/deserialization-pattern
            '[:find ?e ?v :where [?e :user/created-at ?v]]
            attr-types))))

  (testing "pull syntax"
    (is (= {:type :vector
            :pattern {:type :deserializable-form}}
           (sut/deserialization-pattern
            '[:find [(pull ?e [:user/email :user/created-at]) ...]
              :where [?e :user/created-at ?v]]
            attr-types)))

    (is (= {:type :deserializable-form}
           (sut/deserialization-pattern
            '[:find (pull ?e [:user/email :user/created-at]) .
              :where [?e :user/created-at ?v]]
            attr-types)))

    (is (= {:type :set
            :pattern {:type :tuple
                      :entries [nil {:type :deserializable-form}]}}
           (sut/deserialization-pattern
            '[:find ?e (pull ?e [:user/email]) :where [?e :user/created-at ?v]]
            attr-types)))))

(deftest deserialize-by-pattern
  (is (= :client-id
         (sut/deserialize-by-pattern "client-id" :keyword-backed-by-string {})))

  (is (= [:client-id-1 :client-id-2]
         (sut/deserialize-by-pattern ["client-id-1" "client-id-2"]
                                     {:type :vector
                                      :pattern :keyword-backed-by-string}
                                     {})))

  (is (= ["not-a-client-id" :client-id "nope"]
         (sut/deserialize-by-pattern ["not-a-client-id" "client-id" "nope"]
                                     {:type :tuple
                                      :entries [nil :keyword-backed-by-string nil]}
                                     {})))

  (is (= #{["not-a-client-id" :client-id]}
         (sut/deserialize-by-pattern #{["not-a-client-id" "client-id"]}
                                     {:type :set
                                      :pattern {:type :tuple
                                                :entries [nil :keyword-backed-by-string]}}
                                     {}))))

(deftest find-var->type-mapping
  (is (= {'?v :java.time/instant}
         (sut/find-var->type-mapping '[:find ?v :where [_ :user/created-at ?v]]
                                     attr-types)))

  (is (= {'?updated :java.time/instant}
         (sut/find-var->type-mapping '[:find ?email ?updated
                                       :in $
                                       :where [?e :user/email ?email]
                                       [(get-else $ ?e :user/updated-at nil) ?updated]]
                                     attr-types))))
