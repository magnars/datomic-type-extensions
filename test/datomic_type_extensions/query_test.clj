(ns datomic-type-extensions.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [datomic-type-extensions.query :as sut]))

(defn attr-info [value-type]
  {:dte/valueType value-type
   :db/cardinality :db.cardinality/one})

(def attr->attr-info
  {:user/created-at (attr-info :java.time/instant)
   :user/updated-at (attr-info :java.time/instant)
   :client/id (attr-info :keyword-backed-by-string)})

(deftest deserialization-pattern
  (testing "a single value"
    (testing "- serialized"
      (is (= :java.time/instant
             (sut/deserialization-pattern
              '[:find ?v . :where [?e :user/created-at ?v]]
              attr->attr-info))))

    (testing "- not serialized"
      (is (nil? (sut/deserialization-pattern
                 '[:find ?v . :where [?e :user/name ?v]]
                 attr->attr-info))))

    (testing "- entity id"
      (is (nil? (sut/deserialization-pattern
                 '[:find ?e . :where [?e :user/created-at ?v]]
                 attr->attr-info)))))

  (testing "vector"
    (is (= {:type :vector
            :pattern :java.time/instant}
           (sut/deserialization-pattern
            '[:find [?v ...] :where [?e :user/created-at ?v]]
            attr->attr-info))))

  (testing "sets of tuples"
    (is (= {:type :set
            :pattern {:type :tuple
                      :entries [nil :java.time/instant]}}
           (sut/deserialization-pattern
            '[:find ?e ?v :where [?e :user/created-at ?v]]
            attr->attr-info))))

  (testing "pull syntax"
    (is (= {:type :vector
            :pattern {:type :deserializable-form}}
           (sut/deserialization-pattern
            '[:find [(pull ?e [:user/email :user/created-at]) ...]
              :where [?e :user/created-at ?v]]
            attr->attr-info)))

    (is (= {:type :deserializable-form}
           (sut/deserialization-pattern
            '[:find (pull ?e [:user/email :user/created-at]) .
              :where [?e :user/created-at ?v]]
            attr->attr-info)))

    (is (= {:type :set
            :pattern {:type :tuple
                      :entries [nil {:type :deserializable-form}]}}
           (sut/deserialization-pattern
            '[:find ?e (pull ?e [:user/email]) :where [?e :user/created-at ?v]]
            attr->attr-info)))))

(deftest deserialization-pattern-with-map-query
  (testing "a single value"
    (testing "- serialized"
      (is (= :java.time/instant
             (sut/deserialization-pattern
              '{:find [?v .] :where [[?e :user/created-at ?v]]}
              attr->attr-info))))

    (testing "- not serialized"
      (is (nil? (sut/deserialization-pattern
                 '{:find [?v .] :where [[?e :user/name ?v]]}
                 attr->attr-info))))

    (testing "- entity id"
      (is (nil? (sut/deserialization-pattern
                 '{:find [?e .] :where [[?e :user/created-at ?v]]}
                 attr->attr-info)))))

  (testing "vector"
    (is (= {:type :vector
            :pattern :java.time/instant}
           (sut/deserialization-pattern
            '{:find [[?v ...]] :where [[?e :user/created-at ?v]]}
            attr->attr-info))))

  (testing "sets of tuples"
    (is (= {:type :set
            :pattern {:type :tuple
                      :entries [nil :java.time/instant]}}
           (sut/deserialization-pattern
            '{:find [?e ?v] :where [[?e :user/created-at ?v]]}
            attr->attr-info))))

  (testing "pull syntax"
    (is (= {:type :vector
            :pattern {:type :deserializable-form}}
           (sut/deserialization-pattern
            '{:find [[(pull ?e [:user/email :user/created-at]) ...]]
              :where [[?e :user/created-at ?v]]}
            attr->attr-info)))

    (is (= {:type :deserializable-form}
           (sut/deserialization-pattern
            '{:find [(pull ?e [:user/email :user/created-at]) .]
              :where [[?e :user/created-at ?v]]}
            attr->attr-info)))

    (is (= {:type :set
            :pattern {:type :tuple
                      :entries [nil {:type :deserializable-form}]}}
           (sut/deserialization-pattern
            '{:find [?e (pull ?e [:user/email])]
              :where [[?e :user/created-at ?v]]}
            attr->attr-info)))))

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
                                     attr->attr-info)))

  (is (= {'?updated :java.time/instant}
         (sut/find-var->type-mapping '[:find ?email ?updated
                                       :in $
                                       :where [?e :user/email ?email]
                                       [(get-else $ ?e :user/updated-at nil) ?updated]]
                                     attr->attr-info))))

(deftest vector-style-query->map-style-query
  (is
   (= '{:find [?name]
        :where [[_ :person/name ?name]]}
      (sut/vector-style-query->map-style-query
       '[:find ?name
         :where [_ :person/name ?name]]))))

(deftest canonicalized-query->return-map-keys
  (is (= [:name :age]
         (sut/canonicalized-query->return-map-keys
          '{:find [?e]
            :keys [name age]
            :where [[?e :person/name]]})))
  (is (= [:person/name :person/age]
         (sut/canonicalized-query->return-map-keys
          '{:find [?e]
            :keys [person/name person/age]
            :where [[?e :person/name]]})))
  (is (= '[name age]
         (sut/canonicalized-query->return-map-keys
          '{:find [?e]
            :syms [name age]
            :where [[?e :person/name]]})))
  (is (= ["name" "age"]
         (sut/canonicalized-query->return-map-keys
          '{:find [?e]
            :strs [name age]
            :where [[?e :person/name]]})))
  (testing "throws when two key types are supplied"
    (is
     (thrown? clojure.lang.ExceptionInfo
              (sut/canonicalized-query->return-map-keys
               '{:find [?e]
                 :keys [name]
                 :syms [age]
                 :where [[?e :person/name]]})))))

(deftest query->stripped-query+return-map-keys
  (testing "query without return map keys"
    (testing "vector form"
      (is (= ['{:find [?e] :where [[?e :person/name]]} nil]
             (sut/query->stripped-canonicalized-query+return-map-keys
              '[:find ?e
                :where [?e :person/name]]))))
    (testing "map form"
      (is (= ['{:find [?e] :where [[?e :person/name]]} nil]
             (sut/query->stripped-canonicalized-query+return-map-keys
              '{:find [?e]
                :where [[?e :person/name]]})))))
  (testing "query with return map keys"
    (testing "vector form"
      (is (= ['{:find [?e]
                :where [[?e :person/name]]}
              '(:name)]
             (sut/query->stripped-canonicalized-query+return-map-keys
              '[:find ?e
                :keys name
                :where [?e :person/name]])))
      (is (= ['{:find [?e]
                :where [[?e :person/name]]}
              '("name")]
             (sut/query->stripped-canonicalized-query+return-map-keys
              '[:find ?e
                :strs name
                :where [?e :person/name]]))))
    (testing "map form"
      (is (= ['{:find [?e]
                :where [[?e :person/name]]}
              '(name)]
             (sut/query->stripped-canonicalized-query+return-map-keys
              '{:find [?e]
                :syms [name]
                :where [[?e :person/name]]}))))))

(deftest return-maps-request
  (is (= #{{:name "Teodor"} {:name "Magnar"}}
         (set
          (sut/return-maps #{["Teodor"]
                             ["Magnar"]}
                           '[:name])))))

(comment
  (remove-ns (symbol (str *ns*)))
  (set! *print-namespace-maps* false)
  )
