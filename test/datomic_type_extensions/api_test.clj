(ns datomic-type-extensions.api-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [datomic-type-extensions.api :as api]
            [datomic-type-extensions.core :as core]
            [datomic-type-extensions.types :as types])
  (:import java.time.Instant))

;; :java.time/instant

(defmethod types/get-backing-datomic-type :java.time/instant [_] :db.type/instant)
(defmethod types/serialize :java.time/instant [_ ^Instant instant] (java.util.Date/from instant))
(defmethod types/deserialize :java.time/instant [_ ^java.util.Date inst] (Instant/ofEpochMilli (.getTime inst)))

;; :keyword-backed-by-string

(defmethod types/get-backing-datomic-type :keyword-backed-by-string [_] :db.type/string)
(defmethod types/serialize :keyword-backed-by-string [_ kw] (name kw))
(defmethod types/deserialize :keyword-backed-by-string [_ s] (keyword s))

(def attr-types
  {:user/created-at :java.time/instant
   :user/updated-at :java.time/instant
   :client/id :keyword-backed-by-string})

(deftest serialize-tx-data
  (is (= [{:db/id 123 :user/created-at #inst "2017-01-01T00:00:00"}
          [:db/retract 123 :user/updated-at #inst "2017-02-02T00:00:00"]
          [:db/add 456 :client/id "the-client"]
          [:db/add 123 :user/name "no serialization needed"]]
         (core/serialize-tx-data
          attr-types
          [{:db/id 123 :user/created-at #time/inst "2017-01-01T00:00:00Z"}
           [:db/retract 123 :user/updated-at #time/inst "2017-02-02T00:00:00Z"]
           [:db/add 456 :client/id :the-client]
           [:db/add 123 :user/name "no serialization needed"]])))

  (testing "nested maps"
    (is (= [{:client/users [{:user/created-at #inst "2017-01-01T00:00:00.000-00:00"}
                            {:user/created-at #inst "2018-01-01T00:00:00.000-00:00"}]
             :client/admin {:user/created-at #inst "2016-01-01T00:00:00"}}]
           (core/serialize-tx-data
            attr-types
            [{:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}
                             {:user/created-at #time/inst "2018-01-01T00:00:00Z"}]
              :client/admin {:user/created-at #time/inst "2016-01-01T00:00:00Z"}}]))))

  (testing "nested tx-data"
    (is (= {:conformity {:txs [[[:db/add 456 :client/id "the-client"]]]}}
           (core/serialize-tx-data
            attr-types
            {:conformity {:txs [[[:db/add 456 :client/id :the-client]]]}})))))

(deftest serialize-lookup-ref
  (is (= 123 (core/serialize-lookup-ref attr-types 123)))
  (is (= [:client/id "the-client"]
         (core/serialize-lookup-ref attr-types [:client/id :the-client]))))

(deftest add-backing-types
  (is (= [{:db/ident :user/created-at
           :db/valueType :db.type/instant
           :dte/valueType :java.time/instant
           :db/cardinality :db.cardinality/one}
          {:db/ident :client/id
           :db/unique :db.unique/identity
           :db/valueType :db.type/string
           :dte/valueType :keyword-backed-by-string
           :db/cardinality :db.cardinality/one}]
         (api/add-backing-types
          [{:db/ident :user/created-at
            :dte/valueType :java.time/instant
            :db/cardinality :db.cardinality/one}
           {:db/ident :client/id
            :db/unique :db.unique/identity
            :dte/valueType :keyword-backed-by-string
            :db/cardinality :db.cardinality/one}]))))

(defn create-conn []
  (let [url (str "datomic:mem://" (d/squuid))]
    (api/create-database url)
    (api/connect url)))

(def migrations
  [{:db/ident :user/email
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/created-at
    :dte/valueType :java.time/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/updated-at
    :dte/valueType :java.time/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :client/id
    :db/unique :db.unique/identity
    :dte/valueType :keyword-backed-by-string
    :db/cardinality :db.cardinality/one}
   {:db/ident :client/users
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident :client/admin
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}])

(defn create-migrated-conn []
  (let [conn (create-conn)]
    (->> migrations
         (api/transact conn)
         deref)
    conn))

(deftest find-attr-types
  (is (= {:user/created-at :java.time/instant
          :user/updated-at :java.time/instant
          :client/id :keyword-backed-by-string}
         (api/find-attr-types (d/db (create-migrated-conn))))))

(deftest transact-async
  (is (= {:user/created-at #inst "2017-01-01T00:00:00"}
         (let [conn (create-migrated-conn)]
           (api/transact-async conn
                               [{:user/email "foo@example.com"
                                 :user/created-at #time/inst "2017-01-01T00:00:00Z"}])
           (d/sync conn)
           (d/pull (d/db conn)
                   [:user/created-at]
                   [:user/email "foo@example.com"])))))

(defn create-populated-conn []
  (let [conn (create-migrated-conn)]
    @(api/transact
      conn
      [{:client/id :the-client
        :client/users [{:user/email "foo@example.com"
                        :user/created-at #time/inst "2017-01-01T00:00:00Z"}]}])
    conn))

(deftest entity
  (let [wrapped-entity (api/entity (d/db (create-populated-conn))
                                   [:user/email "foo@example.com"])]
    (testing "deserializes registered attribute"
      (is (= #time/inst "2017-01-01T00:00:00Z" (:user/created-at wrapped-entity))))

    (testing "leaves unregistered attributes alone"
      (is (= "foo@example.com" (:user/email wrapped-entity))))

    (testing "implements Associative"
      (is (= {:user/created-at #time/inst "2017-01-01T00:00:00Z"
              :user/email "foo@example.com"}
             (select-keys wrapped-entity #{:user/created-at :user/email :client/id :foo/bar}))))

    (testing "implements ILookup"
      (is (= (.valAt wrapped-entity :user/email) "foo@example.com"))
      (is (= (.valAt wrapped-entity :user/email :not-found) "foo@example.com"))
      (is (nil? (.valAt wrapped-entity :user/missing-attr)))
      (is (= (.valAt wrapped-entity :user/missing-attr :not-found) :not-found))))

  (testing "can use entity lookup ref"
    (is (not (nil? (api/entity (d/db (create-populated-conn))
                               [:client/id :the-client])))))

  (let [db (d/db (create-populated-conn))
        datomic-entity (d/entity db [:client/id "the-client"])
        wrapped-entity (api/entity db [:client/id :the-client])]

    (testing "equality semantics"
      (is (not= datomic-entity wrapped-entity))
      (is (not= wrapped-entity datomic-entity))
      (is (= wrapped-entity (api/entity db [:client/id :the-client]))))

    (testing "deserializes nested entity attributes"
      (is (= #time/inst "2017-01-01T00:00:00Z"
             (-> wrapped-entity :client/users first :user/created-at))))

    (testing "printing"
      (testing "defaults to only show :db/id"
        (is (= (let [client-db-id (:db/id datomic-entity)]
                 {:db/id client-db-id})
               (edn/read-string (pr-str wrapped-entity)))))

      (testing "shows all attributes when entity has been touched"
        (is (= (let [client-db-id (:db/id datomic-entity)
                     user-db-id (:db/id (first (:client/users datomic-entity)))]
                 {:client/id :the-client
                  :client/users #{{:db/id user-db-id}}
                  :db/id client-db-id})
               (edn/read-string {:readers *data-readers*}
                                (pr-str (d/touch wrapped-entity))))))

      (testing "shows deserialized value of type extended attributes"
        (is (= {:db/id (:db/id (first (:client/users datomic-entity)))
                :user/created-at #time/inst "2017-01-01T00:00:00Z"
                :user/email "foo@example.com"}
               (edn/read-string {:readers *data-readers*}
                                (pr-str (d/touch (first (:client/users wrapped-entity)))))))))))

(deftest pull
  (is (= {:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}]}
         (api/pull (d/db (create-populated-conn))
                   [{:client/users [:user/created-at]}]
                   [:client/id :the-client])))

  (is (= [{:client/id :the-client
           :client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}]}]
         (api/pull-many (d/db (create-populated-conn))
                        [:client/id {:client/users [:user/created-at]}]
                        [[:client/id :the-client]]))))

(deftest since
  (let [conn (create-migrated-conn)
        t (d/basis-t (d/db conn))
        _ (api/transact conn [{:user/email "bar@example.com"
                               :user/created-at #time/inst "2018-01-01T00:00:00Z"}])
        db-since (api/since (d/db conn) t)]
    (is (= #time/inst "2018-01-01T00:00:00Z"
           (:user/created-at (api/entity db-since [:user/email "bar@example.com"]))))
    (is (= db-since
           (api/entity-db (api/entity db-since [:user/email "bar@example.com"]))))))

(deftest with
  (is (= {:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}
                         {:user/created-at #time/inst "2017-02-01T00:00:00Z"}]}
         (api/pull (:db-after (api/with (d/db (create-populated-conn))
                                        [{:client/id :the-client
                                          :client/users [{:user/email "bar@example.com"
                                                          :user/created-at #time/inst "2017-02-01T00:00:00Z"}]}]))
                   [{:client/users [:user/created-at]}]
                   [:client/id :the-client]))))

(deftest q
  (is (= #{[#time/inst "2017-01-01T00:00:00Z"]}
         (api/q
          '[:find ?inst :where [_ :user/created-at ?inst]]
          (d/db (create-populated-conn)))))

  (is (= #{[:the-client {:user/created-at #time/inst "2017-01-01T00:00:00.000Z"}]}
         (api/q '[:find ?c-id (pull ?e [:user/created-at])
                  :where
                  [?c :client/id ?c-id]
                  [?c :client/users ?e]]
                (d/db (create-populated-conn)))))

  (is (= #{[#time/inst "2017-01-01T00:00:00Z"]}
         (api/query
          {:query '[:find ?inst :where [_ :user/created-at ?inst]]
           :args [(d/db (create-populated-conn))]})))

  (is (thrown-with-msg? Exception #"The first input must be a datomic DB so that datomic-type-extensions can deserialize."
                        (api/q '[:find ?inst :in ?e $ :where [?e :user/created-at ?inst]]
                               [:user/email "foo@example.com"] (d/db (create-populated-conn))))))

(comment
  (def conn (create-populated-conn))
  (def db (d/db conn))

  (d/pull (d/db conn)
          [:user/created-at]
          [:user/email "foo@example.com"])

  (d/q '[:find (pull ?e [:user/created-at]) :where [?e :user/email]] db)
  (api/q '[:find (pull ?c [:client/id]) (pull ?e [:user/created-at])
           :where
           [?c :client/id]
           [?c :client/users ?e]] db)

  (d/q '[:find ?c-id (pull ?e [:user/created-at])
         :where
         [?c :client/id ?c-id]
         [?c :client/users ?e]] db)

  (d/q '[:find [(pull ?e [:user/created-at]) ...] :where [?e :user/email]] db)

  (d/q '[:find (pull ?e [:user/created-at]) . :where [?e :user/email]] db)

  (d/q '[:find [?v ...] :where [?e :client/id ?v]] db)

  (let [[[e a v]]
        (seq (d/datoms (d/db conn) :eavt [:user/email "foo@example.com"] :user/created-at))]
    v)

  )
