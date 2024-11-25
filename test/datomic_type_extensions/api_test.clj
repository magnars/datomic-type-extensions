(ns datomic-type-extensions.api-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [are deftest is testing]]
            [datomic-type-extensions.api :as api]
            [datomic-type-extensions.core :as core]
            [datomic-type-extensions.types :as types]
            [datomic.api :as d])
  (:import java.time.Instant))

;; :java.time/instant

(defmethod types/get-backing-datomic-type :java.time/instant [_] :db.type/instant)
(defmethod types/serialize :java.time/instant [_ ^Instant instant] (java.util.Date/from instant))
(defmethod types/deserialize :java.time/instant [_ ^java.util.Date inst] (Instant/ofEpochMilli (.getTime inst)))

;; :keyword-backed-by-string

(defmethod types/get-backing-datomic-type :keyword-backed-by-string [_] :db.type/string)
(defmethod types/serialize :keyword-backed-by-string [_ kw] (name kw))
(defmethod types/deserialize :keyword-backed-by-string [_ s] (keyword s))

;; :edn-backed-by-string

(defmethod types/get-backing-datomic-type :edn-backed-by-string [_] :db.type/string)
(defmethod types/serialize :edn-backed-by-string [_ x] (pr-str x))
(defmethod types/deserialize :edn-backed-by-string [_ x]
  (clojure.edn/read-string
   {:readers {'time/inst java-time-literals.core/parse-instant}}
   x))

(defn attr-info [value-type & [cardinality]]
  {:dte/valueType value-type
   :db/cardinality (or cardinality :db.cardinality/one)})

(def attr->attr-info
  {:user/created-at (attr-info :java.time/instant)
   :user/updated-at (attr-info :java.time/instant)
   :user/demands (attr-info :keyword-backed-by-string :db.cardinality/many)
   :user/edn (attr-info :edn-backed-by-string)
   :client/id (attr-info :keyword-backed-by-string)})

(deftest apply-to-value
  (are [cardinality value result] (= result
                                     (core/apply-to-value
                                      str
                                      {:db/cardinality cardinality}
                                      value))
    :db.cardinality/one 0 "0"
    :db.cardinality/one [0 1 2] "[0 1 2]"
    :db.cardinality/many [0 1 2] ["0" "1" "2"]
    :db.cardinality/many '(0 1 2) '("0" "1" "2")
    :db.cardinality/many #{0 1 2} #{"0" "1" "2"})

  (is (thrown-with-msg?
       Exception #"Value must be either set, list or vector"
       (core/apply-to-value str
                            {:db/cardinality :db.cardinality/many}
                            1))))

(deftest serialize-tx-data
  (is (= [{:db/id 123 :user/created-at #inst "2017-01-01T00:00:00"}
          [:db/retract 123 :user/updated-at #inst "2017-02-02T00:00:00"]
          [:db/add 456 :client/id "the-client"]
          [:db/add 123 :user/name "no serialization needed"]
          [:db/add 123 :user/demands ["peace" "love" "happiness"]]
          [:db/add 123 :user/edn "[1 2 3]"]]
         (core/serialize-tx-data
          attr->attr-info
          [{:db/id 123 :user/created-at #time/inst "2017-01-01T00:00:00Z"}
           [:db/retract 123 :user/updated-at #time/inst "2017-02-02T00:00:00Z"]
           [:db/add 456 :client/id :the-client]
           [:db/add 123 :user/name "no serialization needed"]
           [:db/add 123 :user/demands [:peace :love :happiness]]
           [:db/add 123 :user/edn [1 2 3]]])))

  (testing "serialize and deserialize symmetry for nested dte-backed types"
    (let [data {:user/edn {:user/created-at #time/inst "2017-01-01T00:00:00Z"}}]
      (is (= (core/deserialize attr->attr-info (core/serialize-tx-data attr->attr-info data))
             data))))

  (testing "nested maps"
    (is (= [{:client/users [{:user/created-at #inst "2017-01-01T00:00:00.000-00:00"}
                            {:user/created-at #inst "2018-01-01T00:00:00.000-00:00"}]
             :client/admin {:user/created-at #inst "2016-01-01T00:00:00"}}]
           (core/serialize-tx-data
            attr->attr-info
            [{:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}
                             {:user/created-at #time/inst "2018-01-01T00:00:00Z"}]
              :client/admin {:user/created-at #time/inst "2016-01-01T00:00:00Z"}}]))))

  (testing "multiple values"
    (is (= [{:user/demands ["peace" "love" "happiness"]}]
           (core/serialize-tx-data attr->attr-info [{:user/demands [:peace :love :happiness]}]))))

  (testing "edn value"
    (is (= [{:user/demands ["peace" "love" "happiness"]}]
           (core/serialize-tx-data attr->attr-info [{:user/demands [:peace :love :happiness]}]))))

  (testing "nested tx-data"
    (is (= {:conformity {:txs [[[:db/add 456 :client/id "the-client"]]]}}
           (core/serialize-tx-data
            attr->attr-info
            {:conformity {:txs [[[:db/add 456 :client/id :the-client]]]}})))))

(deftest serialize-lookup-ref
  (is (= 123 (core/serialize-lookup-ref attr->attr-info 123)))
  (is (= [:client/id "the-client"]
         (core/serialize-lookup-ref attr->attr-info [:client/id :the-client]))))

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
   {:db/ident :user/demands
    :dte/valueType :keyword-backed-by-string
    :db/cardinality :db.cardinality/many}
   {:db/ident :user/leaves-empty
    :dte/valueType :keyword-backed-by-string
    :db/cardinality :db.cardinality/many}
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

(deftest find-attr->attr-info
  (is (= {:user/created-at (attr-info :java.time/instant)
          :user/updated-at (attr-info :java.time/instant)
          :user/demands (attr-info :keyword-backed-by-string :db.cardinality/many)
          :user/leaves-empty (attr-info :keyword-backed-by-string :db.cardinality/many)
          :client/id (attr-info :keyword-backed-by-string)}
         (api/find-attr->attr-info (d/db (create-migrated-conn))))))

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
                        :user/created-at #time/inst "2017-01-01T00:00:00Z"
                        :user/demands [:peace :love :happiness]}]}])
    conn))

(deftest entity
  (let [wrapped-entity (api/entity (d/db (create-populated-conn))
                                   [:user/email "foo@example.com"])]
    (testing "deserializes registered attributes"
      (is (= #time/inst "2017-01-01T00:00:00Z" (:user/created-at wrapped-entity)))
      (is (= #{:peace :love :happiness} (set (:user/demands wrapped-entity)))))

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
      (is (= (.valAt wrapped-entity :user/missing-attr :not-found) :not-found))
      (is (nil? (.valAt wrapped-entity :user/leaves-empty))))

    (testing "works with (keys ,,,)"
      (is (= (set (keys wrapped-entity))
             #{:user/email :user/demands :user/created-at})))

    (testing "keeps type when emptied"
      (is (= wrapped-entity (empty wrapped-entity)))))

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
                :user/email "foo@example.com"
                :user/demands #{:peace :love :happiness}}
               (edn/read-string {:readers *data-readers*}
                                (pr-str (d/touch (first (:client/users wrapped-entity)))))))))

    (testing "hashes differently"
      (is (not= (hash datomic-entity)
                (hash wrapped-entity))))))

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

(deftest filter
  (is (= {:client/id :the-client
          :client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}]}
         (let [db (d/db (create-populated-conn))
               the-client (api/entity db [:client/id :the-client])
               keep-eids (into #{(:db/id the-client)}
                               (map :db/id (:client/users the-client)))]
           (api/pull (api/filter db (fn [_ datom] (some #{(:e datom)} keep-eids)))
                     [:client/id {:client/users [:user/created-at]}]
                     (:db/id the-client))))))

(deftest history
  (let [conn (create-populated-conn)
        db (d/db conn)
        the-user (api/entity db [:user/email "foo@example.com"])
        changed-db (:db-after @(api/transact conn [{:user/email "foo@example.com"
                                                    :user/created-at #time/inst "2018-01-01T00:00:00Z"}]))]
    (is (= 3
           (count
            (api/q '[:find ?created-at ?tx ?op
                     :in $ ?user
                     :where
                     [?user :user/created-at ?created-at ?tx ?op]]
                   (api/history changed-db)
                   (:db/id the-user)))))))

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
                               [:user/email "foo@example.com"] (d/db (create-populated-conn)))))

  (testing "Return Maps"
    ;; Return maps is a datomic feature that allows a query to return a sequence of maps.
    ;;
    ;; Datomic docs for return maps: https://docs.datomic.com/query/query-data-reference.html#return-maps
    (is (= '({:created-at #time/inst "2017-01-01T00:00:00.000-00:00"
              :email "foo@example.com"})
           (api/q '[:find ?created-at ?email
                    :keys created-at email
                    :where
                    [?e :user/email ?email]
                    [?e :user/created-at ?created-at]]
                  (d/db (create-populated-conn)))))))

(comment
  (set! *print-namespace-maps* false)

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

  ;; Return maps behavior in Datomic and datomic-type-extensions
  (api/q '[:find ?email ?created-at
           :where
           [?e :user/email ?email]
           [?e :user/created-at ?created-at]]
         db)
  ;; => #{["foo@example.com" #time/inst "2017-01-01T00:00:00Z"]}

  (api/q '{:find [?email ?created-at]
           :keys [email created-at]
           :where
           [[?e :user/email ?email]
            [?e :user/created-at ?created-at]]}
         db)
  ;; => [{:email "foo@example.com", :created-at #time/inst "2017-01-01T00:00:00Z"}]

  (d/q '{:find [?email ?created-at]
         :keys [email created-at]
         :where
         [[?e :user/email ?email]
          [?e :user/created-at ?created-at]]}
       db)
  ;; => [{:email "foo@example.com", :created-at #inst "2017-01-01T00:00:00.000-00:00"}]

  )
