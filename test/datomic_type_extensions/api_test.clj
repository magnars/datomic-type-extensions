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
  (is (= (core/serialize-tx-data
          attr->attr-info
          [{:db/id 123 :user/created-at #time/inst "2017-01-01T00:00:00Z"}
           [:db/retract 123 :user/updated-at #time/inst "2017-02-02T00:00:00Z"]
           [:db/retract 123 :user/updated-at]
           [:db/add 456 :client/id :the-client]
           [:db/add 123 :user/name "no serialization needed"]
           [:db/add 123 :user/demands [:peace :love :happiness]]
           [:db/add 123 :user/edn [1 2 3]]])
         [{:db/id 123 :user/created-at #inst "2017-01-01T00:00:00"}
          [:db/retract 123 :user/updated-at #inst "2017-02-02T00:00:00"]
          [:db/retract 123 :user/updated-at]
          [:db/add 456 :client/id "the-client"]
          [:db/add 123 :user/name "no serialization needed"]
          [:db/add 123 :user/demands ["peace" "love" "happiness"]]
          [:db/add 123 :user/edn "[1 2 3]"]]))

  (testing "serialize and deserialize symmetry for nested dte-backed types"
    (let [data {:user/edn {:user/created-at #time/inst "2017-01-01T00:00:00Z"}}]
      (is (= (core/deserialize attr->attr-info (core/serialize-tx-data attr->attr-info data))
             data))))

  (testing "nested maps"
    (is (= (core/serialize-tx-data
            attr->attr-info
            [{:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}
                             {:user/created-at #time/inst "2018-01-01T00:00:00Z"}]
              :client/admin {:user/created-at #time/inst "2016-01-01T00:00:00Z"}}])
           [{:client/users [{:user/created-at #inst "2017-01-01T00:00:00.000-00:00"}
                            {:user/created-at #inst "2018-01-01T00:00:00.000-00:00"}]
             :client/admin {:user/created-at #inst "2016-01-01T00:00:00"}}])))

  (testing "multiple values"
    (is (= (core/serialize-tx-data attr->attr-info [{:user/demands [:peace :love :happiness]}])
           [{:user/demands ["peace" "love" "happiness"]}])))

  (testing "edn value"
    (is (= (core/serialize-tx-data attr->attr-info [{:user/demands [:peace :love :happiness]}])
           [{:user/demands ["peace" "love" "happiness"]}])))

  (testing "nested tx-data"
    (is (= (core/serialize-tx-data
            attr->attr-info
            {:conformity {:txs [[[:db/add 456 :client/id :the-client]]]}})
           {:conformity {:txs [[[:db/add 456 :client/id "the-client"]]]}}))))

(deftest serialize-lookup-ref
  (is (= (core/serialize-lookup-ref attr->attr-info 123)
         123))
  (is (= (core/serialize-lookup-ref attr->attr-info [:client/id :the-client])
         [:client/id "the-client"])))

(deftest add-backing-types
  (is (= (api/add-backing-types
          [{:db/ident :user/created-at
            :dte/valueType :java.time/instant
            :db/cardinality :db.cardinality/one}
           {:db/ident :client/id
            :db/unique :db.unique/identity
            :dte/valueType :keyword-backed-by-string
            :db/cardinality :db.cardinality/one}])
         [{:db/ident :user/created-at
           :db/valueType :db.type/instant
           :dte/valueType :java.time/instant
           :db/cardinality :db.cardinality/one}
          {:db/ident :client/id
           :db/unique :db.unique/identity
           :db/valueType :db.type/string
           :dte/valueType :keyword-backed-by-string
           :db/cardinality :db.cardinality/one}])))

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
   {:db/ident :user/favorite-foods
    :db/valueType :db.type/tuple
    :db/tupleType :db.type/keyword
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
    :db/cardinality :db.cardinality/one}
   {:db/ident       :person/id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :person/children
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}
   {:db/ident       :person/friends
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}])

(defn create-migrated-conn []
  (let [conn (create-conn)]
    (->> migrations
         (api/transact conn)
         deref)
    conn))

(deftest find-attr->attr-info
  (is (= (api/find-attr->attr-info (d/db (create-migrated-conn)))
         {:user/created-at (attr-info :java.time/instant)
          :user/updated-at (attr-info :java.time/instant)
          :user/demands (attr-info :keyword-backed-by-string :db.cardinality/many)
          :user/leaves-empty (attr-info :keyword-backed-by-string :db.cardinality/many)
          :client/id (attr-info :keyword-backed-by-string)})))

(deftest transact-async
  (is (= (let [conn (create-migrated-conn)]
           (api/transact-async conn
                               [{:user/email "foo@example.com"
                                 :user/created-at #time/inst "2017-01-01T00:00:00Z"}])
           (d/sync conn)
           (d/pull (d/db conn)
                   [:user/created-at]
                   [:user/email "foo@example.com"]))
         {:user/created-at #inst "2017-01-01T00:00:00"})))

(defn create-populated-conn []
  (let [conn (create-migrated-conn)]
    @(api/transact
      conn
      [{:client/id :the-client
        :client/users [{:user/email "foo@example.com"
                        :user/created-at #time/inst "2017-01-01T00:00:00Z"
                        :user/demands [:peace :love :happiness]
                        :user/favorite-foods [:pizza :lasagna :haggis]}]}
       {:person/id "parent1"
        :person/children [{:person/id "child1"}
                          {:person/id "child2"}
                          {:person/id "child3"}]
        :person/friends [{:person/id "sibling1"}]}])
    conn))

(def populated-db (d/db (create-populated-conn)))

(deftest entity
  (let [wrapped-entity (api/entity populated-db
                                   [:user/email "foo@example.com"])]
    (testing "deserializes registered attributes"
      (is (= (:user/created-at wrapped-entity)
             (.get wrapped-entity :user/created-at)
             #time/inst "2017-01-01T00:00:00Z"))
      (is (= (:user/demands wrapped-entity)
             #{:peace :love :happiness})))

    (testing "leaves unregistered attributes alone"
      (is (= (:user/email wrapped-entity)
             "foo@example.com")))

    (testing "implements Associative"
      (is (= (select-keys wrapped-entity #{:user/created-at :user/email :client/id :foo/bar})
             {:user/created-at #time/inst "2017-01-01T00:00:00Z"
              :user/email "foo@example.com"})))

    (testing "implements ILookup"
      (is (= (.valAt wrapped-entity :user/email) "foo@example.com"))
      (is (= (.valAt wrapped-entity :user/email :not-found) "foo@example.com"))
      (is (nil? (.valAt wrapped-entity :user/missing-attr)))
      (is (= (.valAt wrapped-entity :user/missing-attr :not-found) :not-found))
      (is (nil? (.valAt wrapped-entity :user/leaves-empty))))

    (testing "works with (keys ,,,)"
      (is (= (set (keys wrapped-entity))
             #{:user/email :user/demands :user/created-at :user/favorite-foods})))

    (testing "keeps type when emptied"
      (is (= wrapped-entity (empty wrapped-entity))))

    (testing "preserves coll types"
      (testing ":db.type/ref with :db.cardinality/many is a set"
        (is (set? (:user/demands wrapped-entity))))
      (testing ":db.type/tuple is a vector"
        (is (vector? (:user/favorite-foods wrapped-entity))))))

  (testing "can use entity lookup ref"
    (is (not (nil? (api/entity populated-db
                               [:client/id :the-client])))))

  (let [datomic-entity (d/entity populated-db [:client/id "the-client"])
        wrapped-entity (api/entity populated-db [:client/id :the-client])]

    (testing "equality semantics"
      (is (not= datomic-entity wrapped-entity))
      (is (not= wrapped-entity datomic-entity))
      (is (= (api/entity populated-db [:client/id :the-client])
             wrapped-entity)))

    (testing "deserializes nested entity attributes"
      (is (= (-> wrapped-entity :client/users first :user/created-at)
             #time/inst "2017-01-01T00:00:00Z")))

    (testing "printing"
      (testing "defaults to only show :db/id"
        (let [client-db-id (:db/id datomic-entity)
              untouched-entity (api/entity populated-db [:client/id :the-client])]
          (is (= (edn/read-string (pr-str untouched-entity))
                 {:db/id client-db-id}))))

      (testing "shows all attributes when entity has been touched"
        (is (= (set (keys (edn/read-string {:readers *data-readers*}
                                           (pr-str (d/touch wrapped-entity)))))
               #{:db/id :client/users :client/id})))

      (testing "shows the same values as datomic entity when touched"
        (let [datomic-entity (datomic.api/entity (d/db (create-populated-conn)) [:person/id "parent1"])
              wrapped-entity (api/entity (d/db (create-populated-conn)) [:person/id "parent1"])]
          (is (= (edn/read-string (pr-str (d/touch datomic-entity)))
                 (edn/read-string (pr-str (d/touch wrapped-entity)))))))

      (testing "shows deserialized value of type extended attributes"
        (is (= (edn/read-string {:readers *data-readers*}
                                (pr-str (d/touch (first (:client/users wrapped-entity)))))
               {:db/id (:db/id (first (:client/users datomic-entity)))
                :user/created-at #time/inst "2017-01-01T00:00:00Z"
                :user/email "foo@example.com"
                :user/demands #{:peace :love :happiness}
                :user/favorite-foods [:pizza :lasagna :haggis]}))))

    (testing "hashes differently"
      (is (not= (hash datomic-entity)
                (hash wrapped-entity))))))

(deftest pull
  (is (= (api/pull populated-db
                   [{:client/users [:user/created-at]}]
                   [:client/id :the-client])
         {:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}]}))

  (is (= (api/pull-many populated-db
                        [:client/id {:client/users [:user/created-at]}]
                        [[:client/id :the-client]])
         [{:client/id :the-client
           :client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}]}])))

(deftest since
  (let [conn (create-migrated-conn)
        t (d/basis-t (d/db conn))
        _ (api/transact conn [{:user/email "bar@example.com"
                               :user/created-at #time/inst "2018-01-01T00:00:00Z"}])
        db-since (api/since (d/db conn) t)]
    (is (= (:user/created-at (api/entity db-since [:user/email "bar@example.com"]))
           #time/inst "2018-01-01T00:00:00Z"))
    (is (= (api/entity-db (api/entity db-since [:user/email "bar@example.com"]))
           db-since))))

(deftest with
  (is (= (api/pull (:db-after (api/with populated-db
                                        [{:client/id :the-client
                                          :client/users [{:user/email "bar@example.com"
                                                          :user/created-at #time/inst "2017-02-01T00:00:00Z"}]}]))
                   [{:client/users [:user/created-at]}]
                   [:client/id :the-client])
         {:client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}
                         {:user/created-at #time/inst "2017-02-01T00:00:00Z"}]})))

(deftest filter
  (is (= (let [the-client (api/entity populated-db [:client/id :the-client])
               keep-eids (into #{(:db/id the-client)}
                               (map :db/id (:client/users the-client)))]
           (api/pull (api/filter populated-db (fn [_ datom] (some #{(:e datom)} keep-eids)))
                     [:client/id {:client/users [:user/created-at]}]
                     (:db/id the-client)))
         {:client/id :the-client
          :client/users [{:user/created-at #time/inst "2017-01-01T00:00:00Z"}]})))

(deftest history
  (let [conn (create-populated-conn)
        db (d/db conn)
        the-user (api/entity db [:user/email "foo@example.com"])
        changed-db (:db-after @(api/transact conn [{:user/email "foo@example.com"
                                                    :user/created-at #time/inst "2018-01-01T00:00:00Z"}]))]
    (is (= (count
            (api/q '[:find ?created-at ?tx ?op
                     :in $ ?user
                     :where
                     [?user :user/created-at ?created-at ?tx ?op]]
                   (api/history changed-db)
                   (:db/id the-user)))
           3))))

(deftest q
  (is (= (api/q
          '[:find ?inst :where [_ :user/created-at ?inst]]
          populated-db)
         #{[#time/inst "2017-01-01T00:00:00Z"]}))

  (is (= (api/q '[:find ?c-id (pull ?e [:user/created-at])
                  :where
                  [?c :client/id ?c-id]
                  [?c :client/users ?e]]
                populated-db)
         #{[:the-client {:user/created-at #time/inst "2017-01-01T00:00:00.000Z"}]}))

  (is (= (api/query
          {:query '[:find ?inst :where [_ :user/created-at ?inst]]
           :args [populated-db]})
         #{[#time/inst "2017-01-01T00:00:00Z"]}))

  (is (thrown-with-msg? Exception #"The first input must be a datomic DB so that datomic-type-extensions can deserialize."
                        (api/q '[:find ?inst :in ?e $ :where [?e :user/created-at ?inst]]
                               [:user/email "foo@example.com"] populated-db))))

(defmacro extract-exception "Return thrown exception or nil" [form]
  `(try (do ~form nil)
        (catch Exception e# e#)))
#_(extract-exception (+ 1 "lol"))

(deftest q-return-maps
  (testing "legitimate query"
    (is (= (set
            (api/q '[:find ?email ?demand
                     :keys user/email user/demands
                     :where
                     [?user :user/email ?email]
                     [?user :user/demands ?demand]]
                   populated-db))
           #{{:user/email "foo@example.com", :user/demands :peace}
             {:user/email "foo@example.com", :user/demands :happiness}
             {:user/email "foo@example.com", :user/demands :love}})))

  (testing "find-coll or find-tuple"
    (is (= (ex-data
            (extract-exception
             (api/q '[:find [?email ...]
                      :keys user/email
                      :where [_ :user/email ?email]]
                    populated-db)))
           {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
            :cognitect.anomalies/message
            "Cannot use find-coll or find-tuple find specs with return maps"})))

  (testing "find-scalar"
    (is (= (ex-data
            (extract-exception
             #_{:clj-kondo/ignore [:datalog-syntax]}
             (api/q '[:find ?email .
                      :keys user/email
                      :where [_ :user/email ?email]]
                    populated-db)))
           {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
            :cognitect.anomalies/message
            "Cannot use find-scalar find specs with return maps"})))

  (testing "Count of :keys/:strs/:syms must match count of :find"
    (is (= (ex-data
            (extract-exception
             #_{:clj-kondo/ignore [:datalog-syntax]}
             (api/q '[:find ?email ?demands
                      :keys user/email
                      :where
                      [?u :user/email ?email]
                      [?u :user/demands ?demands]]
                    populated-db)))
           {:cognitect.anomalies/category :cognitect.anomalies/incorrect,
            :cognitect.anomalies/message
            "Count of :keys/:strs/:syms must match count of :find"})))

  (testing "Empty result set prior to conversion to return maps"
    (is (= (api/q '[:find ?demand
                    :keys user/demand
                    :where
                    [?u :user/email "Glorfindel@lothlorien.net"]
                    [?u :user/demands ?demand]]
                  populated-db)
           []))))

(deftest stats
  (testing "Query stats returns map with result set in :ret"
    (let [result (api/query
                  {:query '[:find ?inst :where [_ :user/created-at ?inst]]
                   :args [populated-db]
                   :query-stats true})]
      (is (= (:ret result)
             #{[#time/inst "2017-01-01T00:00:00Z"]}))
      (is (some? (:query-stats result)))))
  (testing "IO stats returns map with result set in :ret"
    (let [result (api/query
                  {:query '[:find ?inst :where [_ :user/created-at ?inst]]
                   :args [populated-db]
                   :io-context :user/created-at})]
      (is (= (:ret result)
             #{[#time/inst "2017-01-01T00:00:00Z"]}))
      (is (some? (:io-stats result))))))

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
