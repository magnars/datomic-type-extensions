(ns datomic-type-extensions.api
  (:refer-clojure :exclude [filter sync])
  (:require [clojure.walk :refer [postwalk]]
            [datomic.api :as d]
            [datomic-type-extensions.core :as core]
            [datomic-type-extensions.entity :as entity]
            [datomic-type-extensions.query :as query]
            [datomic-type-extensions.types :as types]
            [potemkin :refer [import-vars]]))

;; store attr-types in db

(defn add-backing-types [tx]
  (postwalk
   (fn [form]
     (if-let [type (and (map? form) (:dte/valueType form))]
       (assoc form :db/valueType (types/get-backing-datomic-type type))
       form))
   tx))

(defn query-attr-types [db]
  (->> (for [attr (->> (d/q '[:find [?e ...] :where [?e :dte/valueType]] db)
                       (map #(d/entity db %)))]
         [(:db/ident attr) (select-keys attr #{:db/cardinality :dte/valueType})])
       (into {})))

(defn find-attr-types [db]
  (or (::attr-types db)
      (query-attr-types db)))

(defn init! [conn]
  (when-not (d/entity (d/db conn) :dte/valueType)
    @(d/transact conn [{:db/ident :dte/valueType
                        :db/valueType :db.type/keyword
                        :db/cardinality :db.cardinality/one}])))

(defn prepare-tx-data [db tx-data]
  (->> tx-data
       (core/serialize-tx-data (find-attr-types db))
       (add-backing-types)))

;; datomic.api

(defn transact [connection tx-data]
  (d/transact connection (prepare-tx-data (d/db connection) tx-data)))

(defn transact-async [connection tx-data]
  (d/transact-async connection (prepare-tx-data (d/db connection) tx-data)))

(defn with [db tx-data]
  (d/with db (prepare-tx-data db tx-data)))

(defn entity [db eid]
  (let [attr-types (find-attr-types db)]
    (entity/wrap (d/entity db (core/serialize-lookup-ref attr-types eid))
                 attr-types)))

(defn pull [db pattern eid]
  (let [attr-types (find-attr-types db)]
    (->> (d/pull db pattern (core/serialize-lookup-ref attr-types eid))
         (core/deserialize attr-types))))

(defn pull-many [db pattern eids]
  (let [attr-types (find-attr-types db)]
    (->> (d/pull-many db pattern (map #(core/serialize-lookup-ref attr-types %) eids))
         (core/deserialize attr-types))))

(defn since [db t]
  (let [attr-types (find-attr-types db)]
    (assoc (d/since db t) ::attr-types attr-types)))

(defn db [connection]
  (let [db (d/db connection)]
    (assoc db ::attr-types (find-attr-types db))))

(defn connect [uri]
  (let [conn (d/connect uri)]
    (init! conn)
    conn))

(defn query [query-map]
  (let [db (first (:args query-map))
        _ (when-not (instance? datomic.db.Db db)
            (throw (Exception. "The first input must be a datomic DB so that datomic-type-extensions can deserialize.")))
        attr-types (find-attr-types db)]
    (query/deserialize-by-pattern
     (d/query query-map)
     (query/deserialization-pattern (:query query-map) attr-types)
     attr-types)))

(defn q [q & inputs]
  (query {:query q :args inputs}))

(import-vars [datomic.api
              add-listener
              as-of
              as-of-t
              attribute
              basis-t
              ;; connect - implemented to init the :dte/valueType attr
              create-database
              datoms
              ;; db - implemented to cache attr-types
              delete-database
              entid
              entid-at
              ;; entity - wraps datomic.Entity to deserialize attrs when accessed
              entity-db
              filter
              function
              gc-storage
              get-database-names
              history
              ident
              index-range
              invoke
              is-filtered
              log
              next-t
              part
              ;; pull - implemented to deserialize return value
              ;; pull-many - implemented to deserialize return value
              ;; q - implemented to deserialize values
              ;; query - ditto
              release
              remove-tx-report-queue
              rename-database
              request-index
              resolve-tempid
              seek-datoms
              shutdown
              ;; since - implemented to keep attr-types on the db
              since-t
              squuid
              squuid-time-millis
              sync
              sync-excise
              sync-index
              sync-schema
              t->tx
              tempid
              touch
              ;; transact - implemented to serialize values
              ;; transact-async - ditto
              tx->t
              tx-range
              tx-report-queue
              ;; with - implemented to serialize values
              ])
