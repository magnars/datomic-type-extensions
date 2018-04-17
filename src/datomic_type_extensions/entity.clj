(ns datomic-type-extensions.entity
  (:require [datomic-type-extensions.types :as types])
  (:import datomic.query.EntityMap))

(defmacro either
  "Like clojure.core/or, but treats false as a truthy value"
  ([] nil)
  ([x] x)
  ([x & next]
   `(if-some [false-or-truthy# ~x]
      false-or-truthy#
      (either ~@next))))

(declare wrap)

(defn deserialize-attr [entity attr-types attr]
  (when-let [type (get attr-types attr)]
    (types/deserialize type (attr entity))))

(deftype TypeExtendedEntityMap [^EntityMap entity attr-types]
  Object
  (hashCode [_]           (.hashCode entity))
  (equals [_ o]           (and (instance? TypeExtendedEntityMap o)
                               (.equiv entity (.entity o))))

  clojure.lang.Seqable
  (seq [_]                 (map (fn [[k v]]
                                  [k (either (deserialize-attr entity attr-types k)
                                             (wrap (.valAt entity k) attr-types))])
                                (.seq entity)))

  clojure.lang.Associative
  (equiv [_ o]            (and (instance? TypeExtendedEntityMap o)
                               (.equiv entity (.entity o))))
  (containsKey [_ k]      (.containsKey entity k))
  (entryAt [_ k]          (let [v (either (deserialize-attr entity attr-types k)
                                          (some-> entity (.entryAt k) .val (wrap attr-types)))]
                            (when (some? v) (first {k v}))))
  (empty [_]              (.empty entity))
  (assoc [_ k v]          (.assoc entity k v))
  (cons  [_ x]            (.cons entity x))
  (count [_]              (.count entity))

  clojure.lang.ILookup
  (valAt [_ k]            (either (deserialize-attr entity attr-types k)
                                  (wrap (.valAt entity k) attr-types)))
  (valAt [_ k not-found]  (either (deserialize-attr entity attr-types k)
                                  (wrap (.valAt entity k not-found) attr-types)))

  datomic.Entity
  (db [_]                 (.db entity))
  (get [_ k]              (wrap (.get entity k)))
  (keySet [_]             (.keySet entity))
  (touch [this]           (do (.touch entity) this)))

(defmethod print-method TypeExtendedEntityMap [entity writer]
  (print-method (into {:db/id (:db/id entity)} entity) writer))

(defn wrap
  [x attr-types]
  (cond
    (instance? datomic.Entity x)
    (TypeExtendedEntityMap. x attr-types)

    (coll? x)
    (set (map #(wrap % attr-types) x))

    :else x))
