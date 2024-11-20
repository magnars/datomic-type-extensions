(ns datomic-type-extensions.entity
  (:require [datomic-type-extensions.core :as core]
            [datomic-type-extensions.types :as types])
  (:import datomic.query.EntityMap))

(defmacro either
  "Like clojure.core/or, but treats false as a truthy value"
  ([] nil)
  ([x] x)
  ([x & next]
   `(if-some [false-or-truthy# ~x]
      false-or-truthy#
      (either ~@next))))

(declare wrap equiv-entity)

(defn deserialize-attr [entity attr->attr-info attr]
  (when-let [val (attr entity)]
    (when-let [attr-info (get attr->attr-info attr)]
      (core/apply-to-value (partial types/deserialize (:dte/valueType attr-info))
                           attr-info
                           val))))

(deftype TypeExtendedEntityMap [^EntityMap entity attr->attr-info touched?]
  Object
  (hashCode [_]           (hash [(.hashCode entity) attr->attr-info]))
  (equals [this o]        (and (instance? TypeExtendedEntityMap o)
                               (equiv-entity this o)))

  clojure.lang.Seqable
  (seq [_]                (map (fn [[k v]]
                                 (clojure.lang.MapEntry.
                                  k
                                  (either (deserialize-attr entity attr->attr-info k)
                                          (wrap (.valAt entity k) attr->attr-info))))
                               (.seq entity)))

  clojure.lang.Associative
  (equiv [this o]         (and (instance? TypeExtendedEntityMap o)
                               (equiv-entity this o)))
  (containsKey [_ k]      (.containsKey entity k))
  (entryAt [_ k]          (let [v (either (deserialize-attr entity attr->attr-info k)
                                          (some-> entity (.entryAt k) .val (wrap attr->attr-info)))]
                            (when (some? v) (first {k v}))))
  (empty [_]              (wrap (.empty entity) attr->attr-info))
  (count [_]              (.count entity))

  clojure.lang.ILookup
  (valAt [_ k]            (either (deserialize-attr entity attr->attr-info k)
                                  (wrap (.valAt entity k) attr->attr-info)))
  (valAt [_ k not-found]  (either (deserialize-attr entity attr->attr-info k)
                                  (wrap (.valAt entity k not-found) attr->attr-info)))

  datomic.Entity
  (db [_]                 (assoc (.db entity) :datomic-type-extensions.api/attr->attr-info attr->attr-info))
  (get [_ k]              (wrap (.get entity k) attr->attr-info))
  (keySet [_]             (.keySet entity))
  (touch [this]           (do (.touch entity)
                              (reset! touched? true)
                              this)))

(defn- equiv-entity [^TypeExtendedEntityMap e1 ^TypeExtendedEntityMap e2]
  (.equiv (let [^EntityMap em (.entity e1)] em)
          (let [^EntityMap em (.entity e2)] em)))

(defmethod print-method TypeExtendedEntityMap [entity writer]
  (print-method (merge {:db/id (:db/id entity)}
                       (when @(.-touched? entity)
                         (into {} entity)))
                writer))

(defn wrap
  [x attr->attr-info]
  (cond
    (instance? datomic.Entity x)
    (TypeExtendedEntityMap. x attr->attr-info (atom false))

    (coll? x)
    (set (map #(wrap % attr->attr-info) x))

    :else x))
