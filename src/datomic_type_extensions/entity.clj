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

(declare wrap)

(defn deserialize-attr [entity attr-infos attr]
  (when-let [val (attr entity)]
    (when-let [attr-info (get attr-infos attr)]
      (core/apply-to-value (partial types/deserialize (:dte/valueType attr-info))
                           attr-info
                           val))))

(deftype TypeExtendedEntityMap [^EntityMap entity attr-infos touched?]
  Object
  (hashCode [_]           (hash [(.hashCode entity) attr-infos]))
  (equals [_ o]           (and (instance? TypeExtendedEntityMap o)
                               (.equiv entity (.entity o))))

  clojure.lang.Seqable
  (seq [_]                 (map (fn [[k v]]
                                  [k (either (deserialize-attr entity attr-infos k)
                                             (wrap (.valAt entity k) attr-infos))])
                                (.seq entity)))

  clojure.lang.Associative
  (equiv [_ o]            (and (instance? TypeExtendedEntityMap o)
                               (.equiv entity (.entity o))))
  (containsKey [_ k]      (.containsKey entity k))
  (entryAt [_ k]          (let [v (either (deserialize-attr entity attr-infos k)
                                          (some-> entity (.entryAt k) .val (wrap attr-infos)))]
                            (when (some? v) (first {k v}))))
  (empty [_]              (wrap (.empty entity) attr-infos))
  (count [_]              (.count entity))

  clojure.lang.ILookup
  (valAt [_ k]            (either (deserialize-attr entity attr-infos k)
                                  (wrap (.valAt entity k) attr-infos)))
  (valAt [_ k not-found]  (either (deserialize-attr entity attr-infos k)
                                  (wrap (.valAt entity k not-found) attr-infos)))

  datomic.Entity
  (db [_]                 (assoc (.db entity) :datomic-type-extensions.api/attr-infos attr-infos))
  (get [_ k]              (wrap (.get entity k) attr-infos))
  (keySet [_]             (.keySet entity))
  (touch [this]           (do (.touch entity)
                              (reset! touched? true)
                              this)))

(defmethod print-method TypeExtendedEntityMap [entity writer]
  (print-method (merge {:db/id (:db/id entity)}
                       (when @(.-touched? entity)
                         (into {} entity)))
                writer))

(defn wrap
  [x attr-infos]
  (cond
    (instance? datomic.Entity x)
    (TypeExtendedEntityMap. x attr-infos (atom false))

    (coll? x)
    (set (map #(wrap % attr-infos) x))

    :else x))
