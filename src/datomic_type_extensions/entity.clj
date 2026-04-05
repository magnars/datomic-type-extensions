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

(declare wrap get-attr equiv-entity)

(defn deserialize-attr [attr->attr-info attr val]
  (when-let [attr-info (and (some? val) (get attr->attr-info attr))]
    (core/apply-to-value (partial types/deserialize (:dte/valueType attr-info))
                         attr-info
                         val)))

(deftype TypeExtendedEntityMap [^EntityMap entity attr->attr-info touched?]
  Object
  (hashCode [_]             (hash [(.hashCode entity) attr->attr-info]))
  (equals [this o]          (and (instance? TypeExtendedEntityMap o)
                                 (equiv-entity this o)))

  clojure.lang.Seqable
  (seq [this]               (map (fn [[k v]]
                                   (clojure.lang.MapEntry.
                                     k
                                     (get-attr this k v)))
                                 (.seq entity)))

  clojure.lang.Associative
  (equiv [this o]           (and (instance? TypeExtendedEntityMap o)
                                 (equiv-entity this o)))
  (containsKey [_ k]        (.containsKey entity k))
  (entryAt [this k]         (let [v (get-attr this k)]
                              (when (some? v) (first {k v}))))
  (empty [_]                (wrap (.empty entity) attr->attr-info))
  (count [_]                (.count entity))

  clojure.lang.ILookup
  (valAt [this k]           (get-attr this k))
  (valAt [this k not-found] (get-attr this k not-found))

  datomic.Entity
  (db [_]                   (assoc (.db entity) :datomic-type-extensions.api/attr->attr-info attr->attr-info))
  (get [this k]             (get-attr this k))
  (keySet [_]               (.keySet entity))
  (touch [this]             (do (.touch entity)
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
    (into (empty x) (map #(wrap % attr->attr-info) x))

    :else x))

(defn get-attr*
  [attr->attr-info attr val]
  (either (deserialize-attr attr->attr-info attr val)
          (wrap val attr->attr-info)))

(defn get-attr
  ([^TypeExtendedEntityMap entity attr]
   (get-attr* (.-attr->attr-info entity) attr (.valAt (.-entity entity) attr)))
  ([^TypeExtendedEntityMap entity attr not-found]
   (get-attr* (.-attr->attr-info entity) attr (.valAt (.-entity entity) attr not-found))))
