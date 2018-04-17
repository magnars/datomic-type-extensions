(ns datomic-type-extensions.core
  (:require [clojure.walk :refer [postwalk]]
            [datomic.api :as d]
            [datomic-type-extensions.types :as types]))

(defn serialize-assertion-tx [form attr-types]
  (if-let [[op e a v] (and (vector? form) form)]
    (if-let [type (and (#{:db/add :db/retract} op)
                       (get attr-types a))]
      (update form 3 #(types/serialize type %))
      form)
    form))

(defn- update-attr [f form [k type]]
  (if (get form k)
    (update form k #(f type %))
    form))

(defn serialize-tx-data [attr-types tx-data]
  (postwalk
   (fn [form]
     (cond
       (map? form) (reduce #(update-attr types/serialize %1 %2) form attr-types)
       (vector? form) (serialize-assertion-tx form attr-types)
       :else form))
   tx-data))

(defn deserialize [attr-types form]
  (postwalk
   (fn [form]
     (if (map? form)
       (reduce #(update-attr types/deserialize %1 %2) form attr-types)
       form))
   form))

(defn serialize-lookup-ref [attr-types eid]
  (if-let [type (and (vector? eid)
                     (attr-types (first eid)))]
    (update eid 1 #(types/serialize type %))
    eid))
