(ns datomic-type-extensions.core
  (:require [clojure.walk :refer [postwalk]]
            [datomic.api :as d]
            [datomic-type-extensions.types :as types]))

(defn apply-to-value [f attr-type val]
  (case (:db/cardinality attr-type)
    :db.cardinality/one (f val)
    :db.cardinality/many (cond
                           (set? val) (set (map f val))
                           (list? val) (map f val)
                           (vector? val) (mapv f val)
                           :else (f val))))

(defn serialize-assertion-tx [form attr-types]
  (if-let [[op e a v] (and (vector? form) form)]
    (let [attr-type (get attr-types a)]
      (if (and (#{:db/add :db/retract} op)
               (:dte/valueType attr-type))
        (update form 3 #(apply-to-value (partial types/serialize (:dte/valueType attr-type)) attr-type %))
        form))
    form))

(defn- update-attr [f form [k type]]
  (if (get form k)
    (update form k #(apply-to-value (partial f (:dte/valueType type)) type %))
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
  (if-let [attr-type (and (vector? eid)
                     (attr-types (first eid)))]
    (update eid 1 #(types/serialize (:dte/valueType attr-type) %))
    eid))
