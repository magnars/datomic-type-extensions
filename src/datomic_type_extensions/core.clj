(ns datomic-type-extensions.core
  (:require [clojure.walk :refer [postwalk prewalk]]
            [datomic.api :as d]
            [datomic-type-extensions.types :as types]))

(defn apply-to-value [f attr-info val]
  (case (:db/cardinality attr-info)
    :db.cardinality/one (f val)
    :db.cardinality/many (cond
                           (set? val) (set (map f val))
                           (list? val) (map f val)
                           (vector? val) (mapv f val)
                           :else (throw (ex-info "Value must be either set, list or vector"
                                                 {:attr-info attr-info :val val})))))

(defn serialize-assertion-tx [form attr->attr-info]
  (if-let [[op e a v] (and (vector? form) form)]
    (let [attr-info (get attr->attr-info a)]
      (if (and (#{:db/add :db/retract} op)
               (:dte/valueType attr-info)
               (some? v))
        (update form 3 #(apply-to-value (partial types/serialize (:dte/valueType attr-info)) attr-info %))
        form))
    form))

(defn- update-attr [f form [k attr-info]]
  (if (get form k)
    (update form k #(apply-to-value (partial f (:dte/valueType attr-info)) attr-info %))
    form))

(defn serialize-tx-data [attr->attr-info tx-data]
  (prewalk
   (fn [form]
     (cond
       (map? form) (reduce #(update-attr types/serialize %1 %2) form attr->attr-info)
       (vector? form) (serialize-assertion-tx form attr->attr-info)
       :else form))
   tx-data))

(defn deserialize [attr->attr-info form]
  (postwalk
   (fn [form]
     (if (map? form)
       (reduce #(update-attr types/deserialize %1 %2) form attr->attr-info)
       form))
   form))

(defn serialize-lookup-ref [attr->attr-info eid]
  (if-let [attr-info (and (vector? eid)
                     (attr->attr-info (first eid)))]
    (update eid 1 #(types/serialize (:dte/valueType attr-info) %))
    eid))
