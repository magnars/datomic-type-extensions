(ns datomic-type-extensions.query
  (:require [datomic-type-extensions.core :as core]
            [datomic-type-extensions.types :as types]))

(defn find-binding [[e a v]]
  (cond
    (keyword? a) [v a]

    (and (seq? e) (= 'get-else (first e)))
    [a (nth e 3)]))

(defn find-var->type-mapping [query attr-types]
  (let [where-clauses (next (drop-while #(not= :where %) query))]
    (->> (keep find-binding where-clauses)
         (keep (fn [[v a]] (when-let [type (attr-types a)]
                             [v type])))
         (into {}))))

(defn deserialization-pattern [query attr-types]
  (let [find-clauses (next (take-while #(not (#{:in :where} %)) query))
        var->type (find-var->type-mapping query attr-types)
        find-pattern #(if (and (seq? %) (= 'pull (first %)))
                        {:type :deserializable-form}
                        (var->type %))]
    (cond
      (= '. (fnext find-clauses))
      (find-pattern (first find-clauses))

      (vector? (first find-clauses))
      {:type :vector
       :pattern (find-pattern (ffirst find-clauses))}

      :else
      {:type :set
       :pattern {:type :tuple
                 :entries (mapv find-pattern find-clauses)}})))

(defn deserialize-by-pattern [form pattern attr-types]
  (cond
    (keyword? pattern)
    (types/deserialize pattern form)

    (= (:type pattern) :vector)
    (mapv #(deserialize-by-pattern % (:pattern pattern) attr-types) form)

    (= (:type pattern) :tuple)
    (mapv #(deserialize-by-pattern %1 %2 attr-types) form (:entries pattern))

    (= (:type pattern) :set)
    (set (map #(deserialize-by-pattern % (:pattern pattern) attr-types) form))

    (= (:type pattern) :deserializable-form)
    (core/deserialize attr-types form)

    :else form))

