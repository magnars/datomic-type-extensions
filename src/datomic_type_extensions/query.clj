(ns datomic-type-extensions.query
  (:require [datomic-type-extensions.core :as core]
            [datomic-type-extensions.types :as types]))

(defn find-binding [[e a v]]
  (cond
    (keyword? a) [v a]

    (and (seq? e) (= 'get-else (first e)))
    [a (nth e 3)]))

(defn find-var->type-mapping [query attr->attr-info]
  (let [where-clauses (:where query)]
    (->> (keep find-binding where-clauses)
         (keep (fn [[v a]] (when-let [attr-info (attr->attr-info a)]
                             [v (:dte/valueType attr-info)])))
         (into {}))))

(defn deserialization-pattern [query attr->attr-info]
  (let [find-clauses (:find query)
        var->type (find-var->type-mapping query attr->attr-info)
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

(defn deserialize-by-pattern [form pattern attr->attr-info]
  (cond
    (keyword? pattern)
    (types/deserialize pattern form)

    (= (:type pattern) :vector)
    (mapv #(deserialize-by-pattern % (:pattern pattern) attr->attr-info) form)

    (= (:type pattern) :tuple)
    (mapv #(deserialize-by-pattern %1 %2 attr->attr-info) form (:entries pattern))

    (= (:type pattern) :set)
    (set (map #(deserialize-by-pattern % (:pattern pattern) attr->attr-info) form))

    (= (:type pattern) :deserializable-form)
    (core/deserialize attr->attr-info form)

    :else form))

(def ^{:doc "Keywords that signify that a new query clause is starting.

Source: https://docs.datomic.com/query/query-data-reference.html"}
  datomic-query-clause-keywords
  #{:find
    :keys :syms :strs
    :with
    :in
    :where})

(defn list-form->map-form [list-form-query]
  (->> list-form-query
       (partition-by datomic-query-clause-keywords)
       (partition 2)
       (map (fn [[[k] clauses]]
              [k (vec clauses)]))
       (into {})))

(defn ->map-form
  [query]
  (cond
    (map? query) query
    (string? query) (throw (ex-info "String-form Datomic queries are not supported by datomic-type-extensions" {}))
    (vector? query) (list-form->map-form query)))

(defn return-map-keys [query]
  (seq
   (let [return-map-part (select-keys query [:strs :keys :syms])
         [key-type the-keys] (first return-map-part)
         coerce-key (get {:strs str :keys keyword :syms symbol} key-type)]
     (mapv coerce-key the-keys))))

(defn validate-return-maps-query!
  "Validate a map-form query before execution

  return maps demand that the :find clause returns a collection of tuples, *and*
  that the number of :find variables match the number of requested keys in the
  return maps.

  See datomic docs for the query spec for the :find clause:
    https://docs.datomic.com/query/query-data-reference.html#arg-grammar
    https://docs.datomic.com/query/query-data-reference.html#find-specs"
  [{:keys [find]} return-map-keys]
  (cond
    ;; find-coll or find-tuple -> return maps not allowed
    (vector? (first find))
    (throw
     (ex-info
      "Cannot use find-coll or find-tuple find specs with return maps"
      {:cognitect.anomalies/category :cognitect.anomalies/incorrect
       :cognitect.anomalies/message "Cannot use find-coll or find-tuple find specs with return maps"}))

    ;; find-scalar -> return maps not allowed
    (= '. (last find))
    (throw
     (ex-info
      "Cannot use find-scalar find specs with return maps"
      {:cognitect.anomalies/category :cognitect.anomalies/incorrect
       :cognitect.anomalies/message "Cannot use find-scalar find specs with return maps"}))

    (not= (count find) (count return-map-keys))
    (throw
     (ex-info
      "Count of :keys/:strs/:syms must match count of :find"
      {:cognitect.anomalies/category :cognitect.anomalies/incorrect
       :cognitect.anomalies/message "Count of :keys/:strs/:syms must match count of :find"}))))

(defn strip-return-maps [query]
  (dissoc query :strs :keys :syms))

(defn return-maps [raw-query-results return-map-keys]
  (if (seq return-map-keys)
    (mapv (partial zipmap return-map-keys) raw-query-results)
    raw-query-results))

(comment

  (def query '{:find [?name]
               :where [[_ :person/name ?name]]})
  (= query (->map-form query))

  )
