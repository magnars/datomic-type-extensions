(ns datomic-type-extensions.query
  (:require [clojure.set :as set]
            [datomic-type-extensions.core :as core]
            [datomic-type-extensions.types :as types]))

(defn find-binding [[e a v]]
  (cond
    (keyword? a) [v a]

    (and (seq? e) (= 'get-else (first e)))
    [a (nth e 3)]))

(defn find-var->type-mapping [query attr->attr-info]
  (let [where-clauses (if (map? query)
                        (:where query)
                        (next (drop-while #(not= :where %) query)))]
    (->> (keep find-binding where-clauses)
         (keep (fn [[v a]] (when-let [attr-info (attr->attr-info a)]
                             [v (:dte/valueType attr-info)])))
         (into {}))))

(defn deserialization-pattern [query attr->attr-info]
  (let [find-clauses (if (map? query)
                       (:find query)
                       (next (take-while #(not (#{:in :where} %)) query)))
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

;; Design notes for implementation of datomic query return maps
;; ============================================================
;;
;; Terminology:
;;
;; - query              - [:find ?e :where ?e :person/name] - a map-form or vector-form Datomic query
;; - query-map          - {:query query :args args}         - query with args, including db
;; - return-map-request - {:keys ['name 'age]}              - query part that requests maps to be returned
;;
;; Known limitations:
;;
;; - A datomic-type-extensions user that has written an illegal query combining
;;   return maps with a :find clause that does not return a set of tuples will
;;   get poor error messages.
;;
;;   Examples:
;;
;;     [:find [?name ...] :keys name :where [_ :person/name ?name]]
;;     [:find ?name . :keys name :where [_ :person/name ?name]]
;;
;;   In these cases, Datomic will return something that doesn't make sense to
;;   use with query return maps - as the user already has requested a different
;;   type of output than sequence of maps.
;;
;; - Worse error messages than datomic if the user requests return maps that
;;   contain an illegal number of keys
;;
;;   Example:
;;
;;     [:find ?name ?age :keys name :where [_ :person/name ?name]]
;;
;;   In this case, the find clause picks out
;;
;; In other words, the implementation of datomic query return maps in
;; datomic-type-extensions is, unfortunately, leaky.

(def ^{:doc "Keywords that signify that a new query clause is starting.

Source: https://docs.datomic.com/query/query-data-reference.html"}
  datomic-query-clause-keywords
  #{:find
    :keys :syms :strs
    :with
    :in
    :where})

(defn vector-style-query->map-style-query [vector-style-query]
  (->> vector-style-query
       (partition-by datomic-query-clause-keywords)
       (partition 2)
       (map (fn [[[k] clauses]]
              [k clauses]))
       (into {})))

(defn ex-string-style-queries-not-supported []
  (ex-info "String-style Datomic queries are not supported by datomic-type-extensions" {}))

(defn canonicalize-query
  [query]
  (cond
    (map? query) query
    (string? query) (ex-string-style-queries-not-supported)
    (vector? query) (vector-style-query->map-style-query query)))

(defn canonicalized-query->return-map-keys [canonicalized-query]
  (let [return-map-part (select-keys canonicalized-query [:strs :keys :syms])]
    (when (seq return-map-part)
      (when-not (= 1 (count return-map-part))
        (throw (ex-info "invalid return map request"
                        {:canonicalized-query canonicalized-query
                         :error ::more-than-one-return-map-clause})))
      (when-not (seq (val (first return-map-part)))
        (throw (ex-info "invalid return map request"
                        {:canonicalized-query canonicalized-query
                         :error ::return-map-keys-not-seqence})))
      (let [[key-type the-keys] (first return-map-part)
            coerce-key (get {:strs str :keys keyword :syms symbol} key-type)]
        (mapv coerce-key the-keys)))))

(defn query->stripped-canonicalized-query+return-map-keys [query]
  (let [canonicalized (canonicalize-query query)]
    [(dissoc canonicalized :strs :keys :syms)
     (canonicalized-query->return-map-keys canonicalized)]))

(defn return-maps [raw-query-results return-map-keys]
  (if (seq return-map-keys)
    (do
      (when (not (and (seq raw-query-results)
                      (seq (first raw-query-results))))
        (throw (ex-info "Return map keys are provided, and query results have illegal data format"
                        {:raw-query-results raw-query-results :return-map-keys return-map-keys})))
      (when (not (= (count return-map-keys)
                    (count (first raw-query-results))))
        (throw (ex-info "Return map key count does not match row size"
                        {:raw-query-results raw-query-results :return-map-keys return-map-keys})))
      (mapv (partial zipmap return-map-keys) raw-query-results))
    raw-query-results))

(comment

  (def query '{:find [?name]
               :where [[_ :person/name ?name]]})
  (= query (canonicalize-query query))

  )
