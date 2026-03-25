(ns return-maps-failure-analysis
  (:require [datomic-type-extensions.api-test]
            [datomic-type-extensions.query :as query]
            [datomic-type-extensions.api :as api]
            [datomic.api :as d]))

(defn dte-without-checks [& args]
  (binding [query/*return-maps-checks* false]
    (apply api/q args)))

(def query-fns-all
  {:raw-datomic d/q
   :dte-with-checks api/q
   :dte-without-checks #'dte-without-checks})

(defn execute [closure]
  (try {:status :success
        :result (closure)}
       (catch Exception e
         {:status :failure
          :exception {:message (ex-message e)
                      :data (ex-data e)}})))

(defn investigate "Execute raw datomic, DTE without checking and DTE with checking"
  [query-fns-map db {:keys [query] :as specimen}]
  (merge specimen
         (into {}
               (for [[identifier query-fn] query-fns-map]
                 [identifier
                  (execute #(query-fn query db))]))))

(def specimens
  '[{:explanation "Query returns seq of tuples"
     :should "Suceed"
     :query [:find ?email ?demand
             :keys user/email user/demands
             :where
             [?user :user/email ?email]
             [?user :user/demands ?demand]]}

    {:explanation "Query returns seq of emails, not seq of tuples"
     :should "fail reasonably"
     :query [:find [?email ...]
             :keys user/email
             :where [_ :user/email ?email]]}

    {:explanation "Query returns single value, not seq of tuples"
     :should "fail reasonably"
     :query [:find ?email . :keys user/email :where [_ :user/email ?email]]}

    {:explanation "return-map-keys arity does not match tuple arity"
     :should "fail reasonably"
     :query [:find ?email ?demands
             :keys user/email
             :where [?u :user/email ?email] [?u :user/demands ?demands]]}

    {:explanation "Query is well-formed but result set is empty"
     :should "succeed"
     :query [:find ?demand
             :keys user/demand
             :where
             [?u :user/email "Glorfindel@lothlorien.net"]
             [?u :user/demands ?demand]]}])

(comment

  (set! *print-namespace-maps* false)
  (def db (d/db (datomic-type-extensions.api-test/create-populated-conn)))

  (->> specimens
       (map #(investigate query-fns-all db %)))

  )
