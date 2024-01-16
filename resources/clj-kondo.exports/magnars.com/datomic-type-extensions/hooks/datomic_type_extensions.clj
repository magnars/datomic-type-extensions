(ns hooks.datomic-type-extensions
  (:require [clj-kondo.hooks-api :as api]))

(defn define-dte [{:keys [node]}]
  (let [[_ _ binding1 body1 binding2 body2] (rest (:children node))]
    {:node
     (api/vector-node
      [(api/list-node
        (list
         (api/token-node 'fn)
         binding1
         body1))
       (api/list-node
        (list
         (api/token-node 'fn)
         binding2
         body2))])}))
