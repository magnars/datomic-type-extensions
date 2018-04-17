(ns datomic-type-extensions.types)

(defmulti get-backing-datomic-type identity)

(defmulti serialize (fn [type _] type))
(defmulti deserialize (fn [type _] type))
