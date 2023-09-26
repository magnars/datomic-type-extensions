(ns datomic-type-extensions.types)

(defmulti get-backing-datomic-type identity)

(defmulti serialize (fn [type _] type))
(defmulti deserialize (fn [type _] type))

(defmacro define-dte [id backing-type serialize-sig serialize-body deserialize-sig deserialize-body]
  `(do
     (defmethod get-backing-datomic-type ~id [_#] ~backing-type)
     (defmethod serialize ~id [_# ~@serialize-sig] ~serialize-body)
     (defmethod deserialize ~id [_# ~@deserialize-sig] ~deserialize-body)))
