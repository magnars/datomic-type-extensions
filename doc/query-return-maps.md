Design notes for implementation of datomic query return maps
============================================================

Terminology
-----------

| term            | example                             | definition                              |
|-----------------|-------------------------------------|-----------------------------------------|
| query           | `[:find ?e :where ?e :person/name]` | a map-form or vector-form Datomic query |
| query-map       | `{:query query :args args}`         | query with args, including db           |
| return-map-keys | `[:name :age]`                      | keys present in returned maps           |

Error messages in Datomic and datomic-type-extensions
-----------------------------------------------------

- A datomic-type-extensions user that has written an illegal query combining
  return maps with a :find clause that does not return a set of tuples will
  get worse error messages than Datomic can provide.

  Examples:

      [:find [?name ...] :keys name :where [_ :person/name ?name]]
      [:find ?name . :keys name :where [_ :person/name ?name]]

  In these cases, Datomic will return something that doesn't make sense to
  use with query return maps - as the user already has requested a different
  type of output than sequence of maps.

- Worse error messages than datomic if the user requests return maps that
  contain an illegal number of keys.

  Example:

      [:find ?name ?age :keys name :where [_ :person/name ?name]]

  Datomic catches this error by analyzing the query, datomic-type-extensions
  catches this error when the return value from the query isn't a set of tuples.

In other words, the implementation of datomic query return maps in
datomic-type-extensions is, unfortunately, leaky!
