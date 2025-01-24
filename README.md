# Datomic type extensions

<img align="right" width=150 src="https://upload.wikimedia.org/wikipedia/en/b/bf/Dead_Kennedys_-_Give_Me_Convenience_or_Give_Me_Death_cover.jpg">

This Clojure library provides custom types for your Datomic attributes. It does
this by wrapping a leaky abstraction around your regular Datomic API.

- Add custom types by implementing the `serialize`, `deserialize` and
  `get-backing-datomic-type` multimethods in the `datomic-type-extensions.types`
  namespace.

- Require `[datomic-type-extensions.api :as d]` instead of `[datomic.api :as d]`.

- When you `d/connect` the first time, a `:dte/valueType` attribute will be
  installed.

- Assert `:dte/valueType` for your typed attributes. When transacting attribute
  definitions, the original `:db/valueType` will be added by looking it up in
  `get-backing-datomic-type`.

- When using `d/transact`, `d/transact-async` or `d/with`, your typed attributes
  will be serialized before being passed to Datomic.

- When using `d/q`, `d/query`, `d/pull` or `d/pull-many`, your typed attributes will be
  deserialized on the way out of Datomic.

- Entities returned by `d/entity` will lazily deserialize their types.

Oh, the convenience!

### Did you say leaky abstraction?

Oh yes. Let's look at some ways this abstraction leaks:

- Database functions see serialized values.

- Where-clauses in queries see serialized values.

- Params to queries are not serialized for you.

- Datoms (as returned by `:tx-data`, indexes, and the log) are not
  deserialized.

There might be more, but during several years of production use, these are the ones we have encountered.

### Usage

Define a custom type:

```clj
(require '[datomic-type-extensions.types :as types])

(types/define-dte :java.time/instant

  ;; native Datomic backing type
  :db.type/instant

  ;; serialize
  [^java.time.Instant this]
  (java.util.Date/from this)

  ;; deserialize
  [^java.util.Date inst]
  (java.time.Instant/ofEpochMilli (.getTime inst)))
```

If you're interested in storing [java.time](https://docs.oracle.com/javase/8/docs/api/java/time/package-summary.html)
types in Datomic, use [java-time-dte](https://github.com/magnars/java-time-dte).

Then use the custom type:

```clj
(require '[datomic-type-extensions.api :as d])

(defn create-conn []
  (let [url (str "datomic:mem://" (d/squuid))]
    (d/create-database url)
    (d/connect url)))

(def conn (create-conn))

@(d/transact
  conn
  [{:db/ident :user/email
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/created-at
    :dte/valueType :java.time/instant ;; here's the typed attribute
    :db/cardinality :db.cardinality/one}])

@(d/transact
  conn
  [{:user/email "foo@example.com"
    :user/created-at (java.time.Instant/parse "2017-01-01T00:00:00Z")}])

(d/pull (d/db conn)
        [:user/created-at]
        [:user/email "foo@example.com"]) ;; :user/created-at is a java.time.Instant

(:user/created-at (d/entity (d/db conn) [:user/email "foo@example.com"]))
;; => returns a java.time.Instant

(d/q '[:find ?inst . :where [_ :user/created-at ?inst]]
     (d/db conn)) ;; so does this

(let [[[e a v]]
      (seq (d/datoms (d/db conn) :eavt [:user/email "foo@example.com"] :user/created-at))]
  v) ;; this leaks: it returns a java.util.Date (the serialized backing type)
```

### So now what?

Feel free to use it. It's been used for years in several projects. Just be aware
that it is leaky. For me, I hope that one day this entire library will be made
redundant by the Datomic team.

### How can I use it with Conformity?

Since Conformity's `ensure-conforms` transacts for you using the non-wrapped
Datomic API, you can add the backing types like so:

```clj
(d/add-backing-types tx-data)
```

before sending the migrations to Conformity.

If you are also migrating in data that needs to be serialized, you might have to
do the attribute migrations first, and then do:

```clj
(d/prepare-tx-data db tx-data)
```

on the data migration.

## Install

With tools.deps:

```clj
datomic-type-extensions/datomic-type-extensions {:mvn/version "2025.01.24"}
```

With Leiningen:

```clj
[datomic-type-extensions "2025.01.24"]
```

## Changes

#### From 2024.11.20 to 2025.01.24

- Support retractions without a value. ([Tormod Mathiesen](/2food))
- Add support for query-stats and io-stats.
- Add support for return maps in queries.
- Add support for queries as maps.

#### From 2024.02.09 to 2024.11.20

- Bugfix: Calling `seq` on an entity now returns a list of `MapEntry`

    This is the expected behavior of datomic entities, and makes it work with `keys`.

#### From 2024.01.17 to 2024.02.09

- **BREAKING BUGFIX**

    Data with nested dte-backed attributes was not properly deserialized. This
    bug has now been fixed, with potential breakage if your code relied on this
    asymmetry.

    More information in the PR: https://github.com/magnars/datomic-type-extensions/pull/9

#### From 2020-05-26 to 2024.01.17

- Add `datomic-type-extensions.types/define-dte` macro for even more
  convenience.
- Support for linting `define-dte` with clj-kondo.

#### From 2019-09-04 to 2020-05-26

- Better performance with type hints to avoid reflection.

#### From 2019-05-10 to 2019-09-04

- We now wrap `datomic.api/history` to ensure we cache the type extended
  attributes *before* the history db makes us unable to find them.

#### From 2019-02-05 to 2019-05-10

- We now wrap `datomic.api/filter` to ensure we cache the type extended
  attributes *before* you filter them out of the database.

#### From 2019-02-05 to 2019-02-18

Bugfixes:

- Fix serialization / deserialization of multi-value attributes again

#### From 2019-01-23 to 2019-02-05

Bugfixes:

- Fix lookup of missing serialized attribute.

#### From 2018-11-06 to 2019-01-23

Bugfixes:

- Fix serialization / deserialization of multi-value attributes (i.e. :db.cardinality/many)

#### From 2018-04-18 to 2018-11-06

Bugfixes / aligning the APIs with Datomic:

- Make TypeExtendedEntityMap behave more like Datomic wrt printing (Anders Furseth)
- Hash EntityMap and TypeExtendedEntityMap differently
- Cache the attr-types lookup on db gotten from entity
- Wrap entity when emptied

## License

Copyright © Anders Furseth and Magnar Sveen, since 2018

Distributed under the Eclipse Public License, the same as Clojure.
