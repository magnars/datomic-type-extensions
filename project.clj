(defproject datomic-type-extensions "2019-02-05"
  :description "A Clojure library that wraps Datomic API functions to add type extensions."
  :url "https://github.com/magnars/datomic-type-extensions"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[potemkin "0.4.5"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [com.datomic/datomic-free "0.9.5544"]
                                  [java-time-literals "2018-04-06"]]
                   :injections [(require 'java-time-literals.core)]
                   :plugins []
                   :source-paths ["dev"]}})

