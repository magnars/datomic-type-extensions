(defproject datomic-type-extensions "2024.01.17"
  :description "A Clojure library that wraps Datomic API functions to add type extensions."
  :url "https://github.com/magnars/datomic-type-extensions"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[potemkin "0.4.5"]]
  :resource-paths ["resources"]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]
                                  [com.datomic/datomic-free "0.9.5697"]
                                  [java-time-literals "2018-04-06"]
                                  [org.clojure/tools.cli "0.4.1"] ;; for kaocha to recognize command line options
                                  [lambdaisland/kaocha "0.0-389"]
                                  [kaocha-noyoda "2019-01-29"]]
                   :injections [(require 'java-time-literals.core)]
                   :plugins []
                   :source-paths ["dev"]}}
  :aliases {"kaocha" ["run" "-m" "kaocha.runner"]})
