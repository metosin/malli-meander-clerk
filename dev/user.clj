(ns user
  (:require [nextjournal.clerk :as clerk]))

(clerk/serve! {:watch-paths ["notebooks"]
               :browse? true
               :port 6688})

(clerk/show! "notebooks/demo.clj")

(comment
  (clerk/build! {:paths ["notebooks/demo.clj"]}))
