(ns download-portland-maps
  "Export all Portland Maps data to JSONL."
  (:require
   [babashka.http-client :as http]
   [clojure.core.async :as async]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [diehard.core :refer [with-retry]])
  (:import
   (java.io File Writer)
   (java.util.concurrent Semaphore)))

(def api-key (System/getenv "PORTLAND_MAPS_API_KEY"))

;; ----------------------------------------------------------------------
;; Utils

(defmacro vthread
  [& body]
  `(let [p# (promise)]
     (Thread/startVirtualThread
       (fn []
         (deliver p# (do ~@body))))
     p#))


(defn csv-data->maps
  [csv-data]
  (let [header (->> (first csv-data) (map keyword) repeat)]
    (map zipmap header (rest csv-data))))

;; ----------------------------------------------------------------------

(defn lookup
  "Look up property details for a given property on Portland Maps."
  [primaccnum]
  (with-retry {:max-retries 5
               :backoff-ms  [1000 10000 ]}
    (:body
     (http/get "https://www.portlandmaps.com/api/detail.cfm"
               {:headers
                {"Sec-Fetch-Mode"   "cors"
                 "Referer"          "https://www.portlandmaps.com/"
                 "X-Requested-With" "XMLHttpRequest"}
                :query-params
                {"detail_id"   primaccnum
                 "detail_type" "assessor"
                 "sections"    "*"
                 "format"      "json"
                 "embed"       "true"
                 "expand"      "1"
                 "api_key"     api-key}}))))

(defn writer
  "Create a channel that writes all messages to the given file."
  [file]
  (let [ch   (async/chan 100)
        file (io/file file)]
    (when-not (File/.exists file)
      (File/.createNewFile file))
    (vthread
      (with-open [writer (io/writer file)]
        (loop []
          (when-let [msg (str (async/<!! ch))]
            (Writer/.append writer msg)
            (Writer/.append writer "\n")
            (recur)))))
    ch))

(defn export-property
  "Export a single property."
  [{:keys [PROPERTYID]} out-ch err-ch]
  (println PROPERTYID)
  (try
    (async/>!! out-ch (lookup PROPERTYID))
    (catch Exception e
      (async/>!! err-ch (print-str PROPERTYID "=>" e)))))

(defn -main
  [& _args]
  (println "Exporting...")
  (with-open [reader (io/reader (io/resource "taxlots.csv"))]
    ;; Let's spin up 200k threads to process this data.
    (let [semaphore (Semaphore. 10)
          out-ch    (writer "taxlots.jsonl")
          err-ch    (writer "errors.log")
          rows      (csv-data->maps (csv/read-csv reader))
          tasks     (for [row rows]
                      (vthread
                        (try (Semaphore/.acquire semaphore)
                          (export-property row out-ch err-ch)
                          (finally (.release semaphore)))))]
      (run! deref tasks))))
