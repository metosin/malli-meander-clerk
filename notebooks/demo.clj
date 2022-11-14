(ns demo
  (:require [clojure.edn :as edn]
            [malli.core :as m]
            [malli.transform :as mt]
            [malli.generator :as mg]
            [tech.v3.dataset :as ds]
            [meander.util.epsilon]
            [meander.match.epsilon :as mme]
            [malli.provider :as mp]
            [hato.client :as hc]
            [criterium.core :as cc]))

(-> {:id "1"}
    (update :id parse-long)
    (assoc :name "Elegia")
    (update :tags (fnil conj #{}) "poem"))
; => {:id 1, :name "Elegia", :tags #{"poem"}}

;; create data-file
(spit "orders.csv" "id,firstName,lastName,street,item1,item2,zip
 1,Sauli,Niinistö,Mariankatu 2,coffee,buns,00170
 2,Sanna,Marin,Kesärannantie 1,juice,pasta,00250")

(def orders (ds/->dataset "orders.csv" {:key-fn keyword, :parser-fn :string}))

orders
;| :id | :firstName | :lastName |         :street | :item1 | :item2 |  :zip |
;|----:|------------|-----------|-----------------|--------|--------|-------|
;|   1 |      Sauli |  Niinistö |    Mariankatu 2 | coffee |   buns | 00170 |
;|   2 |      Sanna |     Marin | Kesärannantie 1 |  juice |  pasta | 00250 |

(dissoc orders :zip :street)
;| :id | :firstName | :lastName | :item1 | :item2 |
;|----:|------------|-----------|--------|--------|
;|   1 |      Sauli |  Niinistö | coffee |   buns |
;|   2 |      Sanna |     Marin |  juice |  pasta |

(def CSVOrder (mp/provide (ds/rows orders)))

CSVOrder
;[:map
; [:id :string]
; [:firstName :string]
; [:lastName :string]
; [:street :string]
; [:item1 :string]
; [:item2 :string]
; [:zip :string]]

(->> (ds/rows orders)
     (map (m/validator CSVOrder))
     (every? true?))
; => true

(def Order
  [:map {:db/table "Orders"}
   [:id :uuid]
   [:source [:enum "csv" "online"]]
   [:source-id :string]
   [:name {:optional true} :string]
   [:items [:vector :keyword]]
   [:delivered {:default false} :boolean]
   [:address [:map
              [:street :string]
              [:zip :string]]]])

(mg/generate Order {:seed 3})
;{:id #uuid"b36c2541-2db8-4d75-b87d-3413bdacdb7d",
; :source "online",
; :source-id "",
; :items [:y!Aw11EA :PUPjb-_T :DPXc!g:e],
; :delivered true,
; :address {:street "MG7rxPm6jywJSPqEs"
;           :zip "116iS2c74JGKv90oAhJP7aq7iL8iyk"}}

(defn coercer [schema transformer]
  (let [valid? (m/validator schema)
        decode (m/decoder schema transformer)
        explain (m/explainer schema)]
    (fn [x]
      (let [value (decode x)]
        (when-not (valid? value)
          (m/-fail! ::invalid-input {:value value
                                     :schema schema
                                     :explain (explain value)}))
        value))))

(defn load-csv [file]
  (ds/rows (ds/->dataset file {:key-fn keyword, :parser-fn :string})))

(def validate-input (coercer CSVOrder (mt/no-op-transformer)))

(->> (load-csv "orders.csv")
     (map validate-input))
;({:id "1",
;  :firstName "Sauli",
;  :lastName "Niinistö",
;  :street "Mariankatu 2",
;  :item1 "coffee",
;  :item2 "buns",
;  :zip "00170"}
; {:id "2",
;  :firstName "Sanna",
;  :lastName "Marin",
;  :street "Kesärannantie 1",
;  :item1 "juice",
;  :item2 "pasta",
;  :zip "00250"})

(defn matcher [{:keys [pattern expression]}]
  (eval `(fn [data#]
           (let [~'data data#]
             ~(mme/compile-match-args
               (list 'data pattern expression)
               nil)))))

(def transform
  (matcher
   {:pattern '{:id ?id
               :firstName ?firstName
               :lastName ?lastName
               :street ?street
               :item1 !item
               :item2 !item
               :zip ?zip}
    :expression '{:id (random-uuid)
                  :source "csv"
                  :source-id ?id
                  :name (str ?firstName " " ?lastName)
                  :items (vector !item)
                  :address {:street ?street
                            :zip ?zip}}}))

(load-csv "orders.csv")

(->> (load-csv "orders.csv")
     (map validate-input)
     (map transform))
;({:id #uuid"7f765cf9-24a2-4bd8-950f-35f6c8724c65",
;  :source "csv",
;  :source-id "1",
;  :name "Sauli Niinistö",
;  :items #{"buns" "coffee"},
;  :address {:street "Mariankatu 2", :zip "00170"}}
; {:id #uuid"3fc14e81-170f-4234-983a-9d9cc2a47ed5",
;  :source "csv",
;  :source-id "2",
;  :name "Sanna Marin",
;  :items #{"pasta" "juice"},
;  :address {:street "Kesärannantie 1", :zip "00250"}})

(def validate-output
  (coercer
   Order
   (mt/transformer
    (mt/string-transformer)
    (mt/default-value-transformer))))


(->> (load-csv "orders.csv")
     (map validate-input)
     (map transform)
     (map validate-output))
;({:id #uuid"a3fae918-9b5a-4f54-9d32-4c22b74e8922",
;  :source "csv",
;  :source-id "1",
;  :name "Sauli Niinistö",
;  :items #{:buns :coffee},
;  :address {:street "Mariankatu 2", :zip "00170"},
;  :delivered false}
; {:id #uuid"5f4cb486-6990-457b-8fca-bebc25b23277",
;  :source "csv",
;  :source-id "2",
;  :name "Sanna Marin",
;  :items #{:pasta :juice},
;  :address {:street "Kesärannantie 1", :zip "00250"},
;  :delivered false})

(def transformation
  {:registry {:csv/order [:map
                          [:id :string]
                          [:firstName :string]
                          [:lastName :string]
                          [:street :string]
                          [:item1 :string]
                          [:item2 :string]
                          [:zip :string]]
              :domain/order [:map {:db/table "Orders"}
                             [:id :uuid]
                             [:source [:enum "csv" "online"]]
                             [:source-id :string]
                             [:name {:optional true} :string]
                             [:items [:vector :keyword]]
                             [:delivered {:default false} :boolean]
                             [:address [:map
                                        [:street :string]
                                        [:zip :string]]]]}
   :mappings {:source :csv/order
              :target :domain/order
              :pattern '{:id ?id
                         :firstName ?firstName
                         :lastName ?lastName
                         :street ?street
                         :item1 !item
                         :item2 !item
                         :zip ?zip}
              :expression '{:id (random-uuid)
                            :source "csv"
                            :source-id ?id
                            :name (str ?firstName " " ?lastName)
                            :items !item
                            :address {:street ?street
                                      :zip ?zip}}}})

(-> transformation
    (pr-str)
    (edn/read-string)
    (= transformation))
; => true

(defn transformer [{:keys [registry mappings]} source-transformer target-transformer]
  (let [{:keys [source target]} mappings
        xf (comp (map (coercer (get registry source) source-transformer))
                 (map (matcher mappings))
                 (map (coercer (get registry target) target-transformer)))]
    (fn [data] (into [] xf data))))

(def pipeline
  (transformer
   transformation
   (mt/no-op-transformer)
   (mt/transformer
    (mt/string-transformer)
    (mt/default-value-transformer))))

(pipeline (load-csv "orders.csv"))
;({:id #uuid"e809652f-8399-467e-9bab-a0e2bba1548e",
;  :source "csv",
;  :source-id "1",
;  :name "Sauli Niinistö",
;  :items #{:buns :coffee},
;  :address {:street "Mariankatu 2", :zip "00170"},
;  :delivered false}
; {:id #uuid"fc255810-7235-43ba-93bb-e5abc7c78168",
;  :source "csv",
;  :source-id "2",
;  :name "Sanna Marin",
;  :items #{:pasta :juice},
;  :address {:street "Kesärannantie 1", :zip "00250"},
;  :delivered false})

(defn clojure-transformer [x]
  (mapv (fn [{:keys [id firstName lastName street item1 item2 zip]}]
          {:id (random-uuid)
           :source "csv"
           :source-id id
           :name (str firstName " " lastName)
           :items [(keyword item1) (keyword item2)]
           :address {:street street
                     :zip zip}
           :delivered false}) x))


(def data
  (let [!id (atom 0)
        rand-item #(rand-nth ["bun" "coffee" "pasta" "juice"])]
    (->> (hc/get "https://randomuser.me/api/"
                 {:query-params {:results 1000, :seed 123}
                  :as :json})
         :body
         :results
         (map (fn [user]
                {:id (str (swap! !id inc))
                 :firstName (-> user :name :first)
                 :lastName (-> user :name :last)
                 :street (str (-> user :location :street :name) " "
                              (-> user :location :street :number))
                 :item1 (rand-item)
                 :item2 (rand-item)
                 :zip (-> user :location :postcode str)})))))

(take 2 data)
;({:id "1"
;  :firstName "Heldo"
;  :lastName "Campos"
;  :street "Rua Três 9120"
;  :item1 "bun"
;  :item2 "juice"
;  :zip "73921"}
; {:id "2",
;  :firstName "Candice",
;  :lastName "Long",
;  :street "Northaven Rd 4744",
;  :item1 "pasta",
;  :item2 "coffee",
;  :zip "25478"})

(comment

 (require '[criterium.core :as cc])

 ;; 480µs
 (cc/quick-bench (clojure-transformer data))

 ;; 840µs
 (cc/quick-bench (pipeline data)))
