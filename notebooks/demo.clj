(ns demo)

;; # Transforming Data With Malli and Meander
;; Source code for the post: https://www.metosin.fi/blog/transforming-data-with-malli-and-meander/

(require '[nextjournal.clerk :as clerk])

(-> {:id "1"}
    (update :id parse-long)
    (assoc :name "Elegia")
    (update :tags (fnil conj #{}) "poem")
    (clerk/code))

(spit "orders.csv" "id,firstName,lastName,street,item1,item2,zip
 1,Sauli,Niinistö,Mariankatu 2,coffee,buns,00170
 2,Sanna,Marin,Kesärannantie 1,juice,pasta,00250")

(require '[tech.v3.dataset :as ds])

(def orders (ds/->dataset "orders.csv" {:key-fn keyword, :parser-fn :string}))

(clerk/table orders)

(clerk/table (dissoc orders :zip :street))

(require '[malli.provider :as mp])

(def CSVOrder (mp/provide (ds/rows orders)))

(clerk/code CSVOrder)

(require '[malli.core :as m])

(->> (ds/rows orders)
     (map (m/validator CSVOrder))
     (every? true?))

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

(require '[malli.generator :as mg])

(clerk/code (mg/generate Order {:seed 3}))

(defn load-csv [file]
  (ds/rows (ds/->dataset file {:key-fn keyword, :parser-fn :string})))

(require '[malli.transform :as mt])

(def validate-input (m/coercer CSVOrder))

(->> (load-csv "orders.csv")
     (map validate-input)
     (clerk/code))


(require '[meander.match.epsilon :as mme])

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
                  :items !item
                  :address {:street ?street
                            :zip ?zip}}}))

(->> (load-csv "orders.csv")
     (map validate-input)
     (map transform)
     (clerk/code))

(def validate-output
  (m/coercer
   Order
   (mt/transformer
    (mt/string-transformer)
    (mt/default-value-transformer))))

(->> (load-csv "orders.csv")
     (map validate-input)
     (map transform)
     (map validate-output)
     (clerk/code))

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

(require '[clojure.edn :as edn])

(-> transformation
    (pr-str)
    (edn/read-string)
    (= transformation))

(defn transformer [{:keys [registry mappings]} source-transformer target-transformer]
  (let [{:keys [source target]} mappings
        xf (comp (map (m/coercer (get registry source) source-transformer))
                 (map (matcher mappings))
                 (map (m/coercer (get registry target) target-transformer)))]
    (fn [data] (into [] xf data))))

(def pipeline
  (transformer
   transformation
   nil
   (mt/transformer
    (mt/string-transformer)
    (mt/default-value-transformer))))

(clerk/code (pipeline (load-csv "orders.csv")))

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

(require '[hato.client :as hc])

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

(clerk/code (take 2 data))

(comment

 (require '[criterium.core :as cc])

 ;; 480µs
 (cc/quick-bench (clojure-transformer data))

 ;; 840µs
 (cc/quick-bench (pipeline data)))
