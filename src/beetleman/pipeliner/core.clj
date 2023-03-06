(ns beetleman.pipeliner.core
  (:refer-clojure :exclude [await])
  (:require [malli.core :as m]
            [malli.error :as me]))

(defn example-step-handler [{:keys [ctx data]}]
  {:ctx ctx
   :data data})

(def default-schema [:map
                     [:ctx :map]
                     [:data :map]])

(def example-step
  {:handler example-step-handler
   :name    "Example step"
   :schema  default-schema})

(def step-schema [:map
                  [:name :string]
                  [:handler fn?]
                  [:schema [:vector {:min 1} :any]]])

(def steps-schema
  [:vector {:min 1} step-schema])

(defn assert-schema! [schema data message error-data]
  (when-not (m/validate schema data)
    (throw (ex-info message
                    (assoc error-data
                           :errors (->> data
                                        (m/explain schema)
                                        (me/humanize)))))))

(defn- handle-step [step-data
                    {:keys [handler schema name]
                     :or   {schema default-schema}
                     :as   step}
                    previous-step]
  (assert-schema! schema
                  step-data
                  (str "Validation error, step: " name)
                  {:previous-step previous-step})
  (handler (assoc-in step-data
                     [:ctx ::current] step)))

(defprotocol Step
  (execute-step [step-data step previous-step])
  (await [step-data]))

(extend-protocol Step
  Object
  (execute-step [step-data step previous-step]
    (handle-step step-data step previous-step))
  (await [step-data] step-data)

  clojure.lang.IDeref
  (execute-step [step-data step previous-step]
    (future
      (handle-step @step-data step previous-step)))
  (await [step-data] @step-data))

(defn pipeline
  ([steps data]
   (pipeline steps data {}))
  ([steps data ctx]
   (assert-schema! steps-schema steps "Steps validation" {})
   (-> (reduce (fn [{:keys [step-data previous-step]} step]
                 {:step-data     (execute-step step-data step previous-step)
                  :previous-step step})
               {:step-data     {:ctx  ctx
                                :data data}
                :previous-step nil}
               steps)
       :step-data
       await
       :data)))

(comment
  (pipeline [example-step
             example-step
             {:handler (fn [{:keys [ctx data]}]
                         (future data))
              :schema  default-schema
              :name    "step with errors"}
             example-step]
            {:x 1})
  )
