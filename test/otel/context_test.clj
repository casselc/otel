(ns otel.context-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.context :as ctx]))

(deftest root-context-is-empty
  (is (nil? (ctx/get-value ctx/root :anything))))

(deftest with-value-is-immutable
  (let [a (ctx/with-value ctx/root :k 1)
        b (ctx/with-value a :k 2)]
    (is (= 1 (ctx/get-value a :k)))
    (is (= 2 (ctx/get-value b :k)))
    (testing "the root is never mutated"
      (is (nil? (ctx/get-value ctx/root :k))))))

(deftest current-defaults-to-root
  (is (= ctx/root (ctx/current))))

(deftest with-context-installs-and-restores
  (let [c (ctx/with-value ctx/root :k :v)]
    (ctx/with-context c
      (is (= :v (ctx/get-value (ctx/current) :k))))
    (testing "the binding unwinds"
      (is (nil? (ctx/get-value (ctx/current) :k))))))

(deftest with-context-restores-on-throw
  (let [c (ctx/with-value ctx/root :k :v)]
    (is (thrown? Exception
                 (ctx/with-context c (throw (ex-info "boom" {})))))
    (is (nil? (ctx/get-value (ctx/current) :k)))))

(deftest with-context-nests
  (let [outer (ctx/with-value ctx/root :k :outer)
        inner (ctx/with-value outer :k :inner)]
    (ctx/with-context outer
      (is (= :outer (ctx/get-value (ctx/current) :k)))
      (ctx/with-context inner
        (is (= :inner (ctx/get-value (ctx/current) :k))))
      (testing "the outer context is restored after the inner unwinds"
        (is (= :outer (ctx/get-value (ctx/current) :k)))))))

(deftest bind-fn-carries-context-across-a-thread
  (testing "an explicitly bound fn sees the capturing thread's context"
    (let [c (ctx/with-value ctx/root :k :captured)
          f (ctx/with-context c (ctx/bind-fn (fn [] (ctx/get-value (ctx/current) :k))))
          seen (atom nil)
          t (Thread. (fn [] (reset! seen (f))))]
      (.start t)
      (.join t)
      (is (= :captured @seen)))))

(deftest unbound-fn-on-another-thread-sees-root
  (testing "without binding, a raw thread starts at the root context — this is the
            behavior bind-fn exists to correct"
    (let [c (ctx/with-value ctx/root :k :captured)
          seen (atom :unset)
          t (ctx/with-context c (Thread. (fn [] (reset! seen (ctx/get-value (ctx/current) :k)))))]
      (.start t)
      (.join t)
      (is (nil? @seen)))))

(deftest remove-value-clears-a-key
  (let [c (-> ctx/root (ctx/with-value :a 1) (ctx/with-value :b 2))
        d (ctx/remove-value c :a)]
    (is (nil? (ctx/get-value d :a)))
    (is (= 2 (ctx/get-value d :b)))))
