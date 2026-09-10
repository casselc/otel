(ns otel.attribute-schema.main
  "Print a deterministic attribute-schema resource from explicit source files."
  (:require [otel.attribute-schema :as schema]))

(defn generate [root files]
  (schema/analyze-sources
   (mapv (fn [file]
           ;; Validate before constructing or reading a filesystem path.
           (let [file (schema/canonical-source file)]
             (schema/read-forms file (slurp (str root "/" file)))))
         files)))

(defn -main [& args]
  (let [[root files] (if (= "--root" (first args))
                       [(second args) (drop 2 args)]
                       ["." args])]
    (when (empty? files)
      (throw (ex-info
              "usage: -m otel.attribute-schema.main [--root DIR] SOURCE..."
              {:type ::usage})))
    (print (schema/render (generate root files)))))
