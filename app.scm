(use files matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client cjson
     memoize)

(import s-sparql mu-chicken-support)
(use s-sparql mu-chicken-support)

(require-extension sort-combinators)

(define (load-file file)
  (if (feature? 'docker)
      (load (make-pathname "/app" file))
      (load (make-pathname "./" file))))

(load-file "framework/settings.scm")

(load-file "framework/rw.scm") ; factor up to s-sparql

(load-file "framework/sparql-logic.scm")

(load-file "framework/annotations.scm")  

(load-file "framework/utility-transformations.scm")

(load-file "framework/temporary-graph.scm")

(load-file "framework/optimization.scm")

(load-file "framework/main-transformation.scm")

(load-file "framework/constraint-renaming.scm")

(load-file "framework/constraint-renaming-dependencies.scm")

(load-file "framework/constraint.scm")

(load-file "framework/instantiation.scm")

(load-file "framework/deltas.scm")

(load-file "framework/caching.scm")

(load-file "framework/call-specification.scm")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load
(log-message "~%==Query Rewriter Service==")

(when (*plugin*) (load-plugin (*plugin*)))

(log-message "~%Proxying to SPARQL endpoint: ~A " (*sparql-endpoint*))
(log-message "~%and SPARQL update endpoint: ~A " (*sparql-update-endpoint*))

(*port* 8890)

(http-line-limit #f)
