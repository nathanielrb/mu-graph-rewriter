(define *subscribers-file*
  (config-param "SUBSCRIBERSFILE" 
                (if (feature? 'docker)
                    "/config/subscribers.json"
                    "./config/rewriter/subscribers.json")))

(define *subscribers*
  (handle-exceptions exn '()
    (vector->list
     (alist-ref 'potentials
                (with-input-from-file (*subscribers-file*)
                  (lambda () (read-json)))))))

(define *unique-variables* (make-parameter '()))

(define *functional-properties* (make-parameter '()))

(define *queried-properties* (make-parameter '()))

(define *query-functional-properties?* (make-parameter #t))

;; Can be a string, an s-sparql expression, 
;; or a thunk returning a string or an s-sparql expression.
;; Used by (constrain-triple) below.
(define *read-constraint*
  (make-parameter
   `(@QueryUnit
     (@Query
      (CONSTRUCT (?s ?p ?o))
      (WHERE (GRAPH ,(*default-graph*) (?s ?p ?o)))))))

(define *write-constraint*
  (make-parameter
   `(@QueryUnit
      (@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE (GRAPH ,(*default-graph*) (?s ?p ?o)))))))
 
(define *plugin*
  (config-param "PLUGIN" ))

(define *plugin-dir*
  (config-param "PLUGIN_DIR" 
                (if (feature? 'docker)
                    "/config" 
                    "./config/rewriter" )))

(define *cache* (make-hash-table))

(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define (preserve-graphs?)
  (or (header 'preserve-graph-statements)
      ;; (($query) 'preserve-graph-statements)
      (not (*rewrite-graph-statements?*))))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #t))

(define *graphs* (make-parameter #f))

(define (rewrite-select?)
  (or (equal? "true" (header 'rewrite-select-queries))
      ;; (equal? "true" (($query) 'rewrite-select-queries))
      (*rewrite-select-queries?*)))

(define *send-deltas?* 
  (config-param "SEND_DELTAS" #f))

(define *calculate-annotations?* 
  (config-param "CALCULATE_ANNOTATIONS" #t))

(define *calculate-potentials?* 
  (config-param "CALCULATE_POTENTIAL_GRAPHS" #f))
