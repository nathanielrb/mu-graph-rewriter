(use srfi-69)

(define *query-forms* (make-hash-table))

(define query-body-start-index
  (memoize
   (lambda (q)
     (irregex-match-start-index (irregex-search "SELECT|DELETE|INSERT" q)))))

(define query-prefix
  (memoize
   (lambda (q)
     (substring q 0 (query-body-start-index q)))))

(define query-body-end-index
  (memoize
   (lambda (q)
     (irregex-match-start-index (irregex-search "OFFSET|LIMIT|GROUP BY" q)))))

(define (query-form-lookup query)
  (let ((forms (hash-table-ref/default *query-forms* (query-prefix query) '())))
    (let loop ((forms forms))
      (if (null? forms) #f
          (let ((pattern (first (car forms)))
                (form (second (car forms)))
                (form-prefix (third (car forms))))
            (let ((populated-form (populate-cached-query-form pattern form query)))
              (if populated-form (conc form-prefix populated-form)
                  (loop (cdr forms)))))))))

(define (query-form-save! query rewritten-query)
  (let ((prefix (query-prefix query)))
    (let-values (((pattern bindings) (make-query-pattern query)))
      (let ((form (make-query-form rewritten-query bindings))
            (form-prefix (query-prefix rewritten-query)))
        (hash-table-set! *query-forms* prefix
                         (cons (list pattern form form-prefix)
                               (hash-table-ref/default *query-forms* prefix '())))))))

(define (apply-constraints-with-form-cache query)
  (or (query-form-lookup query)
      (let-values (((rewritten-query bindings) (apply-constraints (parse-query query))))
        (let ((rewritten-query-string (write-sparql rewritten-query)))
          ;; deltas
          ;; annotations
          ;; ...
          (log-message "~%==Reworte==~%~A~%" rewritten-query-string)
          (query-form-save! query rewritten-query-string)
          rewritten-query-string ))))

(define (populate-cached-query-form pattern form q)
  (log-message "~%==FORM==~%~A~%" form)
  (let* ((match (irregex-match pattern (substring q (query-body-start-index q)))))
    (log-message "~%~%==Match==~%~%~A" match)
    (and match
         (let ((matches (map (lambda (name-pair)
                               (let ((name (car name-pair))
                                     (index (cdr name-pair)))
                                 (cons (conc "<" (symbol->string name) ">")
                                       (irregex-match-substring match index))))
                             (irregex-match-names match))))
           (log-message "~%~%==Matches==~%~%~A" matches)
           (string-translate* form matches)))))

(define (string->regex str)
  (string-translate* str '(("." . "\\.")
                           ("*" . "\\*")
                           ("?" . "\\?")
)))

(define uri-pat "<[^> ]+>")
(define str-pat "\\\"[^\"]+\\\"")

(define (make-query-pattern q)
  (let ((result (irregex-fold (format "(~A)|(~A)" uri-pat str-pat)  ; do <IRI> 123 "string" but not prefixed:IRI
                               (lambda (from-index match seed)
                                 (let ((substr (irregex-match-substring match))
                                       (n (fourth seed))
                                       (pattern (first seed))
                                       (bindings (second seed)))
                                   (let* ((key* (cdr-when (assoc substr bindings)))
                                          (key (or key* (conc "uri" (number->string n)))))
                                     (list (conc pattern
                                                  (string->regex (substring q from-index (irregex-match-start-index match)))
                                                  (if key*
                                                      (conc "\\k<" key ">")
                                                      (format "(?<~A>~A)" key 
                                                              (cond ((irregex-match uri-pat substr) uri-pat)
                                                                    ((irregex-match str-pat substr) str-pat)))))

                                           (if key* bindings 
                                               (alist-update substr key bindings))
                                           (irregex-match-end-index match)
                                           (+ n 1)))))
                               '("" () #f 0)
                               q
                               #f
                               (query-body-start-index q))))
    (if (third result)
        (values (conc
               (first result)
               (string->regex (substring q (third result))))
                (second result)) ; bindings
        (values q '()))))

(define (make-query-form q bindings)
  (let ((result (irregex-fold (format "(~A)|(~A)" uri-pat str-pat)  ; do <IRI> 123 "string" but not prefixed:IRI
                               (lambda (from-index match seed)
                                 (let ((substr (irregex-match-substring match))
                                       (n (fourth seed))
                                       (form (first seed))
                                       (bindings (second seed)))
                                   (let* ((key* (cdr-when (assoc substr bindings)))
                                          (key (or key* (conc "uri" (number->string n)))))
                                     (list  (conc form
                                                  (substring q from-index (irregex-match-start-index match))
                                                  "<" key ">")

                                           (if key* bindings 
                                               (alist-update substr key bindings))
                                           (irregex-match-end-index match)
                                           (+ n 1)))))

                               `("" ,bindings #f 0)
                               q
                               #f
                               (query-body-start-index q))))
    (if (third result)
        (conc
         (first result)
         (substring q (third result)))
        q)))
            
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; memoization

;; (define keys (memoize keys*))

;;(define renaming (memoize renaming*))

(define unique-variable-substitutions (memoize  unique-variable-substitutions))

(define parse-query (memoize parse-query))

(define rdf-equal? (memoize rdf-equal?)) ; needs to take namespaces as param as well

(define get-constraint-prefixes (memoize get-constraint-prefixes))

(define apply-constraints (memoize apply-constraints))

(define parse-constraint* (memoize parse-constraint*))

(define get-dependencies (memoize get-dependencies))



