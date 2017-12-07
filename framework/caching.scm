(define (string->regex str)
  (string-translate* str '(("." . "\\.")
                           ("*" . "\\*")
                           ("?" . "\\?")
)))

(define uri-pat "<[^> ]+>")
(define str-pat "\\\"[^\"]+\\\"")

(define query-body-start-index
  (memoize
   (lambda (q)
     (irregex-match-start-index (irregex-search "SELECT|DELETE|INSERT" q)))))

(define query-body-end-index
  (memoize
   (lambda (q)
     (irregex-match-start-index (irregex-search "OFFSET|LIMIT|GROUP BY" q)))))

(define (cache-query-forms q)
  (let ((result (irregex-fold (format "(~A)|(~A)" uri-pat str-pat)  ; do <IRI> 123 "string" but not prefixed:IRI
                               (lambda (from-index match seed)
                                 (print (irregex-match-substring match) " - "  (irregex-match-names match))
                                 (let ((substr (irregex-match-substring match))
                                       (pattern (first seed))
                                       (form (second seed))
                                       (bindings (third seed)))
                                   (let* ((key* (cdr-when (assoc substr bindings)))
                                          (key (or key* (symbol->string (gensym 'uri)))))
                                     (list  (conc pattern
                                                  (string->regex (substring q from-index (irregex-match-start-index match)))
                                                  (if key*
                                                      (conc "\\k<" key ">")
                                                      (format "(?<~A>~A)" key 
                                                              (cond ((irregex-match uri-pat substr) uri-pat)
                                                                    ((irregex-match str-pat substr) str-pat)))))

                                            (conc form
                                                  (substring q from-index (irregex-match-start-index match))
                                                  "<" key ">")

                                           (if key* bindings 
                                               (alist-update substr key bindings))
                                           (irregex-match-end-index match)))))

                               '("" "" () end)
                               q
                               #f
                               (query-body-start-index q))))
    (values (conc
             (first result)
             (string->regex (substring q (fourth result))))
            (conc
             (second result)
             (string->regex (substring q (fourth result)))))))
            
(define (populate-cached-query-form pattern form q)
  (let* ((match (irregex-match pattern (substring q (query-body-start-index q)))))
    (and match
         (let ((matches (map (lambda (name-pair)
                               (let ((name (car name-pair))
                                     (index (cdr name-pair)))
                                 (cons (conc "<" (symbol->string name) ">")
                                       (irregex-match-substring match index))))
                             (irregex-match-names match))))
           (string-translate* form matches)))))

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



