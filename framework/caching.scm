(use srfi-69)

(define *query-forms* (make-hash-table))

(define (query-cache-table query)
  (list (query-prefix query)
        (call-if (*read-constraint*))
        (call-if (*write-constraint*))))

(define query-body-start-index
  (memoize
   (lambda (q)
     (irregex-match-start-index (irregex-search "SELECT|DELETE|INSERT" q)))))

(define query-prefix
  (memoize
   (lambda (q)
     (and (string? q) (substring q 0 (query-body-start-index q))))))

(define query-body-end-index
  (memoize
   (lambda (q)
     (and (string? q) (irregex-match-start-index (irregex-search "OFFSET|LIMIT|GROUP BY" q))))))

(define (regex-escape-string str)
  (string-translate* str '(("." . "\\.")
                           ("*" . "\\*")
                           ("?" . "\\?")
                           )))

(define uri-pat "<[^> ]+>")
(define str-pat "\\\"[^\"]+\\\"")
(define form-regex (format "(~A)|(~A)" uri-pat str-pat))

(define (make-query-pattern/form-fold-form pattern query from-index match substr key* key)
  (conc pattern
        (substring query from-index (irregex-match-start-index match))
        (if (string=? key substr) substr
            (conc "<" key ">"))))

(define (make-query-pattern/form-fold-pattern pattern query from-index match substr key* key)
  (conc pattern
        (regex-escape-string (substring query from-index (irregex-match-start-index match)))
        (if key*
            (conc "\\k<" key ">")
            (format "(?<~A>~A)" key 
                    (cond ((irregex-match uri-pat substr) uri-pat)
                          ((irregex-match str-pat substr) str-pat))))))

(define (make-query-pattern/form-fold query form?)
  (lambda (from-index form-match seed)
    (match seed
      ((pattern bindings end-index n)
       (let* ((substr (irregex-match-substring form-match))
              (key* (cdr-when (assoc substr bindings)))
              (key (or key*
                       (and (not form?) (conc "uri" (number->string n)))
                       substr)))
         (list (if form?
                   (make-query-pattern/form-fold-form pattern query from-index form-match substr key* key)
                   (make-query-pattern/form-fold-pattern pattern query from-index form-match substr key* key))
               (if (or key* form?)
                   bindings 
                   (alist-update substr key bindings))
               (irregex-match-end-index form-match)
               (+ n 1)))))))

(define (make-query-pattern/form query #!optional form? (bindings '()))
  (match (irregex-fold form-regex (make-query-pattern/form-fold query form?)
                       `("" ,bindings #f 0) query #f
                       (query-body-start-index query))
    ((pattern bindings end-index _)
     (if end-index
         (values (conc pattern
                       (if form? (substring query end-index)
                           (regex-escape-string (substring query end-index))))
                 bindings)
          (values query '())))))

(define (make-query-form query bindings)
  (make-query-pattern/form query #t bindings))

(define (make-query-pattern query)
  (make-query-pattern/form query))

(define (query-form-save! query-string rewritten-query annotations annotations-query-string deltas-query-string bindings update?)
  (let-values (((pattern form-bindings) (make-query-pattern query-string)))
    (let* ((prefix (query-prefix query-string))
           (make-form (lambda (q) (and q (make-query-form q form-bindings))))
           (form (make-form rewritten-query))
           (form-prefix (query-prefix rewritten-query))
           (annotations-form (make-form annotations-query-string))
           (annotations-form-prefix (query-prefix annotations-query-string))
           (deltas-form (make-form deltas-query-string))
           (deltas-form-prefix (query-prefix deltas-query-string))
           (table (query-cache-table query-string)))
      (hash-table-set! *query-forms*
                       table
                       (cons (list pattern form form-prefix
                                   annotations annotations-form annotations-form-prefix
                                   deltas-form deltas-form-prefix
                                   bindings update?)
                             (hash-table-ref/default *query-forms* table '()))))))

(define (populate-cached-query-form pattern form match query-string)
  (let ((matches (map (lambda (name-pair)
                        (let ((name (car name-pair))
                              (index (cdr name-pair)))
                                 (cons (conc "<" (symbol->string name) ">")
                                       (irregex-match-substring match index))))
                      (irregex-match-names match))))
    (string-translate* form matches)))

(define (query-form-lookup query-string)
  (let ((forms (hash-table-ref/default *query-forms* 
                                       (query-cache-table query-string)
                                       '())))
    (let loop ((forms forms))
      (if (null? forms) (values #f #f)
          (let ((pattern (first (car forms))))
            (let ((form-match (irregex-match pattern (substring query-string (query-body-start-index query-string)))))
              (if form-match (values form-match (car forms)) 
                  (loop (cdr forms)))))))))

(define (apply-constraints-with-form-cache logkey query-string)
  (let-values (((form-match form-list) (query-form-lookup query-string)))
    (if form-match
        (match form-list
          ((pattern form form-prefix 
                    annotations annotations-form annotations-prefix
                    deltas-form deltas-prefix bindings update?)
           (log-message "~%Using cached form~%")
          (values (conc form-prefix (populate-cached-query-form pattern form form-match query-string))
                  annotations
                  (and annotations-form 
                       (conc annotations-prefix
                             (populate-cached-query-form pattern annotations-form form-match query-string)))
                  (and deltas-form 
                       (conc deltas-prefix
                             (populate-cached-query-form pattern deltas-form form-match query-string)))
                  bindings
                  update?)))
        (let ((query (parse-query query-string)))
          (let-values (((ut1 st1) (cpu-time)))
            (let-values (((rewritten-query bindings) (apply-constraints query)))
              (let-values (((ut2 st2) (cpu-time)))
                (log-message "~%==Rewrite Time (~A)==~%~Ams / ~Ams~%" logkey (- ut2 ut1) (- st2 st1))
                (let* ((update? (update-query? query))
                       (annotations (and (*calculate-annotations?*) (get-annotations rewritten-query bindings))))
                  (let-values (((aquery annotations-pairs) (if annotations  (annotations-query annotations query)
                                                               (values #f #f))))
                    (let ((queried-annotations (and aquery (query-annotations aquery annotations-pairs)))
                          (deltas-query (and (*send-deltas?*) (notify-deltas-query rewritten-query)))
                          (rewritten-query-string (write-sparql rewritten-query))
                          (annotations-query-string (and aquery (write-sparql aquery)))
                          (deltas-query-string (and deltas-query (write-sparql deltas-query))))
                  (query-form-save! query-string 
                                    rewritten-query-string
                                    annotations
                                    annotations-query-string
                                    deltas-query-string
                                    bindings update?)
                  (values rewritten-query-string 
                          annotations
                          annotations-query-string
                          deltas-query-string
                          bindings
                          update?
                          )))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; memoization

;; (define keys (memoize keys*))

;;(define renaming (memoize renaming*))

(define-syntax memoize-save
  (syntax-rules ()
    ((_ proc)
     (begin
       (print (quote proc))
       (put! (quote proc) 'memoized proc)
       (memoize proc)))))

(define-syntax rememoize
  (syntax-rules ()
    ((_ proc)
     (memoize (get (quote proc) 'memoized)))))

(define-syntax unmemoized
  (syntax-rules ()
    ((_ proc)
     (or (get (quote proc) 'memoized)
         proc))))

(define unique-variable-substitutions (memoize-save unique-variable-substitutions))

(define parse-query (memoize-save parse-query))

(define rdf-equal? (memoize-save rdf-equal?)) ; needs to take namespaces as param as well

(define get-constraint-prefixes (memoize-save get-constraint-prefixes))

(define parse-constraint (memoize-save parse-constraint))

(define replace-headers (memoize-save replace-headers))

(define get-dependencies (memoize-save get-dependencies))

;; (define apply-constraints (memoize-save apply-constraints))

(define apply-constraints-with-form-cache (memoize-save apply-constraints-with-form-cache))


