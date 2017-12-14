(use srfi-69)

(define-syntax timed-let
  (syntax-rules ()
    ((_ label (let-exp (vars/vals ...) body ...))
     (let-values (((ut1 st1) (cpu-time)))
       (let ((t1 (current-milliseconds)))
         (let-exp (vars/vals ...)
           (let-values (((ut2 st2) (cpu-time)))
             (let ((t2 (current-milliseconds)))
               (debug-message "~%[~A] ~A Time: ~Ams / ~Ams / ~Ams~%" (logkey) label (- ut2 ut1) (- st2 st1) (- t2 t1))
               body ...))))))))

(define-syntax timed
  (syntax-rules ()
    ((_ label body ...)
     (let-values (((ut1 st1) (cpu-time)))
       (let ((t1 (current-milliseconds)))
         (let ((result body ...))
           (let-values (((ut2 st2) (cpu-time)))
             (let ((t2 (current-milliseconds)))
               (debug-message "~%[~A] ~A Time: ~Ams / ~Ams / ~Ams~%" (logkey) label (- ut2 ut1) (- st2 st1) (- t2 t1))
               result))))))))

(define-syntax timed-limit
  (syntax-rules ()
    ((_ limit label expression body ...)
     (let-values (((ut1 st1) (cpu-time)))
       (let ((t1 (current-milliseconds)))
         (let ((result body ...))
           (let-values (((ut2 st2) (cpu-time)))
             (let ((t2 (current-milliseconds)))
               (when (> (- ut2 ut1) limit)
                     (debug-message "~%[~A] Exceeded time limit for ~A: ~Ams / ~Ams / ~Ams~%~A~%~%"
                                    (logkey) label (- ut2 ut1) (- st2 st1) (- t2 t1)
                                    expression))
               result))))))))


(define *debug?* (make-parameter #t))

(define (debug-message str #!rest args)
  (when (*debug?*)
    (apply format (current-error-port) str args)))

(define *cache-forms?* (make-parameter #t))

(define *query-forms* (make-hash-table))

(define (query-cache-table query)
  (list (query-prefix query) ; this is slow!
        (call-if (*read-constraint*))
        (call-if (*write-constraint*))))

;; this is risky and approximate (eg, 'delete' in PREFIX uri)
;; (define query-body-start-index
;;   (memoize
;;    (lambda (q)
;;      (irregex-match-start-index (irregex-search (irregex "SELECT|DELETE|INSERT" 'i) q)))))

(define query-body-start-index
  (memoize
   (lambda (q)
     (irregex-match-end-index
      (irregex-search
       (irregex "[[:whitespace:]]*(?:(?:PREFIX|BASE) +[a-z]+: +<[^>]+>[[:whitespace:]]*)*" 's 'i)
       q)))))

(define query-prefix
  (memoize
   (lambda (q)
     (and (string? q) (substring q 0 (query-body-start-index q))))))

(define query-body-end-index
  (memoize
   (lambda (q)
     (and (string? q) (irregex-match-start-index (irregex-search (irregex "OFFSET|LIMIT|GROUP BY" 'i) q))))))

(define (regex-escape-string str)
  (string-translate* str '(("\\" . "\\\\")
                           ("$" . "\\$")
                           ("." . "\\.")
                           ("|" . "\\|")
                           ("+" . "\\|")
                           ("(" . "\\(")
                           (")" . "\\)")
                           ("[" . "\\[")
                           ("{" . "\\{")
                           ("*" . "\\*")
                           ("?" . "\\?")
                           ("^" . "\\^")
                           )))

(define uri-pat "<[^> ]+>")
(define str-pat "\\\"[^\"]+\\\"")
(define form-regex (irregex (format "(~A)|(~A)" uri-pat str-pat)))

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
                    (cond ((irregex-match (irregex uri-pat) substr) uri-pat)
                          ((irregex-match (irregex str-pat) substr) str-pat))))))

(define (make-query-pattern/form-fold query form?) ; this is also slow!
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
          (values (if form? query (regex-escape-string query))
                  '())))))

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
                       (cons
                        (list (irregex pattern) form form-prefix
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

(define (query-form-lookup query-string read-constraint write-constraint)
  (if (*cache-forms?*)
      (let ((forms (timed "table" (hash-table-ref/default *query-forms* 
                                                          (query-cache-table query-string)
                                                          '()))))
        (let loop ((forms forms))
          (if (null? forms) (values #f #f)
              (let ((pattern (first (car forms))))
                (let* ((subst (timed "subst" 
                                           (substring query-string (query-body-start-index query-string))))
                       (form-match (timed "match" (irregex-match pattern subst))))
                  (if form-match
                      (values form-match (car forms)) 
                      (loop (cdr forms))))))))
      (values #f #f)))

(define (apply-constraints-with-form-cache query-string
                                           #!optional
                                           (read-constraint (call-if (*read-constraint*)))
                                           (write-constraint (call-if (*write-constraint*))))
  (timed-let "Cache Lookup"
    (let-values (((form-match form-list) (query-form-lookup query-string read-constraint write-constraint)))
      (if form-match
          (timed-let "Populate Cache Form"
            (let-values (((rewritten-query-string annotations annotations-query-string 
                                                  deltas-query-string bindings update?)
                          (match form-list
                            ((pattern form form-prefix 
                                      annotations annotations-form annotations-prefix
                                      deltas-form deltas-prefix bindings update?)
                             (debug-message "~%[~A] Using cached form~%" (logkey)) ;; + logkey of cached form; otherwise log rw
                             (values (conc form-prefix (populate-cached-query-form pattern form form-match query-string))
                                     annotations
                                     (and annotations-form 
                                          (conc annotations-prefix
                                                (populate-cached-query-form pattern annotations-form form-match query-string)))
                                     (and deltas-form 
                                          (conc deltas-prefix
                                                (populate-cached-query-form pattern deltas-form form-match query-string)))
                                     bindings
                                     update?)))))
              (values rewritten-query-string 
                      annotations
                      annotations-query-string
                      deltas-query-string
                      bindings
                      update?)))

          (let ((query (parse-query query-string)))
            (timed-let "Rewrite"
              (let-values (((rewritten-query bindings) (apply-constraints query)))
                (let* ((update? (update-query? query))
                       (annotations (and (*calculate-annotations?*) 
                                         (handle-exceptions exn #f
                                                            (get-annotations rewritten-query bindings)))))
                  (let-values (((aquery annotations-pairs) (if annotations
                                                               (annotations-query annotations rewritten-query)
                                                               (values #f #f))))
                    (let* ((queried-annotations (and aquery 
                                                     (handle-exceptions exn 
                                                                        (begin (log-message "~%[~A]  ==Error Getting Queried Annotations==~%~A~%~%" (logkey) aquery)
                                                                               #f)
                                                                        (query-annotations aquery annotations-pairs))))
                           (deltas-query (and (*send-deltas?*) (notify-deltas-query rewritten-query)))
                           (rewritten-query-string (write-sparql rewritten-query))
                           (annotations-query-string (and aquery (write-sparql aquery)))
                           (deltas-query-string (and deltas-query (write-sparql deltas-query))))
                      (when (*cache-forms?*)
                            (timed "Save Cache Form"
                                   (query-form-save! query-string 
                                                     rewritten-query-string
                                                     annotations
                                                     annotations-query-string
                                                     deltas-query-string
                                                     bindings update?)))
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

(define-syntax unmemoize
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

;; (define apply-constraints-with-form-cache (memoize-save apply-constraints-with-form-cache))


