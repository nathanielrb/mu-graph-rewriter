(use srfi-69 srfi-18 abnf lexgen)
(require-extension mailbox)

(define *debug?* (make-parameter #t))

(define (debug-message str #!rest args)
  (when (*debug?*)
    (apply format (current-error-port) str args)))

(define *cache-forms?* (make-parameter #t))

(define *query-forms* (make-hash-table))


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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Quick Splitting of Prologue/Body by Parsing
(define ws*
  (repetition
   (alternatives char-list/wsp char-list/crlf char-list/lf char-list/cr)))

(define PrefixDecl*
  (concatenation
   ws* (char-list/:s "PREFIX") ws* PNAME_NS ws* IRIREF ws*))

(define BaseDecl*
  (concatenation
   ws* (char-list/:s "BASE") ws* IRIREF ws*))

(define Prologue*
  (repetition
   (alternatives BaseDecl* PrefixDecl*)))

(define split-query-prefix
  (memoize
   (lambda (q)
     (match-let (((prefix body) 
                  (map list->string
                       (lex Prologue* error q))))
       (values prefix body)))))

(define get-query-prefix 
  (memoize
   (lambda (q)
     (timed "prefix"
     (let-values (((prefix body) (split-query-prefix q)))
       prefix)))))

(define get-query-body 
  (memoize
   (lambda (q)
     (let-values (((prefix body) (split-query-prefix q)))
       body))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; REST
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
(define uri-regex (irregex uri-pat))
(define str-pat "\\\"[^\"]+\\\"")
(define str-regex (irregex str-pat))
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
            (conc "(?<" key ">"
                  (let ((c (substring substr 0 1)))
                    (cond ((equal? c "<") uri-pat)
                          ((equal? c "\"") str-pat)))
                  ")"))))                 

(define (make-query-pattern/form-fold query-body form?)
  (lambda (from-index form-match seed)
    (match seed
      ((pattern bindings end-index n)
       (let* ((substr (irregex-match-substring form-match))
              (key* (cdr-when (assoc substr bindings)))
              (key (or key*
                       (if form? substr
                           (conc "uri" (number->string n))))))
         (list ((if form?
                    make-query-pattern/form-fold-form
                    make-query-pattern/form-fold-pattern)
                pattern query-body from-index form-match substr key* key)
               (if (or key* form?)
                   bindings 
                   (alist-update substr key bindings))
               (irregex-match-end-index form-match)
               (+ n 1)))))))

(define (make-query-pattern/form query #!optional form? (bindings '()))
  (let ((query-body (get-query-body query)))
    (match (irregex-fold form-regex
                         (make-query-pattern/form-fold query-body form?)
                         `("" ,bindings #f 0) 
                         query-body)
     ((pattern bindings end-index _)
      (if end-index
          (values (conc pattern
                        (if form?
                            (substring query-body end-index)
                            (regex-escape-string (substring query-body end-index))))
                  bindings)
          (values (if form? query-body (regex-escape-string query-body))
                  '()))))))

(define (make-query-form query bindings)
  (make-query-pattern/form query #t bindings))

(define make-query-pattern
  (memoize 
   (lambda (query)
     (make-query-pattern/form query))))

(define (query-cache-key query)
  (list (get-query-prefix query)
        (make-query-pattern query)  
        (call-if (*read-constraint*))
        (call-if (*write-constraint*))))

(define *cache-mailbox* (make-mailbox))

(define cache-save-daemon
  (make-thread
    (lambda ()
      (let loop ()
        (let ((thunk (mailbox-receive! *cache-mailbox*)))
          (handle-exceptions exn
                             (debug-message "[~A] Error saving cache forms%" (logkey))
           (thunk)
           (debug-message "[~A] Saved cache form" (logkey))))
        (loop) ) ) ))

(thread-start! cache-save-daemon)

(define (enqueue-cache-action! thunk)
  (mailbox-send! *cache-mailbox* thunk))

(define (query-form-save! query-string rewritten-query annotations annotations-query-string deltas-query-string bindings update? key)
  (let-values (((pattern form-bindings) (make-query-pattern query-string)))
    (let* ((prefix (get-query-prefix query-string))  
           (make-form (lambda (q) (and q (timed "make-form" (make-query-form q form-bindings)))))
           (form (make-form rewritten-query))
           (form-prefix (get-query-prefix rewritten-query))
           (annotations-form (and annotations-query-string (make-form annotations-query-string)))
           (annotations-form-prefix (and annotations-query-string (get-query-prefix annotations-query-string)))
           (deltas-form (and deltas-query-string (make-form deltas-query-string)))
           (deltas-form-prefix (and deltas-query-string (get-query-prefix deltas-query-string))))

      (hash-table-set! *query-forms*
                       (query-cache-key query-string)
                        (list (irregex pattern) form form-prefix
                              annotations annotations-form annotations-form-prefix
                              deltas-form deltas-form-prefix
                              bindings update? key)))))

(define (populate-cached-query-form pattern form match query-string)
  (let ((matches (map (lambda (name-pair)
                        (let ((name (car name-pair))
                              (index (cdr name-pair)))
                                 (cons (conc "<" (symbol->string name) ">")
                                       (irregex-match-substring match index))))
                      (irregex-match-names match))))
    (string-translate* form matches)))

(define (query-form-lookup query-string)
  (if (*cache-forms?*)
      (let ((cached-form (timed "tableget" (hash-table-ref/default *query-forms* (query-cache-key query-string) #f))))
        ;; (print "==looked and got==\n" (and cached-form (length cached-form)
        ;;                                    (irregex-match (first cached-form) (get-query-body query-string))))
        (if cached-form
            (let ((pattern-regex (first cached-form)))
              (values (timed "match" (irregex-match pattern-regex (get-query-body query-string)))
                      cached-form))
            (values #f #f)))
      (values #f #f)))

(define (apply-constraints-with-form-cache query-string
                                           #!optional
                                           (read-constraint (call-if (*read-constraint*)))
                                           (write-constraint (call-if (*write-constraint*))))
  (timed-let "Cache Lookup"
    (let-values (((form-match form-list) (query-form-lookup query-string)))
      ;; (print "==looked up and got==\n" form-match "\n" form-list)
      (if form-match
          (match form-list
            ((pattern form form-prefix annotations annotations-form annotations-prefix
                      deltas-form deltas-prefix bindings update? cached-logkey)
             (log-message "~%[~A] Using cached form of ~A~%" (logkey) cached-logkey)
             (values (replace-headers
                      (conc form-prefix
                            (populate-cached-query-form pattern form form-match query-string)))
                     annotations
                     (and annotations-form 
                          (replace-headers
                           (conc annotations-prefix
                                 (populate-cached-query-form pattern annotations-form form-match query-string))))
                     (and deltas-form 
                          (replace-headers
                           (conc deltas-prefix
                                 (populate-cached-query-form pattern deltas-form form-match query-string))))
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

                      (log-message "~%[~A]  ==Rewritten Query==~%~A~%" (logkey) rewritten-query-string)

                      (when (*cache-forms?*)
                            (let ((key (logkey)) (rc (*write-constraint*)) (wc (*write-constraint*)))
                              (debug-message "[~A] Saving cache form~%" (logkey))
                              (enqueue-cache-action!
                               (lambda ()
                                 (parameterize ((logkey key)
                                                (*read-constraint* rc)
                                                (*write-constraint* wc))
                                  (if (timed "relook-up" (query-form-lookup query-string))
                                      (begin (debug-message "Already cached~%") #f)
                                      (timed "Save Cache Form"
                                             (query-form-save! query-string 
                                                               rewritten-query-string
                                                               annotations
                                                               annotations-query-string
                                                               deltas-query-string
                                                               bindings update? key))))))))
                      (values (replace-headers rewritten-query-string)
                              annotations
                              (and annotations-query-string (replace-headers annotations-query-string))
                              (and deltas-query-string (replace-headers deltas-query-string))
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


