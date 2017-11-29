;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implentation
(define (virtuoso-error exn)
  (if (or ((condition-predicate 'client-error) exn)
          ((condition-predicate 'server-error) exn))
      (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                          ((condition-property-accessor 'server-error 'response) exn)))
            (body (or ((condition-property-accessor 'client-error 'body) exn)
                      ((condition-property-accessor 'server-error 'body) exn))))
        (log-message "Virtuoso error: ~A" exn)
        (log-message "~A~%" (condition->list exn))
        (when body
              (log-message "~%==Virtuoso Error==~% ~A ~%" body))
        (when response
              (log-message "~%==Reason==:~%~A~%" (response-reason response)))
        (abort 'virtuoso-error))
      (abort exn)))

(define (rewriter-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (log-message "~%" exn)
    (when body
      (log-message "~%==Rewriter Error==~% ~A ~%" body))
    (when response
      (log-message "~%==Reason==:~%~A~%" (response-reason response)))
    (abort exn)))

;; better : parameterize body and req-headers, and define functions...
(define $query (make-parameter (lambda (q) #f)))
(define $body (make-parameter (lambda (q) #f)))
(define $mu-session-id (make-parameter #f))
(define $mu-call-id (make-parameter #f))

(define (proxy-query key rewritten-query-string endpoint)
  (handle-exceptions exn
                     (virtuoso-error exn)
    (let ((t1 (current-milliseconds)))
      (let-values (((result uri response)
                    (with-input-from-request 
                     (make-request method: 'POST
                                   uri: (uri-reference endpoint)
                                   headers: (headers
                                             '((Content-Type application/x-www-form-urlencoded)
                                               (Accept application/sparql-results+json)))) 
                     `((query . , (format #f "~A" rewritten-query-string)))
                     read-string)))
        
        (log-message "~%==Query Time (~A)==~%~Ams~%" key (- (- t1 (current-milliseconds))))
        (values result uri response)))))

(define parse-query* (memoize parse-query))

(define (parse key q)
  (let-values (((ut1 st1) (cpu-time)))
    (let ((result (parse-query* q)))
      (let-values (((ut2 st2) (cpu-time)))
        (log-message "~%==Parse Time (~A)==~%~Ams / ~Ams~%" 
                     key (- ut2 ut1) (- st2 st1))
        result))))

(define (log-headers) (log-message "~%==Received Headers==~%~A~%" (*request-headers*)))

(define (log-received-query query-string query)
  (log-headers)
  (log-message "~%==Rewriting Query==~%~A~%" query-string)
  ;; (log-message "~%==With Constraint==~%~A~%" (write-sparql (if (procedure? (*constraint*)) ((*constraint*)) (*constraint*))))
  (log-message "~%==Parsed As==~%~A~%" (write-sparql query)))

(define (call-if C) (if (procedure? C) (C) C))


(define-syntax time
  (syntax-rules ()
    ((_ key body ...)
     (let-values (((ut1 st1) (cpu-time)))
       (let-values (((result bindings) body ...))
         (let-values (((ut2 st2) (cpu-time)))
           (log-message "~%==Rewrite Time (~A)==~%~Ams / ~Ams~%" key (- ut2 ut1) (- st2 st1))
           (values result bindings)))))))

(define (rewrite-call)
  (lambda (_)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (logkey (gensym 'query))
         (query (parse logkey query-string)))

    (log-headers)
    (log-message "~%==Rewriting Query (~A)==~%~A~%" logkey query-string)
    (log-message "~%==Parsed As (~A)==~%~A~%" logkey (write-sparql query))

    (let-values (((rewritten-query bindings)
                  (parameterize (($query $$query) ($body $$body))
                    (handle-exceptions exn 
                        (begin (log-message "~%==Rewriting Error (~A)==~%" logkey) 
                               (log-message "~%~A~%" ((condition-property-accessor 'exn 'message) exn))
                               (print-error-message exn (current-error-port))
                               (print-call-chain (current-error-port))
                               (abort exn))
                        
                        (time logkey (rewrite-constraints query))))))
      (let ((rewritten-query-string (write-sparql rewritten-query)))
        
        (log-message "~%==Rewritten Query (~A)==~%~A~%" logkey rewritten-query-string)

        (handle-exceptions exn 
            (virtuoso-error exn)

          (when (and (update-query? rewritten-query) (*send-deltas?*))
            (notify-deltas rewritten-query))

          (plet-if (not (update-query? query))
                   ((potential-graphs (handle-exceptions exn
                                          (begin (log-message "~%Error getting potential graphs or annotations (~A): ~A~%" 
                                                              logkey exn) 
                                                 
                                                 #f)
                                        (cond ((*calculate-potentials?*)
                                               (get-all-graphs rewritten-query))
                                              ((*calculate-annotations?*)
                                               (get-annotations rewritten-query bindings))
                                              (else #f))))
                   ((result response)
                    (let-values (((result uri response)
                                  (proxy-query logkey
                                               ;;(add-prefixes rewritten-query-string)
                                               rewritten-query-string
                                               (if (update-query? query)
                                                   (*sparql-update-endpoint*)
                                                   (*sparql-endpoint*)))))
                      (close-connection! uri)
                      (list result response))))
                   
              (when (or (*calculate-potentials?*) (*calculate-annotations?*))
                    (log-message "~%==Potentials (~A)==~%(Will be sent in headers)~%~A~%"  logkey potential-graphs))
              
              (let ((headers (headers->list (response-headers response))))
                (log-message "~%==Results (~A)==~%~A~%" 
                             logkey (substring result 0 (min 1500 (string-length result))))
                (mu-headers headers)
                result))))))))

        

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification
(define-rest-call 'GET '("sparql") (rewrite-call))
(define-rest-call 'POST '("sparql") (rewrite-call))

(define-namespace rewriter "http://mu.semte.ch/graphs/")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Test suite
;; prefix/namespace problems!
(define (construct-intermediate-graph-rules graph)
  `(((@QueryUnit @Query)  . ,rw/quads)
    ((CONSTRUCT)
     . ,(lambda (block bindings)
          (values `((INSERT
                     (GRAPH ,graph ,@(cdr block))))
                  bindings)))
    (,values . ,rw/copy)))
    
;; could be abstracted & combined with select-query-rules
(define (replace-dataset-rules graph)
  `(((@QueryUnit @Query) . ,rw/quads)
    ((GRAPH)
     . ,(rw/lambda (block) (cddr block)))
    ((@Prologue @SubSelect WHERE FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
    ((@Dataset) 
     . ,(rw/lambda (block)
          (parameterize ((*default-graph* graph)) ; not used
            `((@Dataset ,@(make-dataset 'FROM (list graph) #f))))))
    (,select? . ,rw/copy)
    (,triple? . ,rw/copy)
    (,list? . ,rw/list)))

(define (test query #!optional (cleanup? #t))
  (let ((rewritten-query (rewrite-query query (top-rules)))
        (intermediate-graph 
         (expand-namespace
          (symbol-append '|rewriter:| (gensym 'graph)))))
    (log-message "Creating intermediate graph: ~A " intermediate-graph)
    (print
     (sparql-update
      (write-sparql 
       (rewrite-query 
        (if (procedure? (*read-constraint*)) ((*read-constraint*)) (*read-constraint*))
        (construct-intermediate-graph-rules intermediate-graph)))))

    (parameterize ((*query-unpacker* sparql-bindings))
      (let ((r1 (sparql-select (write-sparql rewritten-query)))
            (r2 (sparql-select (write-sparql (rewrite-query query (replace-dataset-rules intermediate-graph))))))
        (log-message "~%==Expected Results==~%~A" r2)
        (log-message "~%==Actual Results==~%~A" r1)
        (when cleanup?
          (sparql-update (format "DELETE WHERE { GRAPH ~A { ?s ?p ?o } }" intermediate-graph)))
        (values r1 r2)))))

(define (test-call _)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (cleanup? (not (string-ci=? "false" (or ($$query 'cleanup) "true"))))
         (query (parse "test" query-string)))
    (let-values (((actual expected) (test query cleanup?)))
      `((expected . ,(list->vector expected))
        (actual . ,(list->vector actual))
        (equal . ,(equal? expected actual))))))

(define-rest-call 'POST '("test") test-call)

(define (sandbox-call _)
  (let* ((body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string ($$body 'query))
         (session-id (conc "\"" ($$body 'session-id) "\""))
         (read-constraint-string ($$body 'readconstraint))
	 (write-constraint-string ($$body 'writeconstraint))
	 (read-constraint (parse-constraint read-constraint-string))
	 (write-constraint (parse-constraint write-constraint-string))
         (query (parse "sandbox" query-string))
	 (fprops (map string->symbol
		      (string-split (or ($$body 'fprops) "") ", ")))
	 (qprops (map string->symbol
		      (string-split (or ($$body 'qprops) "") ", ")))  
         (query-fprops? (equal? "true" ($$body 'query-fprops)))
         (unique-vars (map string->symbol
                              (string-split (or ($$body 'uvs) "") ", "))))
    (parameterize ((*write-constraint* write-constraint)
    		   (*read-constraint* read-constraint)
		   (*functional-properties* fprops)
                   (*queried-properties* qprops)
                   (*unique-variables* unique-vars)
                   (*query-functional-properties?* query-fprops?))
      (let-values (((rewritten-query bindings) (rewrite-constraints query)))
        (let* ((annotations (get-annotations rewritten-query bindings))
               (qt-annotations (and annotations (query-time-annotations annotations)))
              (queried-annotations (and annotations (query-annotations annotations rewritten-query)))
              (functional-property-substitutions (get-binding/default 'functional-property-substitutions bindings '())))
        (log-message "~%===Annotations===~%~A~%" annotations)
        (log-message "~%===Queried Annotations===~%~A~%"
                     (format-queried-annotations queried-annotations))
        `((rewrittenQuery . ,(format (write-sparql rewritten-query)))
          (annotations . ,(format-annotations qt-annotations))
          (queriedAnnotations . ,(format-queried-annotations queried-annotations))))))))

(define (apply-call _)
  (let* ((body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
	 (read-constraint-string ($$body 'readconstraint))
	 (write-constraint-string ($$body 'writeconstraint))
	 (fprops (map string->symbol
		      (string-split (or ($$body 'fprops) "") ", ")))
	 (qprops (map string->symbol
		      (string-split (or ($$body 'qprops) "") ", ")))
         (query-fprops? (equal? "true" ($$body 'query-fprops)))
         (unique-vars (map string->symbol
                              (string-split (or ($$body 'uvs) "") ", "))))

    (log-message "~%Redefining read and write constraints~%")

    ;; brute-force redefining constraints
    (set!
      *write-constraint*
      (make-parameter
       ;; (with-session-id write-constraint-string)))
       (lambda () (parse-constraint write-constraint-string))))

    (set!
      *read-constraint*
      (make-parameter
       ;; (with-session-id write-constraint-string)))
       (lambda () (parse-constraint read-constraint-string))))

    (set! *functional-properties* (make-parameter fprops))
    (set! *queried-properties* (make-parameter qprops))
    (set! *unique-variables* (make-parameter unique-vars))
    (set! *query-functional-properties?* (make-parameter query-fprops?))
    `((success .  "true"))))

(define (format-queried-annotations queried-annotations)
  (list->vector
   (map (lambda (annotation)
          (match annotation
            ((key val)
             `((key . ,(symbol->string key))
               (var . ,(write-uri val))))
            (key `((key . ,(symbol->string key))))))
        queried-annotations)))

(define (format-annotations annotations)
  (list->vector
   (map (lambda (annotation)
          (match annotation
            ((`*values* rest) `((key . ,(format "~A" rest)))) ; fudging
            ((key var) `((key . ,(symbol->string key))
                         (var . ,(symbol->string var))))
            (key `((key . ,(symbol->string key))))))
        annotations)))

(define-rest-call 'POST '("sandbox") sandbox-call)

(define-rest-call 'POST '("apply") apply-call)

(define-rest-call 'GET '("auth")
  (lambda (_)
    (let* ((session (header 'mu-session-id))
           (results (sparql-select-unique 
                     "SELECT ?user WHERE { <~A> mu:account ?user }" 
                     session))
           (user (and results (alist-ref 'user results))))
      (if user
          `((user . ,(write-uri user)))
          `((user . #f))))))

(define-rest-call 'POST '("auth")
  (lambda (_)
    (let* ((body (read-request-body))
           ($$body (let ((parsed-body (form-urldecode body)))
                     (lambda (key)
                       (and parsed-body (alist-ref key parsed-body)))))
           (user ($$body 'user))
           (session-id (header 'mu-session-id)))
      (sparql-update "DELETE WHERE { GRAPH <http://mu.semte.ch/authorization> { ?s ?p ?o } }")
      (sparql-update "INSERT DATA { GRAPH <http://mu.semte.ch/authorization> { <~A> mu:account <~A> } }" 
                     session-id user))))

(define-rest-call 'POST '("proxy")
  (lambda (_)
    (let ((query (read-request-body)))
      (log-message "~%==Proxying Query==~%~A~%" (add-prefixes query))
      (proxy-query "proxy"
                   (add-prefixes query)
		   (if (update-query? (parse "test" query))
		       (*sparql-update-endpoint*)
		       (*sparql-endpoint*))))))

(define-rest-call 'DELETE '("clear")
  (lambda  (_)
    (parameterize ((*replace-session-id?* #f))
      (let ((graphs 
             (delete-duplicates
              (append 
               (get-all-graphs ((*read-constraint*)))
               (get-all-graphs ((*write-constraint*)))))))
        (sparql-update 
         (write-sparql
          `(@UpdateUnit
            (@Update
             (@Prologue ,@(constraint-prologues))
             (DELETE
              (GRAPH ?g (?s ?p ?o)))
             (WHERE
              (GRAPH ?g (?s ?p ?o))
              (VALUES ?g ,@graphs))))))))
          
       ;; (conc "DELETE { "
       ;;       " GRAPH ?g { ?s ?p ?o } "
       ;;       "} "
       ;;       "WHERE { "
       ;;       " GRAPH ?g { ?s ?p ?o } "
       ;;       " VALUES ?g { "
       ;;       (string-join (map (cut format "~%  ~A" <>) graphs))
       ;;       " } "
       ;;       "}"))
      `((success .  "true"))))

(define (load-plugin name)
  (load (make-pathname (*plugin-dir*) name ".scm")))

(define (save-plugin name read-constraint-string write-constraint-string fprops qprops unique-vars query-fprops?)
  (let* ((replace (lambda (str) 
                   (irregex-replace/all "^[ \n]+" (irregex-replace/all "[\"]" str "\\\"") "")))
         (read-constraint-string (replace read-constraint-string))
         (write-constraint-string (and write-constraint-string (replace write-constraint-string)))
         (fprops (or fprops (*functional-properties*)))
         (unique-vars  (map symbol->string (or unique-vars (*unique-variables*)))))

    (with-output-to-file (make-pathname (*plugin-dir*) name "scm")
      (lambda ()
        (format #t "(*functional-properties* '~A)~%~%" fprops)
        (format #t "(*unique-variables* '~A)~%~%" unique-vars)
        (format #t "(*query-functional-properties?* ~A)~%~%" query-fprops?)
        (format #t "(*queried-properties* '~A)~%~%" qprops)
        (format #t (conc "(define-constraint  ~%"
                         (if write-constraint-string
                             "  'read ~%"
                             "  'read/write ~%")
                         "  (lambda ()"
                         "    \"~%"
                         read-constraint-string
                         "  \"))~%~%"))
        
        (when write-constraint-string
              (format #t (conc "(define-constraint  ~%"
                               "  'write ~%"
                               "  (lambda () "
                               "    \"~%"
                               write-constraint-string
                               "  \"))~%~%")))
        ))))

(define-rest-call 'POST '("plugin" name)
  (rest-call (name)
    (let* ((body (read-request-body))
           ($$body (let ((parsed-body (form-urldecode body)))
                     (lambda (key)
                       (and parsed-body (alist-ref key parsed-body)))))

           (session-id (conc "\"" ($$body 'session-id) "\""))
           (read-constraint-string ($$body 'readconstraint))
           (write-constraint-string ($$body 'writeconstraint))
           (fprops (map string->symbol
                        (string-split (or ($$body 'fprops) "") ", ")))
           (qprops (map string->symbol
                        (string-split (or ($$body 'qprops) "") ", ")))
           (query-fprops? (equal? "true" ($$body 'query-fprops)))
           (unique-vars (map string->symbol
                             (string-split (or ($$body 'uvs) "") ", "))))
      (save-plugin name read-constraint-string write-constraint-string fprops qprops unique-vars query-fprops?)
      `((success . "true")))))

(define-rest-call 'GET '("plugin")
  (lambda (_)
    `((plugins . ,(list->vector
                   (sort
                    (map pathname-file (glob (make-pathname (*plugin-dir*) "*.scm")))
                    string<=))))))

(define-rest-call 'GET '("plugin" name)
  (rest-call (name)
    (load-plugin name)
    (parameterize ((*replace-session-id?* #f)
                   (*write-annotations?* #t))
      `((readConstraint . ,(write-sparql (call-if (*read-constraint*))))
        (writeConstraint . ,(write-sparql (call-if (*write-constraint*))))
        (functionalProperties . ,(list->vector (map ->string (*functional-properties*))))
        (queriedProperties . ,(list->vector (map ->string (*queried-properties*))))
        (queryFunctionalProperties . ,(*query-functional-properties?*))
        (uniqueVariables . ,(list->vector (map ->string (*unique-variables*))))))))

;; (define (serve-file path)
;;   (log-message "~%Serving ~A~%" path)
;;   (call-with-input-file path
;;     (lambda (port)
;;       (read-string #f port))))

;; (define (sandbox filename)
;;   (let ((filename (if (equal? filename "") "index.html" filename)))
;;     (if (feature? 'docker) 
;;         (make-pathname "/app/sandbox/" filename)
;;         (make-pathname "./sandbox/" filename))))

;; (define-rest-call 'GET '("sandbox") (lambda (_) (serve-file (sandbox "index.html"))))

;; ;; a way to do this directly in mu-chicken-support?
;; (define-rest-call 'GET '("sandbox" file)
;;   (rest-call
;;    (file)
;;    (serve-file (sandbox file))))

;; (define-rest-call 'GET '("sandbox" dir file)
;;   (rest-call
;;    (dir file)
;;    (serve-file (sandbox (string-join (list dir file) "/")))))

;; (define-rest-call 'GET '("sandbox" dir dir2 file)
;;   (rest-call
;;    (dir dir2 file)
;;    (serve-file (sandbox (string-join (list dir dir2 file) "/")))))
