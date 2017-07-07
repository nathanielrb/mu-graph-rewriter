(use s-sparql s-sparql-parser mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace rewriter "http://mu.semte.ch/graphs/")

(define query-namespaces (make-parameter (*namespaces*)))

(define *realm* (make-parameter #f))

(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #f))

(*default-graph* '<http://mu.semte.ch/graphs>)

(define *realm-id-graph*
  (config-param "REALM_ID_GRAPH" '<http://mu.semte.ch/uuid> read-uri))

(define *cache* (make-hash-table))

(define *session-realm-ids* (make-hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

(define (get-type subject)
  (query-unique-with-vars
   (type)
   (s-select '?type (s-triples `((,subject a ?type)))
	     from-graph: #f)
   type))

(define (get-realm realm-id)
  (and realm-id
       (query-unique-with-vars
        (realm)
        (s-select '?realm (s-triples `((?realm mu:uuid ,realm-id)))
                  from-graph: (*realm-id-graph*))
        realm)))

(define (add-realm realm graph graph-type)
  (sparql/update
   (s-insert
    (s-triples
     `((,graph rewriter:realm ,realm)
       (,graph a rewriter:Graph)
       (,graph rewriter:type ,graph-type))))))

(define (delete-realm realm graph)
  (sparql/update
   (if graph
       (s-delete
        (s-triples `((,graph ?p ?o)))
        where: (s-triples `((,graph ?p ?o))))
       (s-delete
        (s-triples `((?graph ?p ?o)))
        where: (s-triples `((?graph rewriter:realm ,realm)
                            (?graph ?p ?o)))))))
         
(define (get-graph-query stype p)
  `((GRAPH ,(*default-graph*)
          (?graph a rewriter:Graph)
          (?rule rewriter:predicate ,p)
          (?rule rewriter:subjectType ,stype)
          ,(if (*realm*)
               `(UNION ((?rule rewriter:graphType ?type)
                        (?graph rewriter:type ?type)
                        (?graph rewriter:realm ,(*realm*)))
                       ((?rule rewriter:graph ?graph)))
                `(?rule rewriter:graph ?graph)))))

(define (get-graph stype p)
  (parameterize ((*namespaces* (append (*namespaces*) (query-namespaces))))
    (car-when
     (hit-hashed-cache
      *cache* (list stype p (*realm*))
      (query-with-vars 
       (graph)
       (s-select 
        '?graph
        (s-triples (get-graph-query stype p))
        from-graph: #f)
       graph)))))

(define (graph-match-statements graph s stype p)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
    `((OPTIONAL
       ,@(splice-when
          (and (not (iri? stype))
               `((,s a ,stype))))
       (GRAPH ,(*default-graph*) 
              (,rule a rewriter:GraphRule)
              (,graph a rewriter:Graph)
              ,(if (*realm*)
                   `(UNION ((,rule rewriter:graph ,graph))
                           ((,rule rewriter:graphType ,gtype)
                            (,graph rewriter:type ,gtype)
                            (,graph rewriter:realm ,(*realm*))))
                   `(,rule rewriter:graph ,graph))
              ,(if (equal? p 'a)
                   `(,rule rewriter:predicate rdf:type)
                   `(,rule rewriter:predicate ,p))
              (,rule rewriter:subjectType ,stype))))))

(define (all-graphs)
  (hit-hashed-cache
   *cache* (list 'graphs (*realm*))
   (query-with-vars 
    (graph)
    (s-select 
     '?graph
     (s-triples
      `((GRAPH
         ,(*default-graph*)
         (?graph a rewriter:Graph)
         ,@(splice-when
            (and (*realm*)
                 `((UNION ((?rule rewriter:graphType ?type)
                           (?graph rewriter:type ?type)
                           (?graph rewriter:realm ,(*realm*)))
                          ((?rule rewriter:graph ?graph)))))))))
     from-graph: #f)
     graph)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rewriting

(define query-namespaces (make-parameter (*namespaces*)))

(define (PrefixDecl? decl) (equal? (car decl) 'PREFIX))

(define (BaseDecl? decl) (equal? (car decl) 'BASE))

(define (graph? quad)
  (and (list? quad)
       (equal? (car quad) 'GRAPH)))

(define (special? quad)
  (case (car quad)
    ((WHERE INSERT DELETE 
	    |DELETE WHERE| |DELETE DATA| |INSERT WHERE| |INSERT DATA|
	    |@()| |@[]| MINUS OPTIONAL UNION FILTER BIND GRAPH)
     #t)
    (else #f)))

(define (remove-trailing-char sym #!optional (len 1))
  (let ((s (symbol->string sym)))
    (string->symbol
     (substring s 0 (- (string-length s) len)))))

(define (query-prefixes QueryUnit)
  (map (lambda (decl)
         (list (remove-trailing-char (cadr decl)) (write-uri (caddr decl))))
       (filter PrefixDecl? (unit-prologue QueryUnit))))

(define (query-bases QueryUnit)
  (map (lambda (decl)
         (list (cadr decl) (write-uri (caddr decl))))
       (map cdr (filter BaseDecl? (unit-prologue QueryUnit)))))


(define (rewrite-triples-reverse triples bindings
                                 #!key (statements '()) (in-place? #t) (graph-statements '()))
  (if (null? triples)
      (values statements graph-statements bindings)
      (let ((triple (car triples)))
        (case (car triple)
          ((*GRAPH*) (rewrite-triples-reverse (cdr triples) bindings
                                              statements: (append (cadr triple) statements)
                                              graph-statements: graph-statements))
          (else (let-values (((new-statements new-graph-statements new-bindings)
                              (rewrite-special triple bindings in-place?: in-place?)))
                (rewrite-triples-reverse (cdr triples)
                                         (unify-bindings bindings new-bindings)
                                         statements: (append new-statements statements)
                                         graph-statements: (append graph-statements new-graph-statements)
                                         in-place?: in-place?)))))))
      
      
(define (rewrite-triples triples bindings
                         #!key (statements '()) (in-place? #t) (graph-statements '()) )
  (cond ((null? triples)
         (rewrite-triples-reverse statements bindings graph-statements: graph-statements in-place?: in-place?))
        ((special? (car triples))
         (rewrite-triples (cdr triples)
                          bindings
                          in-place?: in-place?
                          statements: (cons (car triples) statements)))
        (else
         (match (car triples)
           ((s p o) (let ((stype (get-type-binding bindings s)))
                      (if (and (iri? stype)  (iri? p) (get-graph stype p))
                          (rewrite-triple-in-place triples stype statements
                                                   in-place? graph-statements 
                                                   bindings)
                                                   
                          
                          (rewrite-triples-queried triples stype statements 
                                                   in-place? graph-statements 
                                                   bindings))))))))


(define (type-def triple)
  (match triple
    ((s `a o)
     `((,s . ((type . ,o)))))
    (else #f)))

(define (type-defs triples)
  (join (filter values (map type-def triples))))

(define (assign-type-defs triples bindings)
  (let ((declared-bindings (type-defs triples)))
    (unify-bindings
     bindings
     (let loop ((bindings bindings)
                (triples triples))
       (if (null? triples)
           bindings
           (match (car triples)
             ((s p o)
              (if (get-type-binding bindings s)
                  (loop bindings (cdr triples))
                  (let ((stype
                         (or (get-type-binding declared-bindings s)
                             (new-sparql-variable "stype"))))
                    (loop (cons `(,s . ((type . ,stype))) bindings)
                          (cdr triples)))))
             (else (loop bindings  (cdr triples)))))))))


(define (rewrite-triplesblock triples #!optional (bindings '()) #!key in-place?)
  (let ((triples (flatten-graphs (expand-triples triples))))
    (let ((new-bindings (assign-type-defs triples bindings)))
      (print "EXISTING BINDINGS")(print bindings)
      (print "NEW BINDINGS")(print new-bindings)(newline)
      (rewrite-triples triples new-bindings in-place?: in-place?))))

(define (replace-dataset where-clause label-key)
  (let ((graphs (append (if (*rewrite-graph-statements?*)
                        '()
                        (extract-graphs where-clause))
                    (all-graphs)))
        (type  (case label-key ((from) '@Dataset) ((using) '@Using)))
        (label (case label-key ((from) 'FROM) ((using) 'USING)))
        (label-named (case label-key
                       ((from) '|FROM NAMED|)
                       ((using) '|USING NAMED|))))
    `((,type
       (,label ,(*default-graph*))
       ,@(map (lambda (graph) 
                `(,label ,graph))
              graphs)
       (,label-named ,(*default-graph*))
       ,@(map (lambda (graph) 
                `(,label-named ,graph))
              graphs)))))

(define (unify-bindings new-bindings old-bindings)
  (append new-bindings old-bindings)) 

(define (map-values/3 proc lst)
  (if (null? lst)
      (values '() '() '())
      (let-values (((car-a car-b car-c) (proc (car lst)))
                   ((cdr-a cdr-b cdr-c) (map-values/3 proc (cdr lst))))
        (values (cons car-a cdr-a)
                (cons car-b cdr-b)
                (cons car-c cdr-c)))))

(define (update-bindings bindings s #!optional stype pred graph)
  (let ((binding (or (alist-ref s bindings) '())))
    (alist-update 
     s
     (alist-update 
      'type 
      (or stype (alist-ref 'type binding))
      (or (and pred graph
               (alist-update
                'predicates
                (alist-update pred graph (or (alist-ref 'predicates binding) '()))
                binding))
          binding))
     bindings)))

(define (get-type-binding bindings s)
  (nested-alist-ref bindings s 'type))

(define (get-graph-binding bindings s pred)
  (nested-alist-ref bindings s 'predicates pred))

(define (rewrite-triple-in-place triples stype statements in-place? graph-statements bindings)
  (match (car triples)
    ((s p o)
     (let ((graph (get-graph stype p)))
       (rewrite-triples (cdr triples)
                        (update-bindings bindings s stype p graph)
                        statements: (cons `(*GRAPH* ((GRAPH ,graph (,s ,p ,o))))
                                          statements)
                        graph-statements: graph-statements
                        in-place?: in-place?)))))

(define (rewrite-triples-queried triples stype statements in-place? graph-statements bindings)
  (match (car triples)
    ((s p o)
     (let* ((bound-graph (get-graph-binding bindings s p))
            (graph (or bound-graph (new-sparql-variable "graph")))
            (new-graph-statement (if bound-graph '()
                                     (graph-match-statements graph s stype p))))
       (rewrite-triples (cdr triples)
                        (update-bindings bindings s stype p graph)
                        statements: (cons
                                     `(*GRAPH* ((GRAPH ,graph (,s ,p ,o))
                                                 ,@(splice-when
                                                    (and in-place? new-graph-statement))))
                                     statements)
                        graph-statements:  (if bound-graph graph-statements
                                               (cons (car new-graph-statement) graph-statements))
                        in-place?: in-place?)))))

(define (rewrite-triples triples bindings
                         #!key (statements '()) (in-place? #t) (graph-statements '()) )
  (if (null? triples)
      (values statements graph-statements bindings)
      (match (car triples)
	((s p o) (let ((stype
                        (or (get-type-binding bindings s)
                            (and (iri? s)
                                 (get-type (expand-namespace s (query-namespaces))))
                            (new-sparql-variable "stype"))))
		   (if (and (iri? stype)  (iri? p) (get-graph stype p))
                       (rewrite-triple-in-place triples stype statements
                                                 in-place? graph-statements 

                                                 (if (alist-ref s bindings)
                                                     bindings
                                                     (update-bindings bindings s stype)))

                       (rewrite-triples-queried triples stype statements 
                                              in-place? graph-statements 
                                              bindings)))))))

(define (rewrite-special group bindings #!key in-place?)
  (case (car group)
    ((WHERE INSERT DELETE |DELETE WHERE| |DELETE DATA| |INSERT WHERE| |INSERT DATA| |@()| |@[]| MINUS OPTIONAL)
     (let-values (((rewritten-quads graph-statements new-bindings)
                   (rewrite-triplesblock (cdr group) bindings in-place?: in-place?)))
       (values (list (cons (car group) rewritten-quads))
               graph-statements
               new-bindings)))
    ((UNION)
     (let-values (((rewritten-quads graph-statements new-bindings)
                   (map-values/3 (cute rewrite-triplesblock <> bindings in-place?: in-place?)
                                 (cdr group))))
       (values (list (cons (car group) rewritten-quads))
               graph-statements
               (join new-bindings))))
    ((FILTER BIND) (values (list group) '() '()))
    ((GRAPH) (if (*rewrite-graph-statements?*)
                 (rewrite-triplesblock (cddr group))
                 (values (list group) '() '())))
    (else #f)))

(define (flatten-graphs triples)
  (if (*rewrite-graph-statements?*)
      (join (map (lambda (triple)
                   (if (equal? 'GRAPH (car triple))
                       (cddr triple)
                       (list triple)))
                 triples))
      triples))

(define (flatten-graphs-recursive quads)
  (join (map (lambda (triple)
               (if (pair? (car triple))
                   (list (flatten-graphs-recursive triple))
                   (case (car triple)
                     ((GRAPH) (cddr triple))
                     ((OPTIONAL MINUS UNION)
                      `((,(car triple) ,@(flatten-graphs-recursive (cdr triple)))))
                     (else (list triple)))))
             quads)))

    
(define (extract-graphs quads #!optional (graphs '()))
  (if (null? quads)
      graphs
      (let ((quad (car quads)))
        (cond ((graph? quad) 
               (extract-graphs
                (cdr quads) (cons (cadr quad) (extract-graphs (cddr quad) graphs))))
              ((special? quad)
               (extract-graphs
                (cdr quads) (extract-graphs (cdr quad) graphs)))
              (else
               (extract-graphs (cdr quads) graphs))))))

(define (extract-all-variables where-clause)
  (delete-duplicates (filter sparql-variable? (flatten where-clause))))

(define (rewrite-part-name part-name where-statements?)
  (case part-name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else part-name)))

(define (rewrite-query-parts part bindings where-clause)
  (case (car part)
    ((WHERE) (values '(WHERE) '() '()))
    ((@Dataset) (values (replace-dataset where-clause 'from) '() '()))
    ((@Using) (values (replace-dataset where-clause 'using) '() '()))
    ((DELETE INSERT |INSERT DATA| |DELETE DATA| |DELETE WHERE|)
     (let-values (((rewritten-quads graph-statements type-bindings)
                   (rewrite-triplesblock (cdr part) bindings)))
       (values (cons (rewrite-part-name (car part) (pair? graph-statements))
                     rewritten-quads)
               graph-statements
               type-bindings)))
    ((SELECT |SELECT DISTINCT| |SELECT REDUCED|)
     (values `(,(car part)
               ,@(if (equal? (cdr part) '(*))
                     (extract-all-variables where-clause)
                     (cdr part)))
             '() '()))
    (else (values part '() '()))))

(define (rewrite-query-part unit)
  (let ((where-clause (alist-ref 'WHERE (cdr unit))))
    (let-values (((where-statements _ bindings)
                  (if where-clause
                      (rewrite-triplesblock where-clause '() in-place?: #t)
                      (values '() '() '()))))
      (let-values (((parts graph-statements _)
                    (let loop ((parts (cdr unit))
                               (rewritten-parts '())
                               (g-statements '())
                               (t-bindings bindings))
                      (if (null? parts)
                          (values rewritten-parts g-statements t-bindings)
                          (let-values (((rw gs tbs)
                                        (rewrite-query-parts (car parts) t-bindings where-clause)))
                            (loop (cdr parts) (append rewritten-parts (list rw))
                                  (append g-statements (list gs)) tbs))))))
        (let ((joined-graph-statements (join (filter pair? graph-statements))))
          (if (or (pair? where-clause) (pair? (join graph-statements)))
              (alist-update 'WHERE 
                            `((@Query (|SELECT DISTINCT| *)
                                      (WHERE ,@(append where-statements joined-graph-statements))))
                                       ;;(append where-statements joined-graph-statements)
                            (filter pair? parts))
              (alist-update 'WHERE '()
                            (filter pair? parts))))))))


(define (rewrite QueryUnit #!optional realm)
  (cons '@Unit
        (parameterize ((query-namespaces (query-prefixes QueryUnit)))
          (map (lambda (Query)
                 (map (lambda (part)
                        (case (car part)
                          ((@Query)
                           (if (*rewrite-select-queries?*)
                               (cons (car part) (rewrite-query-part part))
                               (cons (car part)
                                     (alist-update 'WHERE (flatten-graphs-recursive
                                                           (alist-ref 'WHERE (cdr part)))
                                                   (alist-update '@Dataset (replace-dataset '() 'from)
                                                                 (cdr part))))))
                          ((@Update)
                           (cons (car part)
                                 (rewrite-query-part
                                  (cons 
                                   '@Update
                                   (alist-update '@Using ;; make sure there's a Using to replace
                                                 (alist-ref '@Using (cdr part))
                                                 (cdr part))))))
                          ((@Prologue)
                           `(@Prologue
                             (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
                             ,@(cdr part)))
                          (else part)))
                      Query))
               (alist-ref '@Unit QueryUnit)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Calls
(define (virtuoso-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (format (current-error-port)  "~%==Virtuoso Error==~% ~A ~%" body)
    (when response
      (format (current-error-port)
              "~%==Reason==:~%~A~%"
              (response-reason response)))
    (abort exn)))

;; omigod refactor this please
(define (rewrite-call _)
  (let* (($ (request-vars source: 'query-string))
         (body (read-request-body))
         ($body (let ((parsed-body (form-urldecode body)))
                  (lambda (key)
                    (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($ 'query) ($body 'query) body))
         (query (parse-query query-string))
         (req-headers (request-headers (current-request)))
         (mu-session-id (header-value 'mu-session-id req-headers))
         (graph-realm (or (get-realm (hash-table-ref/default *session-realm-ids* mu-session-id #f))
                          (header-value 'mu-graph-realm req-headers)
                          ($ 'graph-realm)
                          ($body 'graph-realm) 
                          (get-realm (header-value 'mu-graph-realm-id req-headers))
                          (get-realm ($ 'graph-realm-id))))
         (rewritten-query (parameterize ((*realm* graph-realm)
                                         (*rewrite-graph-statements?* 
                                          (not (or (header-value
                                                    'preserve-graph-statements req-headers)
                                                   ($ 'preserve-graph-statements))))
                                         (*rewrite-select-queries?* 
                                          (or (equal? "true" (header-value
                                                              'rewrite-select-queries req-headers))
                                              (equal? "true" ($ 'rewrite-select-queries))
                                              (*rewrite-select-queries?*))))
                            (rewrite query))))

      (format (current-error-port) "~%==Received Headers==~%~A~%" req-headers)
      (format (current-error-port) "~%==Graph Realm==~%~A~%" graph-realm)
      (format (current-error-port) "~%==Rewriting Query==~%~A~%" query-string)
      (format (current-error-port) "~%==Parsed As==~%~A~%" (write-sparql query))
      (format (current-error-port) "~%==Rewritten Query==~%~A~%" (write-sparql rewritten-query))

      (handle-exceptions exn 
          (virtuoso-error exn)
        
        (parameterize ((tcp-read-timeout #f)
                       (tcp-write-timeout #f)
                       (tcp-connect-timeout #f))
          (let-values (((result uri response)
                        (with-input-from-request 
                         (make-request method: 'POST
                                       uri: (uri-reference (*sparql-endpoint*))
                                     headers: (headers
                                                '((Content-Type application/x-www-form-urlencoded)
                                                  (Accept application/sparql-results+json))))                                               
                                                ;;(append (headers->list (request-headers (current-request)))

                         `((query . , (format #f "~A" (write-sparql rewritten-query))))
                         read-string)))
            (close-connection! uri)
            (let ((headers (headers->list (response-headers response))))
              (format (current-error-port) "~%==Results==~%~A~%" 
                      (substring result 0 (min 1000 (string-length result))))
              (mu-headers headers)
              (format #f "~A~" result)))))))

(define change-realm-call 
  (rest-call
   (realm-id)
   (let ((mu-session-id (header-value 'mu-session-id (request-headers (current-request)))))

     (format (current-error-port) "~%Changing graph-realm-id for mu-session-id ~A to ~A~%"
             mu-session-id realm-id)
             
     (and mu-session-id
          (hash-table-set! *session-realm-ids* mu-session-id realm-id)
          `((mu-session-id . ,mu-session-id)
            (realm-id . ,realm-id))))))
                     
(define (add-realm-call _)
  (let* ((req-headers (request-headers (current-request)))
         (body (read-request-json))
         (realm (or (read-uri (alist-ref 'graph-realm body))
                    (get-realm (alist-ref 'graph-realm-id body))))
         (graph-type (read-uri (alist-ref 'graph-type body)))
         (graph (read-uri (alist-ref 'graph body))))
    (format (current-error-port) "~%Adding graph-realm ~A for ~A  ~%"
            realm graph)
    (add-realm realm graph graph-type)))

(define (delete-realm-call _)
  (let* ((req-headers (request-headers (current-request)))
         (body (read-request-json))
         (realm (or (read-uri (alist-ref 'graph-realm body))
                    (get-realm (alist-ref 'graph-realm-id body))))
         (graph (read-uri (alist-ref 'graph body))))
    (format (current-error-port) "~%Deleting graph-realm for ~A or ~A  ~%"
            realm graph)
    (delete-realm realm graph)))
                  
(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

;; (define-rest-call 'GET '("session" "realm") get-realm-call)

(define-rest-call 'PATCH '("session" "realm" realm-id) change-realm-call)

(define-rest-call 'POST '("realm") add-realm-call)
(define-rest-call 'POST '("realm" realm-id) add-realm-call)

(define-rest-call 'DELETE '("realm") delete-realm-call)
(define-rest-call 'DELETE '("realm" realm-id) delete-realm-call)

(*port* 8890)
