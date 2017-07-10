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

(define *realm-id-graph*
  (config-param "REALM_ID_GRAPH" '<http://mu.semte.ch/uuid> read-uri))

(define *print-messages?*
  (config-param "PRINT_MESSAGES" #t))

(define log-message
  (let ((port (if (or (feature? 'docker)
                      (*print-messages?*))
                  (current-error-port)
                  (open-output-file "rewriter.log"))))
    (lambda (str #!rest args)
      (apply format port str args))))

(define *cache* (make-hash-table))

(define *session-realm-ids* (make-hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries
(define (get-type subject)
  (hit-hashed-cache
   *cache* (list 'Type subject)
   (query-unique-with-vars
    (type)
    (s-select '?type (s-triples `((,subject a ?type)))
              from-graph: #f)
    type)))

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
  (member (car quad)
          '(WHERE
            DELETE |DELETE WHERE| |DELETE DATA|
            INSERT |INSERT WHERE| |INSERT DATA|
            |@()| |@[]| MINUS OPTIONAL UNION FILTER BIND GRAPH)))

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
             (((or `GRAPH `OPTIONAL `MINUS) . _)  (loop bindings (cdr triples)))
             ((s p o)
              (if (get-type-binding bindings s)
                  (loop bindings (cdr triples))
                  (let ((stype
                         (or (get-type-binding declared-bindings s)
                             (and (iri? s)
                                 (get-type (expand-namespace s (query-namespaces))))
                             (new-sparql-variable "stype"))))
                    (loop (cons `(,s . ((type . ,stype))) bindings)
                          (cdr triples)))))
             (else (loop bindings (cdr triples)))))))))

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

(define (rewrite-triple-in-place triples stype bindings statements graph-statements #!key in-place?)
  (match (car triples)
    ((s p o)
     (let ((graph (get-graph stype p)))
       (rewrite-triples (cdr triples)
                        (update-bindings bindings s stype p graph)
                        (cons `(*GRAPH* ((GRAPH ,graph (,s ,p ,o))))
                                          statements)
                        graph-statements
                        in-place?: in-place?)))))

(define (rewrite-triples-queried triples stype bindings statements graph-statements #!key in-place?)
  (match (car triples)
    ((s p o)
     (let* ((bound-graph (get-graph-binding bindings s p))
            (graph (or bound-graph (new-sparql-variable "graph")))
            (new-graph-statement (and (not bound-graph) 
                                      (graph-match-statements graph s stype p))))
       (rewrite-triples (cdr triples)
                        (update-bindings bindings s stype p graph)
                        (cons
                         `(*GRAPH* ((GRAPH ,graph (,s ,p ,o))
                                    ,@(splice-when
                                       (and in-place? new-graph-statement))))
                         statements)
                        (if bound-graph graph-statements
                            (cons (car new-graph-statement)
                                  graph-statements))
                        in-place?: in-place?)))))

(define (rewrite-triples-reverse triples bindings statements graph-statements
                                 #!key (in-place? #t))
  (if (null? triples)
      (values statements graph-statements bindings)
      (let ((triple (car triples)))
        (case (car triple)
          ((*GRAPH*) (rewrite-triples-reverse (cdr triples) bindings
                                              (append (cadr triple) statements)
                                              graph-statements
                                              in-place?: in-place?))
          (else (let-values (((new-statements new-graph-statements new-bindings)
                              (rewrite-special triple bindings in-place?: in-place?)))
                (rewrite-triples-reverse (cdr triples)
                                         (unify-bindings bindings new-bindings)
                                         (append new-statements statements)
                                         (append graph-statements new-graph-statements)
                                         in-place?: in-place?)))))))
            
(define (rewrite-triples triples bindings
                         #!optional (statements '())  (graph-statements '()) 
                         #!key (in-place? #t))
  (cond ((null? triples)
         (rewrite-triples-reverse statements bindings '() graph-statements in-place?: in-place?))
        ((special? (car triples))
         (rewrite-triples (cdr triples)  bindings                          
                          (cons (car triples) statements) '()
                          in-place?: in-place?))
        (else
         (match (car triples)
           ((s p o) (let ((stype (get-type-binding bindings s)))
                      (if (and (iri? stype) (iri? p) (get-graph stype p))
                          (rewrite-triple-in-place triples stype bindings
                                                   statements graph-statements 
                                                   in-place?: in-place?)
                          (rewrite-triples-queried triples stype bindings
                                                   statements graph-statements 
                                                   in-place?: in-place?))))))))

(define (rewrite-triplesblock triples #!optional (bindings '()) #!key in-place?)
  (let ((triples (flatten-graphs (expand-triples triples))))
    (let ((new-bindings (assign-type-defs triples bindings)))
      (rewrite-triples triples new-bindings '() '() in-place?: in-place?))))

(define (rewrite-special-block group bindings in-place?)
  (let-values (((rewritten-quads graph-statements new-bindings)
                (rewrite-triplesblock (cdr group) bindings in-place?: in-place?)))
    (values (list (cons (car group) rewritten-quads))
            graph-statements
            new-bindings)))

(define (rewrite-special-union group bindings in-place?)
  (let-values (((rewritten-quads graph-statements new-bindings)
                (map-values/3 (cute rewrite-triplesblock <> bindings in-place?: in-place?)
                              (cdr group))))
    (values (list (cons (car group) rewritten-quads))
            graph-statements
            (join new-bindings))))

(define (rewrite-special group bindings #!key in-place?)
  (case (car group)
    ((WHERE |@()| |@[]| MINUS OPTIONAL
      DELETE |DELETE WHERE| |DELETE DATA|
      INSERT |INSERT WHERE| |INSERT DATA|) 
     (rewrite-special-block group bindings in-place?))
    ((UNION) (rewrite-special-union group bindings in-place?))
    ((FILTER BIND) (values (list group) '() '()))
    ((GRAPH) (if (*rewrite-graph-statements?*)
                 (rewrite-triplesblock (cddr group))
                 (values (list group) '() '())))
    (else #f)))

(define (rewrite-part-name part-name where-statements?)
  (case part-name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else part-name)))

(define (rewrite-update-query part bindings)
  (let-values (((rewritten-quads graph-statements type-bindings)
                (rewrite-triplesblock (cdr part) bindings)))
    (values (cons (rewrite-part-name (car part) (pair? graph-statements))
                  rewritten-quads)
            graph-statements
            type-bindings)))

(define (rewrite-query-parts part bindings where-clause)
  (case (car part)
    ((WHERE) (values '(WHERE) '() '()))
    ((@Dataset) 
     (values (replace-dataset where-clause 'from) '() '()))
    ((@Using)
     (values (replace-dataset where-clause 'using) '() '()))
    ((DELETE INSERT |INSERT DATA| |DELETE DATA| |DELETE WHERE|)
     (rewrite-update-query part bindings))
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
                            (loop (cdr parts) 
                                  (append rewritten-parts (list rw))
                                  (append g-statements (list gs)) 
                                  tbs))))))
        (let ((joined-graph-statements (join (filter pair? graph-statements))))          
          (alist-update 'WHERE 
                        (if (or (pair? where-clause)
                                (pair? (join graph-statements)))
                            `((@Query (|SELECT DISTINCT| *)
                                      (WHERE
                                       ,@(append where-statements
                                                 joined-graph-statements))))
                            '())
                            (filter pair? parts)))))))

(define (rewrite QueryUnit #!optional realm)
  (cons '@Unit
        (parameterize ((query-namespaces (query-prefixes QueryUnit)))
          (map (lambda (Query)
                 (map (lambda (part)
                        (case (car part)
                          ((@Query)
                           (if (*rewrite-select-queries?*)
                               (cons (car part) (rewrite-query-part part))
                               (cons '@Query
                                     (alist-update 
                                      'WHERE 
                                      (flatten-graphs-recursive
                                       (alist-ref 'WHERE (cdr part)))
                                      (alist-update '@Dataset (replace-dataset '() 'from)
                                                    (cdr part))))))
                          ((@Update)
                           (cons '@Update
                                 (rewrite-query-part
                                  (cons 
                                   '@Update
                                   (alist-update 
                                    '@Using
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
;; Call Implentations
(define (virtuoso-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (when body
      (log-message "~%==Virtuoso Error==~% ~A ~%" body))
    (when response
      (log-message "~%==Reason==:~%~A~%" (response-reason response)))
    (abort exn)))

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
                            (print "REALM!: " (*realm*))
                            (print "QUERY? "  query)
                            (rewrite query))))
    (print "MADe IT" rewritten-query)

    (when (*print-messages?*)
      (log-message "~%==Received Headers==~%~A~%" req-headers)
      (log-message "~%==Graph Realm==~%~A~%" graph-realm)
      (log-message "~%==Rewriting Query==~%~A~%" query-string)
      (log-message "~%==Parsed As==~%~A~%" (write-sparql query))
      (log-message "~%==Rewritten Query==~%~A~%" (write-sparql rewritten-query)))

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
              (when  (*print-messages?*)
                (log-message "~%==Results==~%~A~%" 
                              (substring result 0 (min 1000 (string-length result)))))
              (mu-headers headers)
              (format #f "~A~" result)))))))

(define change-session-realm-call 
  (rest-call
   (realm-id)
   (let ((mu-session-id (header-value 'mu-session-id (request-headers (current-request)))))
     (log-message "~%Changing graph-realm-id for mu-session-id ~A to ~A~%"
             mu-session-id realm-id)
     (hash-table-set! *session-realm-ids* mu-session-id realm-id)
     `((mu-session-id . ,mu-session-id)
       (realm-id . ,realm-id)))))

(define (delete-session-realm-call _)
  (let ((mu-session-id (header-value 'mu-session-id (request-headers (current-request)))))
    (log-message "~%Removing graph-realm-id for mu-session-id ~A to ~A~%"
            mu-session-id realm-id)
    (hash-table-delete! *session-realm-ids* mu-session-id)
     `((mu-session-id . ,mu-session-id)
       (realm-id . #f))))
                     
(define (add-realm-call _)
  (let* ((req-headers (request-headers (current-request)))
         (body (read-request-json))
         (realm (or (read-uri (alist-ref 'graph-realm body))
                    (get-realm (alist-ref 'graph-realm-id body))))
         (graph-type (read-uri (alist-ref 'graph-type body)))
         (graph (read-uri (alist-ref 'graph body))))
    (log-message "~%Adding graph-realm ~A for ~A  ~%"
            realm graph)
    (add-realm realm graph graph-type)))

(define (delete-realm-call _)
  (let* ((req-headers (request-headers (current-request)))
         (body (read-request-json))
         (realm (or (read-uri (alist-ref 'graph-realm body))
                    (get-realm (alist-ref 'graph-realm-id body))))
         (graph (read-uri (alist-ref 'graph body))))
    (log-message "~%Deleting graph-realm for ~A or ~A  ~%"
            realm graph)
    (delete-realm realm graph)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specifications                 
(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

;; (define-rest-call 'GET '("session" "realm") get-realm-call)

(define-rest-call 'PATCH '("session" "realm" realm-id) change-session-realm-call)

(define-rest-call 'DELETE '("session" "realm") delete-session-realm-call)

(define-rest-call 'POST '("realm") add-realm-call)
(define-rest-call 'POST '("realm" realm-id) add-realm-call)

(define-rest-call 'DELETE '("realm") delete-realm-call)
(define-rest-call 'DELETE '("realm" realm-id) delete-realm-call)

(*port* 8890)
