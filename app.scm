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

(define *realm-id-graph*
  (config-param "REALM_ID_GRAPH" '<http://mu.semte.ch/uuid>))

(define *cache* (make-hash-table))

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

(define (replace-dataset where-clause)
  (let ((graphs (append (if (*rewrite-graph-statements?*)
                        '()
                        (extract-graphs where-clause))
                    (all-graphs))))
    `((@Dataset
       (FROM ,(*default-graph*))
       ,@(map (lambda (graph) 
                `(FROM ,graph))
              graphs)))))

(define (replace-using where-clause)
  (let ((graphs (append (if (*rewrite-graph-statements?*)
                        '()
                        (extract-graphs where-clause))
                    (all-graphs))))
  `((@Dataset    
     (USING ,(*default-graph*))
     ,@(map (lambda (graph) 
              `(USING ,graph))
            graphs)))))

(define (unify-bindings new-bindings old-bindings)
  (append new-bindings old-bindings)) 

(define (map-values/2 proc lst)
  (if (null? lst)
      (values '() '())
      (let-values (((car-a car-b) (proc (car lst)))
                   ((cdr-a cdr-b) (map-values/2 proc (cdr lst))))
        (values (cons car-a cdr-a)
                (cons car-b cdr-b)))))        

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
                        statements: (cons `((GRAPH ,graph (,s ,p ,o)))
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
                        statements: (append
                                     statements
                                     `(((GRAPH ,graph (,s ,p ,o))
                                       ,@(splice-when
                                          (and in-place? new-graph-statement)))))
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
(define (graph? quad)
  (and (list? quad)
       (equal? (car quad) 'GRAPH)))

(define (special? quad)
  (case (car quad)
    ((WHERE INSERT DELETE 
	    |DELETE WHERE| |DELETE DATA| |INSERT WHERE| |INSERT DATA|
	    |@()| |@[]| MINUS OPTIONAL UNION FILTER GRAPH)
     #t)
    (else #f)))

(define (rewrite-special group bindings #!key in-place?)
  (case (car group)
    ((WHERE INSERT DELETE |DELETE WHERE| |DELETE DATA| |INSERT WHERE| |INSERT DATA| |@()| |@[]| MINUS OPTIONAL)
     (let-values (((rewritten-quads graph-statements new-bindings)
                   (rewrite-quads (cdr group) bindings in-place?: in-place?)))
       (values (list (cons (car group) rewritten-quads))
               graph-statements
               new-bindings)))
    ((UNION)
     (let-values (((rewritten-quads graph-statements new-bindings)
                   (map-values/3 (cute rewrite-quads <> bindings in-place?: in-place?)
                                 (cdr group))))
       (values (list (cons (car group) rewritten-quads))
               graph-statements
               (join new-bindings))))
    ((FILTER) (values (list group) '() '()))
    ((GRAPH) (if (*rewrite-graph-statements?*)
                 (rewrite-quads (cddr group))
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

;; this re-orders the triples and quads, which isn't great...
(define (rewrite-quads quads #!optional (bindings '()) #!key in-place?)
  (cond ((special? quads)
         (let-values (((x y z) (rewrite-special quads bindings in-place?: in-place?)))
           (values x y z)))
        (else
         (let-values (((quads-not-triples triples)
                       (partition special? (flatten-graphs (expand-triples quads)))))
           (let ((new-bindings (unify-bindings (type-defs triples) bindings)))
             (let-values (((rewritten-triples graph-statements triples-bindings)
                           (rewrite-triples triples new-bindings 
                                            in-place?: in-place?))
                          ((rewritten-quads quads-graph-statements quads-bindings)
                           (map-values/3 (cute rewrite-quads <> new-bindings in-place?: in-place?)
                                         quads-not-triples)))
               (values (join (append rewritten-triples rewritten-quads))
                       (append (filter pair? graph-statements) (filter pair? quads-graph-statements))
                       (append (join quads-bindings)
                               triples-bindings))))))))
    
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
  (if (not where-statements?)
      part-name
      (case part-name
        ((|INSERT DATA|) 'INSERT)
        ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
        (else part-name))))

(define (rewrite-update-unit-part part bindings where-clause)
  (case (car part)
    ((WHERE) (values '(WHERE) '() '()))
    ((@Dataset) (values (replace-dataset where-clause) '() '()))
    ((@Using) (values (replace-using where-clause) '() '()))
    ((DELETE INSERT |INSERT DATA| |DELETE DATA| |DELETE WHERE|)
     (let-values (((rewritten-quads graph-statements type-bindings)
                   (rewrite-quads (cdr part) bindings)))
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

(define (rewrite-update-unit unit)
  (let ((where-clause (alist-ref 'WHERE (cdr unit))))
    (let-values (((where-statements _ bindings)
                  (if where-clause
                      (rewrite-quads where-clause '() in-place?: #t)
                      (values '() '() '()))))
      (let-values (((parts graph-statements _)
                    (let loop ((parts (cdr unit))
                               (rewritten-parts '())
                               (g-statements '())
                               (t-bindings bindings))
                      (if (null? parts)
                          (values rewritten-parts g-statements t-bindings)
                          (let-values (((rw gs tbs)
                                        (rewrite-update-unit-part (car parts) t-bindings where-clause)))
                            (loop (cdr parts) (append rewritten-parts (list rw))
                                  (append g-statements (list gs)) tbs))))))
        (let ((joined-graph-statements (join (filter pair? graph-statements))))
          (if (or (pair? where-clause) (pair? (join graph-statements)))
              (alist-update 'WHERE 
                            `((@Query (|SELECT DISTINCT| *)
                                      (WHERE ,@(append where-statements joined-graph-statements))))
                                       ;;(append where-statements joined-graph-statements)
                            (filter pair? parts))
            (filter pair? parts)))))))


(define (rewrite QueryUnit #!optional realm)
  (cons '@Unit
        (parameterize ((query-namespaces (query-prefixes QueryUnit)))
          (map (lambda (unit)
                 (map (lambda (part)
                        (case (car part)
                          ((@Query)
                           (cons (car part)
                                 (rewrite-update-unit part)))
                          ((@Update)
                           (cons (car part)
                                 (rewrite-update-unit 
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
                      unit))
               (alist-ref '@Unit QueryUnit)))))

(define (rewrite-call _)
  (let* (($ (request-vars source: 'query-string))
         (query-string ($ 'query)))
    (let* ((query (parse-query (or query-string (read-request-body))))
           (req-headers (request-headers (current-request)))
           (graph-realm (or (header-value 'mu-graph-realm req-headers)
                            (get-realm (header-value 'mu-graph-realm-id req-headers))))
           (rewritten-query (parameterize ((*realm* graph-realm)
                                           (*rewrite-graph-statements?* 
                                            (not
                                             (header-value 'preserve-graph-statements req-headers))))
                              (rewrite query))))

      (format (current-error-port) "~%==Graph Realm==~%~A~%" graph-realm)
      (format (current-error-port) "~%==Rewriting Query==~%~A~%" (write-sparql query))
      (format (current-error-port) "~%==Rewritten Query==~%~A~%" (write-sparql rewritten-query))

    (let-values (((result uri response)
		  (with-input-from-request 
		   (make-request method: 'POST
				 uri: (uri-reference (*sparql-endpoint*))
				 headers: (headers
                                           (append
                                            (headers->list (request-headers (current-request)))
                                            '((Content-Type application/x-www-form-urlencoded)
                                              (Accept application/json)))))
		   `((query . , (format #f "~A" (write-sparql rewritten-query))))
                   read-string)))
      (close-connection! uri)
      (let ((headers (headers->list (response-headers response))))
	(format (current-error-port) "~%==Result==~%~A~%" result)
	(mu-headers headers)
	(format #f "~A~" result))))))

(define-rest-call 'POST '("sparql") rewrite-call)

(define-rest-call 'GET '("sparql") rewrite-call)

(*port* 8890)
