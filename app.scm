(use s-sparql s-sparql-parser mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace graphs "http://mu.semte.ch/graphs/")

(define query-namespaces (make-parameter (*namespaces*)))

(define *realm* (make-parameter #f))

(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define *from/using-graphs* (make-parameter '()))

(define *cache* (make-hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

(define (get-type subject)
  (query-unique-with-vars
   (type)
   (s-select '?type (s-triples `((,subject a ?type)))
	     from-graph: #f)
   type))

(define (get-graph-query stype p)
  `((GRAPH ,(*default-graph*)
          (?graph a graphs:Graph)
            
          
          (?rule graphs:predicate ,p)
          (?rule graphs:subjectType ,stype)
           
          ,(if (*realm*)
               `(UNION ((?rule graphs:graphType ?type)
                        (?graph graphs:type ?type)
                        (?graph graphs:realm ,(*realm*)))
                       ((?rule graphs:graph ?graph)))
                `(?rule graphs:graph ?graph)))))

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
               `((,s a ,stype)))) ;; ??
       (GRAPH ,(*default-graph*) 
              (,rule a graphs:GraphRule)
              (,graph a graphs:Graph)
              ,(if (*realm*)
                   `(UNION ((,rule graphs:graph ,graph))
                           ((,rule graphs:graphType ,gtype)
                            (,graph graphs:type ,gtype)
                            (,graph graphs:realm ,(*realm*))))
                   `(,rule graphs:graph ,graph))
              ,(if (equal? p 'a)
                   `(,rule graphs:predicate rdf:type)
                   `(,rule graphs:predicate ,p))
              (,rule graphs:subjectType ,stype))))))

;; add restriction on realms...
(define (all-graphs)
  (hit-hashed-cache
   *cache* 'all-graphs
   (query-with-vars 
    (graph)
    (s-select 
     '?graph
     (s-triples `((GRAPH ,(*default-graph*)
                         (?graph a graphs:Graph))))
     from-graph: #f)
     ;; (s-triples `((?graph a graphs:Graph))))
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

(define (replace-dataset)
  `((@Dataset
     (|FROM NAMED| ,(*default-graph*))
     ,@(map (lambda (graph) 
              `(FROM ,graph))
            (append (*from/using-graphs*)
                    (all-graphs)))
     ,@(map (lambda (graph) 
              `(|FROM NAMED| ,graph))
            (all-graphs)))))

(define (replace-using)
  `((@Dataset
     (|USING NAMED| ,(*default-graph*))
     ,@(map (lambda (graph) 
              `(USING ,graph))
            (append (*from/using-graphs*) 
                    (all-graphs)))
     ,@(map (lambda (graph) 
              `(|USING NAMED| ,graph))
            (all-graphs)))))

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
                        (update-bindings bindings stype p graph)
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
                        (update-bindings bindings stype p graph)
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
  (print "flattening?? " (*rewrite-graph-statements?*))
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
                       (append graph-statements quads-graph-statements)
                       (append (join quads-bindings)
                               triples-bindings))))))))
    
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
    ((@Dataset) (values (replace-dataset) '() '()))
    ((@Using) (values (replace-using) '() '()))
    ((DELETE INSERT |INSERT DATA| |DELETE DATA| |DELETE WHERE|)
     (let-values (((rewritten-quads graph-statements _)
                   (rewrite-quads (cdr part) bindings)))
       (values (cons (rewrite-part-name (car part) (pair? graph-statements))
                     rewritten-quads)
               graph-statements
               '())))
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
                    (map-values/3
                     (cute rewrite-update-unit-part <> bindings where-clause)
                     (cdr unit))))
        (let ((joined-graph-statements (join graph-statements)))
          (if (or (pair? where-clause) (pair? (join graph-statements)))
              (alist-update 'WHERE (append where-statements
                                           ;; Virtuoso workaround, for INSERT
                                           (if (pair? joined-graph-statements)
                                               `((@Query (SELECT *)
                                                         (WHERE ,@joined-graph-statements)))
                                               '()))
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
                             (PREFIX |graphs:| <http://mu.semte.ch/graphs/>)
                             ,@(cdr part)))
                          (else part)))
                      unit))
               (alist-ref '@Unit QueryUnit)))))

(define (rewrite-call _)
  (let* (($ (request-vars source: 'query-string))
         (query-string ($ 'query)))
    (let* ((query (parse-query (or query-string (read-request-body))))
          (graph-realm (header-value 'mu-graph-realm (request-headers (current-request))))
          (rewritten-query (parameterize ((*realm* graph-realm))
                             (rewrite query))))

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
	(format (current-error-port) "~%==Result==~%~A" result)
	(mu-headers headers)
	(format #f "~A~" result))))))

(define-rest-call 'POST '("sparql") rewrite-call)

(define-rest-call 'GET '("sparql") rewrite-call)

(*port* 8890)
