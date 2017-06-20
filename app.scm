(use s-sparql s-sparql-parser mu-chicken-support matchable)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace graphs "http://mu.semte.ch/graphs/")

(*default-graph* '<http://mu.semte.ch/graphs>)

(define *rules-graph* (make-parameter '<http://mu.semte.ch/graphs>))

(define query-namespaces (make-parameter (*namespaces*)))

(define *realm* (make-parameter #f))

(define *use-realms?* (make-parameter #t))

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
           ((?graph a graphs:Graph)
            
            ,@(if (*use-realms?*)
                  `((?graph graphs:realm ,(*realm*)))
                  '()) ;; ??
            
            (UNION ((?rule graphs:predicate ,p))
                   (((?rule graphs:subjectType ,stype)))))
           
           (UNION ((?graph graphs:type ?type)
                   (?rule graphs:graphType ?type))
                  ((?rule graphs:graph ?graph))))))

(define (get-graph stype p)
  (car-when
   (hit-hashed-cache
    *cache* (list stype p (or (*realm*) "_ALL"))
    (query-with-vars 
     (graph)
     (s-select 
      '?graph
      (s-triples (get-graph-query stype p))
      from-graph: #f)
     graph))))

(define (graph-match-statements graph s stype p)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
  ;;  `(GRAPH ,(*rules-graph*) 
    `((GRAPH ,(*rules-graph*) 
             (,rule a graphs:GraphRule)
             (,graph a graphs:Graph)
             ,(if (*use-realms?*)
                  `(UNION ((,rule graphs:graph ,graph))
                          ((,rule graphs:graphType ,gtype)
                           (,graph graphs:type ,gtype)
                           (,graph graphs:realm ,(*realm*))))
                  `(,rule graphs:graph ,graph)))
	    
      ;;(UNION
       (OPTIONAL
        (,s a ,stype)
        (GRAPH ,(*rules-graph*)               
               ,(if (equal? p 'a)
                     `(,rule graphs:predicate rdf:type)
                    `(,rule graphs:predicate ,p))))
       (OPTIONAL (,s a ,stype)
        (GRAPH ,(*rules-graph*) (,rule graphs:subjectType ,stype))))))

;; but if using realms, restrict graphs on realm...
(define (all-graphs)
  (hit-hashed-cache
   *cache* 'all-graphs
   (query-with-vars 
    (graph)
    (s-select 
     '?graph
     (s-triples `((?graph a graphs:Graph))))
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
         (list (remove-trailing-char (cadr decl))
               (write-uri (caddr decl))))
       (filter PrefixDecl? (unit-prologue QueryUnit))))

(define (query-bases QueryUnit)
  (map (lambda (decl)
         (list (cadr decl)
               (write-uri (caddr decl))))
       (map cdr (filter BaseDecl? (unit-prologue QueryUnit)))))

(define (special? quad)
  (case (car quad)
    ((WHERE INSERT DELETE 
	    |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
	    |@()| |@[]| MINUS OPTIONAL UNION)
     #t)
    (else #f)))

(define (type-def triple)
  (match triple
    ((s `a o) (cons s o))
    (else #f)))

(define (type-defs triples)
  (filter values (map type-def triples)))

;; rewrite!
(define (unify-bindings new-bindings old-bindings)
  (append new-bindings old-bindings)) 

(define (rewrite-triple-in-place triples stype statements in-place? graph-statements type-bindings)
  (match (car triples)
    ((s p o)
     (rewrite-triples (cdr triples)
                      (cons (cons s stype) type-bindings)
                      statements: (cons `(GRAPH ,(get-graph stype p) (,s ,p ,o))
                                        (append 
                                         statements))
                      in-place?: in-place?
                      graph-statements: graph-statements))))

(define (update-type-bindings bindings stype p graph)
  (alist-update 'type stype
                (alist-update 'predicates
                              (alist-update p graph 
                                            (or (alist-ref 'predicates bindings) '()))
                              bindings)))

(define (rewrite-triples-query triples stype statements in-place? graph-statements type-bindings)
  (match (car triples)
    ((s p o)
     (let* ((preds (nested-alist-ref type-bindings s 'predicates))
            (bound-graph (and preds (alist-ref p preds)))
            (graph (or bound-graph (new-sparql-variable "graph")))
            (new-graph-statement (if bound-graph
                                     '()
                                     (graph-match-statements graph s stype p))))
       (rewrite-triples (cdr triples)

                        (alist-update s                
                                      (update-type-bindings (or (alist-ref s type-bindings) '())
                                                            stype p graph)
                                      type-bindings)
                        
                        statements: (cons
                                     (cons 
                                      `(GRAPH ,graph (,s ,p ,o))
                                      (if in-place? 
                                          (list new-graph-statement)
                                          '()))
                                     statements)
                        graph-statements:  (if bound-graph
                                               graph-statements
                                               (cons new-graph-statement graph-statements))
                        in-place?: in-place?)))))

(define (rewrite-triples triples type-bindings
                         #!key (statements '()) (in-place? #t) (graph-statements '()) )
  (if (null? triples)
      (values statements graph-statements type-bindings)
      (match (car triples)
	((s p o) (let ((stype ;; (or ;; (alist-ref s type-bindings)
                        (or (nested-alist-ref type-bindings s 'type)
                            (and (iri? s)
                                 (get-type (expand-namespace s (query-namespaces))))
                            (new-sparql-variable "stype"))))
		   (if (and (iri? p) (iri? stype))
                       (rewrite-triples-in-place triples stype statements
                                                 in-place? graph-statements 
                                                 (alist-update s stype type-bindings))
                       (rewrite-triples-query triples stype statements 
                                              in-place? graph-statements 
                                              type-bindings)))))))

(define (rewrite-special group type-bindings #!key in-place?)
  (case (car group)
    ((WHERE INSERT DELETE 
             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
             |@()| |@[]| MINUS OPTIONAL)
     (let-values (((rewritten-quads graph-statements t-bindings)
                   (rewrite-quads (cdr group) type-bindings in-place?: in-place?)))
       (values (list (cons (car group) rewritten-quads))
               graph-statements
               t-bindings)))
    ((UNION)
     (let-values (((rewritten-quads graph-statements t-bindings)
                   (map-values/3 (cute rewrite-quads <> type-bindings in-place?: in-place?) (cdr group))))
       (values (cons (car group) rewritten-quads)
               graph-statements
               (join t-bindings))))
    (else #f)))

(define (flatten-graphs triples)
  (join (map (lambda (triple)
               (if (equal? 'GRAPH (car triple))
                   (cddr triple)
                   (list triple)))
             triples)))

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

(define (rewrite-quads quads #!optional (type-bindings '()) #!key in-place?)
  (print "QUADS " quads) (newline)
  (if (special? quads)
      (let-values (((x y z) (rewrite-special quads type-bindings in-place?: in-place?)))
        (values x y z))
      (let-values (((quads-not-triples triples)
		    (partition special? (flatten-graphs (expand-triples quads)))))
	(let ((new-bindings (unify-bindings (type-defs triples) type-bindings)))
          (let-values (((rewritten-triples graph-statements triples-t-bindings)
                        (rewrite-triples triples new-bindings 
                                         in-place?: in-place?))
                       ((rewritten-quads quads-graph-statements quads-t-bindings)
                        (map-values/3 (cute rewrite-quads <> new-bindings in-place?: in-place?)
                                      quads-not-triples)))
            (values (join (append rewritten-triples rewritten-quads))
                    (append graph-statements quads-graph-statements)
                    (append (join quads-t-bindings)
                            triples-t-bindings)))))))
    
(define (replace-dataset)
  `((@Dataset
     (|FROM NAMED| ,(*rules-graph*))
     ,@(map (lambda (graph) 
              `(FROM ,graph))
            (all-graphs))
     ,@(map (lambda (graph) 
              `(|FROM NAMED| ,graph))
            (all-graphs)))))

(define (replace-using)
  `((@Dataset
     (|USING NAMED| ,(*rules-graph*))
     ,@(map (lambda (graph) 
              `(|USING NAMED| ,graph))
            (all-graphs)))))

(define (rewrite-update-unit unit)
  (let ((where-clause (alist-ref 'WHERE (cdr unit))))
    (let-values (((where-statements _ type-bindings)
                  (if where-clause
                      (rewrite-quads where-clause '() in-place?: #t)
                      (values '() '() '()))))
      (let-values (((parts graph-statements t-bindings)
                    (map-values/3
                   (lambda (part)
                     (case (car part)
                       ((WHERE) (values '() '() '())) ;; placeholder (WHERE)
                       ((DELETE INSERT |INSERT DATA| |DELETE DATA| |DELETE WHERE|)
                        (let-values (((rewritten-quads g-statements _)
                                      (rewrite-quads (cdr part) type-bindings)))
                          (values (cons (car part) rewritten-quads)
                                  g-statements
                                  '())))
                       ((@Dataset) (values (replace-dataset) '() '()))
                       ((@Using) (values (replace-using) '() '()))
                       (else (values part '() '()))))
                   (cdr unit))))
        (alist-update 'WHERE (append where-statements
                                     (join graph-statements))
                      (filter pair? parts))))))

(define (rewrite QueryUnit #!optional realm)
  (parameterize ((query-namespaces (query-prefixes QueryUnit)))
    (map (lambda (unit)
           (case (car unit)
             ((@Update @Query)
              (cons (car unit)
                    (rewrite-update-unit unit)))
             (else unit)))
         (alist-ref '@Unit QueryUnit))))

;; testing

(define-namespace skos "http://www.w3.org/2004/02/skos/core#")
(define-namespace eurostat "http://data.europa.eu/eurostat/")

;(*sparql-endpoint* "http://172.31.63.185:8890/sparql")

(define v (parse-query "PREFIX pre: <http://www.home.com/> 
                  PREFIX rdf: <http://www.gooogle.com/> 

WITH <http://www.google.com/>
DELETE { ?a mu:uuid ?b . ?l ?m ?o }
WHERE {
  ?s ?p ?o . ?x ?y ?z
}"))

(define u (parse-query ;; (car (lex QueryUnit err "
"PREFIX dc: <http://schema.org/dc/> 
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

DELETE WHERE {
   ?a mu:uuid ?b . ?c mu:uuid ?e . <http://data.europa.eu/eurostat/graphs/rules/ECOICOPs> mu:title ?h
  } 

"))

(define s (parse-query "PREFIX pre: <http://www.home.com/> 
                  PREFIX rdf: <http://www.gooogle.com/> 
SELECT ?a ?a
FROM <http://www.google.com/>
FROM NAMED <http://www.google.com/>  
  WHERE {
     GRAPH ?g { ?s ?p ?o }
  } 
"))

(define t (parse-query "PREFIX pre: <http://www.home.com/> 
                  PREFIX rdf: <http://www.gooogle.com/> 
SELECT ?a ?a
FROM <http://www.google.com/>
FROM NAMED <http://www.google.com/>  
  WHERE {
     OPTIONAL { ?s ?p ?o }

    {?a ?b ?c} UNION { ?d ?e ?f }
  } 
"))

(define t2 (parse-query "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

SELECT ?s
  WHERE {
    ?s a skos:Concept.
    ?s ?p ?o
  } 
"))

(define t2b (parse-query "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

INSERT DATA {
    eurostat:ECOICOP4 a skos:Concept .
    eurostat:product1 mu:category eurostat:ECOICOP4 .
  } 
"))

(define t2c (parse-query "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

INSERT {
   ?product mu:category ?ecoicop .
  } 
WHERE {
  ?product mu:category eurostat:ECOICOP2.
  ?ecoicop mu:uuid \"ecoicop1\"
}
"))

;;    eurostat:ECOICOP4 mu:uuid \"ecoicop4\" .

(define t3 (parse-query "
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

SELECT *
  WHERE {
    GRAPH <http://google> {
    OPTIONAL { ?s mu:category ?category }
    OPTIONAL { ?t mu:uuid ?id }
  }
  } 
"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Calls

;; (define (descendance-call relation inverse?)
;;   (rest-call (scheme-id id)

;;  (*handlers* `((GET ("test") ,(lambda (b) `((status . "success"))))
;;                (GET ("schemes") ,concept-schemes-call)

 
