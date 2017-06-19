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
   ;;(hit-property-cache
   ;;  p (string->symbol (or (*realm*) "_ALL")) ;; ** also stype
   (hit-hashed-cache
    *cache* (list stype p (or (*realm*) "_ALL"))
    (query-with-vars 
     (graph)
     (s-select 
      '?graph
      (s-triples (get-graph-query stype p))
      from-graph: #f)
     graph))))

(define (graph-match-statements graph stype p)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
    `(GRAPH ,(*rules-graph*) 
	    (,rule a graphs:GraphRule)
	    (,graph a graphs:Graph)
	    ,(if (*use-realms?*)
		 `(UNION ((,rule graphs:graph ,graph))
			 ((,rule graphs:graphType ,gtype)
			  (,graph graphs:type ,gtype)
			  (,graph graphs:realm ,(*realm*))))
		 `(,rule graphs:graph ,graph))
	    
	    (UNION ,@(splice-when (and (not (equal? p 'a))
                                      `(((,rule graphs:predicate ,p)))))
		   ((,rule graphs:subjectType ,stype))))))

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

; (define (all-graphs) '(<http://g1> <http://g2> ))

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

;; expand namespaces !
(define (rewrite-triples triples type-bindings #!key (statements '()) (in-place? #t) (graph-statements '()) )
  (if (null? triples)
      (values statements graph-statements)
      (match (car triples)
	((s p o) (let ((stype (or (alist-ref s type-bindings)
				  (and (iri? s) (get-type
						 (expand-namespace
						  s (query-namespaces))))
				  (new-sparql-variable "stype"))))

		   (if (and (iri? p) (iri? stype))
		       ;;(append `((GRAPH ,(get-graph stype p) (,s ,p ,o)))
			 ;;      (rewrite-triples (cdr triples) (cons (cons s stype)
                       ;; type-bindings)))
		       (rewrite-triples (cdr triples) 
                                        (cons (cons s stype) type-bindings)
                                        statements: (cons `(GRAPH ,(get-graph stype p) (,s ,p ,o))
                                                            statements)
                                        in-place?: in-place?
                                        graph-statements: graph-statements)

		       (let* ((graph (new-sparql-variable "graph"))
                              (new-graph-statements (graph-match-statements graph stype p)))

                         (rewrite-triples (cdr triples) type-bindings 
                                          statements: (cons (append
                                                             (if in-place? 
                                                                 new-graph-statements 
                                                                 '())
                                                             `((GRAPH ,graph (,s ,p ,o))))
                                                            statements)
			 
                                          graph-statements: (append
                                                             new-graph-statements
                                                             graph-statements)
                                          in-place?: in-place?))))))))

(define (rewrite-special group type-bindings)
  (case (car group)
    ((WHERE INSERT DELETE 
             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
             |@()| |@[]| MINUS OPTIONAL)
     (cons (car group) (rewrite-quads (cdr group) type-bindings)))
    ((UNION)
     (cons (car group) (map (cute rewrite-quads <> type-bindings)  (cdr group))))

    ;; ((GRAPH)
    ;;  (rewrite-quads (cddr group) type-bindings))

    (else #f)))


(define (rewrite-special group type-bindings)
  (case (car group)
    ((WHERE INSERT DELETE 
             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
             |@()| |@[]| MINUS OPTIONAL)
     (let-values (((rewritten-quads graph-statements)
                   (rewrite-quads (cdr group) type-bindings)))
       (values (cons (car group) rewritten-quads)
               graph-statements)))
    ((UNION)
     (let-values (((rewritten-quads graph-statements)
                   (map-values/2 (cute rewrite-quads <> type-bindings) (cdr group))))
       (values (cons (car group) rewritten-quads)
               graph-statements)))

    ;; ((GRAPH)
    ;;  (rewrite-quads (cddr group) type-bindings))

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

(define (rewrite-quads quads #!optional (type-bindings '()) #!key in-place?)
  (or (rewrite-special quads type-bindings)
      (let-values (((quads-not-triples triples)
		    (partition special? (expand-triples quads))))
	(let ((new-bindings (unify-bindings (type-defs triples) type-bindings)))
          (let-values (((rewritten-triples graph-statements)
                        (rewrite-triples (flatten-graphs triples) new-bindings 
                                         in-place?: in-place?))
                       ((rewritten-quads quads-graph-statements)
                        (map-values/2 (cute rewrite-quads <> new-bindings)
                                      quads-not-triples)))
            (values (join (append rewritten-triples rewritten-quads))
                    (append graph-statements quads-graph-statements)))))))
    
(define (replace-dataset)
  `((@Dataset
     (|FROM NAMED| ,(*rules-graph*))
     ,@(map (lambda (graph) 
              `(|FROM NAMED| ,graph))
            (all-graphs)))))

(define (rewrite QueryUnit #!optional realm)
  (parameterize ((query-namespaces (query-prefixes QueryUnit)))
    (map (lambda (unit)
           (case (car unit)
             ((@Query @Update)
              (cons (car unit)
                    (let* ((where-clause (alist-ref 'WHERE (cdr unit)))
                           (where-statements (or 
                                              (and where-clause
                                                   (rewrite-quads where-clause '()
                                                                  in-place?: #t))
                                              '())))
                      (let-values (((parts graph-statements)
                                    (map-values/2 (lambda (part)
                                                    (print (car part))
                                                    (case (car part)
                                                      ((WHERE) (values '() '()))
                                                      ((DELETE INSERT
                                                               |INSERT DATA| |DELETE DATA|
                                                               |DELETE WHERE|)
                                                       (let-values (((rewritten-quads g-statements)
                                                                     (rewrite-quads (cdr part) '())))
                                                         (values (cons (car part) rewritten-quads)
                                                                 g-statements)))
                                                      (else (values part '()))))
                                                  (cdr unit))))
                        ;; but WHERE isn't always last!
                        (append (reverse (filter pair? parts))
                                (list (cons 'WHERE (append (list where-statements)
                                                           (list graph-statements)))))))))
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
    eurostat:ECOICOP4 a skos:Concept .
    ?product mu:category eurostat:ECOICOP4 .
  } 
WHERE {
  ?product mu:uuid eurostat:uuid4
}
"))

;;    eurostat:ECOICOP4 mu:uuid \"ecoicop4\" .

(define t3 (parse-query "
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

SELECT ?s
  WHERE {
    GRAPH <http://google> {
    ?s mu:category ?category
  }
  } 
"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Calls

;; (define (descendance-call relation inverse?)
;;   (rest-call (scheme-id id)

;;  (*handlers* `((GET ("test") ,(lambda (b) `((status . "success"))))
;;                (GET ("schemes") ,concept-schemes-call)

 
