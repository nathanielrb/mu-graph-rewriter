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
  `((GRAPH ,(*rules-graph*)
          (?graph a graphs:Graph)
            
          
          (?rule graphs:predicate ,p)
          (?rule graphs:subjectType ,stype)
           
          ,(if (*use-realms?*)
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
    `((,s a ,stype) ;; ??
      (GRAPH ,(*rules-graph*) 
             (,rule a graphs:GraphRule)
             (,graph a graphs:Graph)
             ,(if (*use-realms?*)
                  `(UNION ((,rule graphs:graph ,graph))
                          ((,rule graphs:graphType ,gtype)
                           (,graph graphs:type ,gtype)
                           (,graph graphs:realm ,(*realm*))))
                  `(,rule graphs:graph ,graph))
             ,(if (equal? p 'a)
                  `(,rule graphs:predicate rdf:type)
                  `(,rule graphs:predicate ,p))
             (,rule graphs:subjectType ,stype)))))

;; add restriction on realms...
(define (all-graphs)
  (hit-hashed-cache
   *cache* 'all-graphs
   (query-with-vars 
    (graph)
    (s-select 
     '?graph
     (s-triples `((GRAPH ,(*rules-graph*)
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
              `(USING ,graph))
            (all-graphs))
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
                        statements: (cons
                                     `((GRAPH ,graph (,s ,p ,o))
                                       ,@(splice-when
                                          (and in-place? new-graph-statement)))
                                     statements)
                        graph-statements:  (if bound-graph graph-statements
                                               (cons new-graph-statement graph-statements))
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
		   (if (and (iri? p) (iri? stype))
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
	    |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
	    |@()| |@[]| MINUS OPTIONAL UNION FILTER)
     #t)
    (else #f)))

(define (rewrite-special group bindings #!key in-place?)
  (case (car group)
    ((WHERE INSERT DELETE |DELETE WHERE| |DELETE DATA| |INSERT WHERE| |@()| |@[]| MINUS OPTIONAL)
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
    (else #f)))

(define (flatten-graphs triples)
  (join (map (lambda (triple)
               (if (equal? 'GRAPH (car triple))
                   (cddr triple)
                   (list triple)))
             triples)))

;; this re-orders the triples and quads, which isn't great...
(define (rewrite-quads quads #!optional (bindings '()) #!key in-place?)
  (if (special? quads)
      (let-values (((x y z) (rewrite-special quads bindings in-place?: in-place?)))
        (values x y z))
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
                            triples-bindings)))))))
    
(define (extract-all-variables where-clause)
  (delete-duplicates (filter sparql-variable? (flatten where-clause))))

(define (rewrite-update-unit-part part bindings where-clause)
  (case (car part)
    ((WHERE) (values '(WHERE) '() '()))
    ((@Dataset) (values (replace-dataset) '() '()))
    ((@Using) (values (replace-using) '() '()))
    ((DELETE INSERT |INSERT DATA| |DELETE DATA| |DELETE WHERE|)
     (let-values (((rewritten-quads graph-statements _)
                   (rewrite-quads (cdr part) bindings)))
       (values (cons (car part) rewritten-quads)
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
        (alist-update 'WHERE (append where-statements (join graph-statements))
                      (filter pair? parts))))))

(define (rewrite QueryUnit #!optional realm)
  (parameterize ((query-namespaces (query-prefixes QueryUnit)))
    (map (lambda (unit)
           (case (car unit)
             ((@Update @Query)
              (cons (car unit)
                    (rewrite-update-unit unit)))
             ((@Prologue)
              `(@Prologue
                (PREFIX |graphs:| <http://mu.semte.ch/graphs/>)
                ,@(cdr unit)))
             (else unit)))
         (alist-ref '@Unit QueryUnit))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  ?ecoicop mu:uuid ?id.
  FILTER( ?id = \"ecoicop1\")
}
"))

;;    eurostat:ECOICOP4 mu:uuid \"ecoicop4\" .

(define t3 (parse-query "
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

SELECT DISTINCT *
  WHERE {
    GRAPH <http://google> {
    OPTIONAL { ?s mu:category ?category }
    OPTIONAL { ?t mu:uuid ?id }
  }
  FILTER( ?s < 45)
  } 
"))

(define t4 (parse-query "
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

INSERT {
  ?s skos:notation \"12345\"
}
  WHERE {
    ?s a skos:Concept .
  } 
"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Calls

;; (define (descendance-call relation inverse?)
;;   (rest-call (scheme-id id)

;;  (*handlers* `((GET ("test") ,(lambda (b) `((status . "success"))))
;;                (GET ("schemes") ,concept-schemes-call)

 
(define *rules-graph* (make-parameter '<http://data.europa.eu/eurostat/graphs>))
(*default-graph* '<http://data.europa.eu/eurostat/graphs>)

(define q1 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ((COUNT (DISTINCT ?uuid)) AS ?count) WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a qb:Observation. 
}
}
"))

(define q2 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?uuid WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a qb:Observation. 
}
} GROUP BY ?uuid OFFSET 0 LIMIT 20
"))

(define q3 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"3bea4b42e69a0360905c837ac12b6515\". 
}
}
"))

(define q4 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT * WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    OPTIONAL {<http://data.europa.eu/eurostat/id/observation/2b8174ad39a111ba85c3544aa4188c5e> eurostat:amount ?amount.}
}
}

"))

(define q5 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
INSERT DATA 
{
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> a schema:Offer.
    <http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> mu:uuid \"594BA222EC3E6C0009000002\".
    <http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:description \"a bag of coke\".
}
}"))

(define q6 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT * WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    OPTIONAL {<http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:description ?description.}
OPTIONAL {<http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:gtin13 ?gtin13.}
OPTIONAL {<http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:identifier ?identifier.}
}
}"))
