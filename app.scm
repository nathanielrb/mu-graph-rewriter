(use s-sparql s-sparql-parser mu-chicken-support matchable)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace rewriter "http://mu.semte.ch/graph-rewriter/")

(*default-graph* '<http://mu.semte.ch/graph-rewriter/>)

(define *rules-graph* (make-parameter '<http://mu.semte.ch/graph-rewriter/>))

(define query-namespaces (make-parameter (*namespaces*)))

(define *realm* (make-parameter #f))

(define *use-realms?* (make-parameter #f))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rewriting

(define (graph-statement graph triple)
  (match triple
    ((s p o)
     (let ((rule (new-sparql-variable "rule"))
           (stype (new-sparql-variable "stype"))
           (gtype (new-sparql-variable "gtype")))
       `(GRAPH ,(*rules-graph*) 
	       (,rule a rewriter:GraphRule)
	       (,graph a rewriter:Graph)
	       ,(if (*use-realms?*)
		    `(UNION ((,rule rewriter:graph ,graph))
			    ((,rule rewriter:graphType ,gtype)
			     (,graph rewriter:type ,gtype)
			     (,graph rewriter:realm ,(*realm*))))
		    `(,rule rewriter:graph ,graph))
                
	       (UNION ((,rule rewriter:predicate ,p))
		      ((,rule rewriter:subjectType ,stype)
		       (,s a ,stype))))))))

(define (special? quad)
  (case (car quad)
    ((WHERE INSERT DELETE 
	    |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
	    |@()| |@[]| MINUS OPTIONAL UNION GRAPH)
     #t)
    (else #f)))

(define (type-def triple)
  (match triple
    ((s `a o) (list s o))
    (else #f)))

(define (type-defs triples)
  (filter values (map type-def triples)))

;; rewrite!
(define (unify-bindings new-bindings old-bindings)
  (append new-bindings old-bindings)) 

;; should test for:
;; -- is a type def
;; -- known type (in bindings
;; -- literal predicate and/or subject
(define (rewrite-triples triples type-bindings)
  (if (null? triples)
      '()
      (let ((graph (new-sparql-variable "graph"))
	    (triple (car triples)))
	(cons (append
	       (graph-statement graph triple)
	       `((GRAPH ,graph ,triple)))
	      (rewrite-triples (cdr triples) type-bindings)))))

(define (rewrite-special group type-bindings)
  (case (car group)
    ((WHERE INSERT DELETE 
             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
             |@()| |@[]| MINUS OPTIONAL)
     (cons (car group) (rewrite-quads (cdr group) type-bindings)))
    ((UNION)
     (cons (car group) (map (cute rewrite-quads <> type-bindings)  (cdr group))))

    ;; remove graph statements -- but what about defaults?
    ((GRAPH)
     (rewrite-quads (cddr group) type-bindings))

    ;; list of triples
    (else #f)))

(define (rewrite-quads quads #!optional (type-bindings '()))
  (or (rewrite-special quads type-bindings)
      (let-values (((quads-not-triples triples)
		    (partition special? (expand-triples quads))))
	(print "QUADS") (print quads-not-triples) (newline)
	(print "TRIPLES")(print triples)(newline)(newline)
	(let ((new-bindings (unify-bindings (type-defs triples)
					    type-bindings)))
	  (append ((cute rewrite-triples <> new-bindings)  triples)
		   (map (cute rewrite-quads <> new-bindings) quads-not-triples))))))
    
;; but if using realms, restrict graphs on realm...
(define (all-graphs)
  (query-with-vars 
         (graph)
         (s-select 
          '?graph
          (s-triples `((?graph a rewriter:Graph))))
         graph))

(define (all-graphs) '(<http://g1> <http://g2> ))

(define (replace-dataset)
  `((@Dataset
     (FROM ,(*rules-graph*))
     ,@(map (lambda (graph) 
              `(FROM ,graph))
            (all-graphs)))))

(define (rewrite QueryUnit #!optional realm)
    (map (lambda (unit)
           (case (car unit)
             ((@Query)
              (cons (car unit)
                    (map (lambda (part)
                           (case (car part)
                             ((@Dataset) (replace-dataset))
                              
                             ((WHERE) (rewrite-quads (cdr part) '()))
			     (else part)))
                         (cdr unit))))
	     ;; @Update : do WHERE first, pass bindings to DELETE/INSERT
             (else unit)))
         (alist-ref '@Unit QueryUnit)))

;;; testing

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

