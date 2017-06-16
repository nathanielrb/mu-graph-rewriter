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

(define graph-counter (make-parameter 0))

(define (graph-statement graph triple)
  (match triple
    ((s p o)
     (let ((rule (new-sparql-variable "rule"))
           (stype (new-sparql-variable "stype"))
           (gtype (new-sparql-variable "gtype")))
       `(((,rule a rewriter:GraphRule)
          (,graph a rewriter:Graph)
          ,(if (*use-realms?*)
               `(UNION ((,rule rewriter:graph ,graph))
                       ((,rule rewriter:graphType ,gtype)
                        (,graph rewriter:type ,gtype)
                        (,graph rewriter:realm ,(*realm*))))
               `(,rule rewriter:graph ,graph))
                
          (UNION ((,rule rewriter:predicate ,p))
                 ((,rule rewriter:subjectType ,stype)
                  (,s a ,stype)))))))))

(define (join-cons conses)
  (fold (lambda (pair accum)
          (cons (cons (car pair) (car accum))
                (cons (cdr pair) (cdr accum))))
        '(() . ())
        conses))

(define (join-cons conses)
  (fold (lambda (pair accum)
          (cons (append (car accum)  (car pair))
                (append (cdr accum) (cdr pair))))
        '(() . ())
        conses))

(define (rewrite-triple triple)
  (let ((graph (new-sparql-variable "graph")))
    (cons
     (graph-statement graph triple)
     `((GRAPH ,graph ,triple)))))

(define (rewrite-triple-path triple-path)
  (join-cons
   (map rewrite-triple
        (expand-triple triple-path))))

(define (rewrite-quads group)
  (or (rewrite-special group)
      (join-cons
       (map rewrite-triple-path
            (expand-triple group)))))

(define (rewrite-special group)
  (case (car group)
    ((WHERE INSERT DELETE 
             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
             |@()| |@[]| MINUS OPTIONAL)
     (match-let (((graph-statements . quads)
                  (join-cons (map rewrite-quads (cdr group)))))
       (cons graph-statements `((,(car group) ,@quads)))))

    ((UNION)
     (match-let (((graph-statements . quads)
                  (join-cons (map rewrite-quads (cdr group)))))
       (cons graph-statements `((,(car group) ,@quads)))))

    ;; remove graph statements -- but what about defaults?
    ((GRAPH)   
     (match-let (((graph-statements . quads)
                  (join-cons (map rewrite-quads (cddr group)))))
       (cons graph-statements quads)))

    ;; list of triples
    (else (and (pair? (car group))
               (match-let (((graph-statements . quads)
                      (join-cons (map rewrite-quads group))))
                 (cons graph-statements (list quads)))))))

;; but if using realms, restrict graphs on realm...
(define (all-graphs)
  (query-with-vars 
         (graph)
         (s-select 
          '?graph
          (s-triples `((?graph a rewriter:Graph))))
         graph))

(define (replace-dataset)
  `((@Dataset
     (FROM ,(*rules-graph*))
     ,@(map (lambda (graph) 
              `(FROM ,graph))
            (all-graphs)))))

(define (rewrite QueryUnit #!optional realm)
    (map (lambda (unit)
           (case (car unit)
             ((@Query) ;; should be redundant except for dataset differences
              (cons (car unit)
                    (map (lambda (part)
                           (case (car part)
                             ((@Dataset) (replace-dataset))
                              
                             ((WHERE)
                              (match-let (((graph-statements . ((`WHERE . quads)))
                                           (rewrite-special part)))
                                `(WHERE ,@quads 
                                        (GRAPH ,(*rules-graph*) ,@graph-statements))))
                              (else part)))
                         (cdr unit))))
             ((@Update) ;; also @Query **
              (cons (car unit)
                    (match-let (((graph-statements . quads-and-others)
                           (join-cons
                            (map (lambda (part)
                                   (case (car part)
                                     ((WHERE INSERT DELETE 
                                             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|)
                                      (rewrite-special part))
                                     (else (cons '() (list part)))))
                                 (cdr unit)))))
                      (map (lambda (pair)
                             (case (car pair)
                               ((@Dataset) `(WITH ,(*rules-graph*)))
                               ((WHERE) `(WHERE ,(cdr pair) 
                                                (GRAPH ,(*rules-graph*) ,@graph-statements)))
                               (else pair)))
                           quads-and-others))))

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

