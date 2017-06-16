(use s-sparql s-sparql-parser mu-chicken-support matchable)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace rewriter "http://mu.semte.ch/graph-rewriter/")

(*default-graph* '<http://mu.semte.ch/graph-rewriter/>)

(define query-namespaces (make-parameter (*namespaces*)))

(define *realm* (make-parameter #f))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rewriting

(define graph-counter (make-parameter 0))

(define (graph-statement graph triple)
  (match triple
    ((s p o)
     (let ((rule (new-sparql-variable "rule"))
           (stype (new-sparql-variable "subjecttype"))
           (gtype (new-sparql-variable "graphtype")))
       `((,rule a rewriter:GraphRule)
         (,graph a rewriter:Graph)
         (UNION ((,rule rewriter:graph ,graph))
                ((,rule rewriter:graphType ,gtype)
                 (,graph rewriter:type ,gtype)
                 (,graph rewriter:realm ,(*realm*))))
                 
         (UNION ((,rule rewriter:predicate ,p))
                ((,rule rewriter:subjectType ,stype)
                 (,s a ,stype))))))))

(define (join-cons conses)
  (fold (lambda (pair accum)
          (cons (cons (car pair) (car accum))
                (cons (cdr pair) (cdr accum))))
        '(() . ())
        conses))

(define (rewrite-quads group)
  (or (rewrite-special group)
      (join-cons
       (map (lambda (triple)
              (let ((graph (new-sparql-variable "graph")))
                (cons
                 (graph-statement graph triple)
                 `(GRAPH ,graph ,triple))))
            (expand-triple group)))))

(define (rewrite-special group)
  (case (car group)
    ((WHERE INSERT DELETE 
             |DELETE WHERE| |DELETE DATA| |INSERT WHERE|
             |@()| |@[]| MINUS OPTIONAL)
     (match-let (((graph-statements . quads)
                  (join-cons (map rewrite-quads (cdr group)))))
       (cons graph-statements `(,(car group) ,@quads))))

    ((UNION)
     (match-let (((graph-statements . quads)
                  (join-cons (map rewrite-quads (cdr group)))))
       (cons graph-statements `(,(car group) ,@quads))))

    ;; remove graph statements -- but what about defaults?
    ((GRAPH)   
     (match-let (((graph-statements . quads)
                  (rewrite-quads (cddr group))))
       (cons graph-statements (list quads))))

    ;; list of triples
    (else (and (pair? (car group))
               (join-cons (map rewrite-quads group))))))

(define (rewrite group)
  (case (car group)
    ((WHERE) (match-let (((graph-statements . (`WHERE . quads))
                          ;;(join-cons (map rewrite-quads (cdr group)))))
                          (rewrite-special group)))
               `(WHERE ,@quads ,graph-statements)))))


;;; testing

(define-namespace skos "http://www.w3.org/2004/02/skos/core#")
(define-namespace eurostat "http://data.europa.eu/eurostat/")

(*sparql-endpoint* "http://172.31.63.185:8890/sparql")

(define u (parse-query ;; (car (lex QueryUnit err "
"PREFIX dc: <http://schema.org/dc/> 
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

DELETE WHERE {
   ?a mu:uuid ?b . ?c mu:uuid ?e . <http://data.europa.eu/eurostat/graphs/rules/ECOICOPs> mu:title ?h
  } 

"))

