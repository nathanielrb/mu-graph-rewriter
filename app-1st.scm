(use s-sparql s-sparql-parser mu-chicken-support matchable)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace rewriter "http://mu.semte.ch/graph-rewriter/")

(*default-graph* '<http://mu.semte.ch/graph-rewriter/>)

(define query-namespaces (make-parameter (*namespaces*)))

(define *realm* (make-parameter #f))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Lisp Model (deprecated)

(define *rules* (make-parameter '()))

(define *graph-types* (make-parameter '()))

(define (rule-equal? subject predicate rule)
  (match-let
      ((((rule-subject rule-predicate) graph-rule) rule))
    (and
     (or (equal? rule-subject '_) 
         (equal? (expand-namespace rule-subject)
                 (expand-namespace subject (query-namespaces))))
     (or (equal? rule-predicate '_) 
         (equal? (expand-namespace rule-predicate)
                 (expand-namespace predicate (query-namespaces))))
     (match graph-rule
       ((`graph graph) graph)
       ((`graphType graph-type) 
        (alist-ref (*realm*) (alist-ref graph-type (*graph-types*))))))))

(define (lookup-rule subject predicate #!optional (rules (*rules*)))
  (and (not (null? rules))
       (or (rule-equal? subject predicate (car rules))
           (lookup-rule subject predicate (cdr rules)))))

(*rules* '(((_ mu:uuid) (graph <graph1>))
           ((_ mu:title) (graphType <ScannerData>))))

(*graph-types* '((<ScannerData> . ((AlbertHeijn . <AlbertHeijn>)
                                   (Lidl . <Lidl>)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; RDF Model

(define (get-rule subject predicate)
  (let ((full-predicate (expand-namespace predicate (query-namespaces))))
    (car-when
     (hit-property-cache
      full-predicate (string->symbol (*realm*))
      ;;(let ((subject-type (get-type subject)))
      (let ((subject-type-query (and (not (sparql-variable? subject))
                                     `((,subject a ?subjectType)))))
        (query-with-vars 
         (graph)
         (s-select 
          '?graph
          (s-triples `(
                       ,@(splice-when subject-type-query)
                       (GRAPH ,(*default-graph*)
                              ((?graph a rewriter:Graph)
                               (OPTIONAL (?graph rewriter:realm ,(*realm*)))
                               
                               (UNION ((?rule rewriter:predicate ,full-predicate))
                                      ,@(splice-when (and subject-type-query
                                                          `(((?rule rewriter:subjectType ?subjectType))))))
                   
                               (UNION ((?graph rewriter:type ?type)
                                       (?rule rewriter:graphType ?type))
                                      ((?rule rewriter:graph ?graph)))))))
          from-graph: #f)
         graph))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rewriting

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

 ;; default-graph from WITH **
(define (add-graph triple)
  (match triple
    ((subject predicate _)
      (let ((graph  (and (not (sparql-variable? predicate))
                         (get-rule subject predicate))))
        (if graph ;; (or graph default-graph)
            `(GRAPH ,graph ,triple)
            triple)))))

(define (graph-of group)
  (and (equal? (car group) 'GRAPH)
       (cadr group)))

(define (join-graphs groups)
  (case (car groups)
    ((UNION) (list groups))
    (else (join
           (map (lambda (sorted-group)
                  (if (graph-of (car sorted-group))
                      `((GRAPH ,(graph-of (car sorted-group))
                               ,@(join (map cddr sorted-group))))
                      sorted-group))
                ((group-by graph-of) 
                 (sort groups
                       (lambda (a b)
                         (string<= (->string (or (graph-of a) ""))
                                   (->string (or (graph-of b) "")))))))))))

(define (rewrite-quads group)
  (case (car group)
    ((WHERE INSERT DELETE 
            |DELETE WHERE| |DELETE DATA| |INSERT WHERE|)
     (cons (car group) (join-graphs (join (map rewrite-quads (cdr group))))))
    ((|@()| |@[]| MINUS OPTIONAL)
     (cons (car group) (join-graphs (join (map rewrite-quads (cdr group))))))
    ((UNION)
     (cons (car group) (map rewrite-quads (cdr group))))
    ((GRAPH) (list (append (take group 2)
                           (join (map rewrite-quads (cddr group))))))
    (else (if (pair? (car group)) ;; list of triples
              (join-graphs (join (map rewrite-quads group)))
              (map add-graph (expand-triple group)))))) ;; triplesamesubject

;; transform WITH expressions into explicit Graph scope
;; (for Update only?)
(define (rewrite QueryUnit #!optional realm)
  (parameterize ((query-namespaces (query-prefixes QueryUnit))
                 (*realm* realm))
    (map (lambda (unit)
           (case (car unit)
             ((@Query @Update)
              (cons (car unit)
                    (map (lambda (part)
                           (case (car part)
                             ((WHERE INSERT DELETE 
                                     |DELETE WHERE| |DELETE DATA| |INSERT WHERE|)
                              (rewrite-quads part))
                             (else part)))
                         (cdr unit))))
             (else unit)))
         (alist-ref '@Unit QueryUnit))))

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

