(use s-sparql s-sparql-parser)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 

(define *rules* '(((_ mu:uuid) ?graph1)))

(define query-namespaces (make-parameter (*namespaces*)))

(define (query-prefixes QueryUnit)
  (map (lambda (decl)
         (list (remove-trailing-char (cadr decl))
               (write-uri (caddr decl))))
       (filter PrefixDecl? (query-prologue QueryUnit))))

(define (query-bases QueryUnit)
  (map (lambda (decl)
         (list (cadr decl)
               (write-uri (caddr decl))))
       (map cdr (filter BaseDecl? (query-prologue QueryUnit)))))

(define (rule-equal? subject predicate rule)
  (match-let
      ((((rule-subject rule-predicate) graph) rule))
    (and
     (or (equal? rule-subject '_) 
         (equal? (expand-namespace rule-subject)
                 (expand-namespace subject (query-namespaces))))
     (or (equal? rule-predicate '_) 
         (equal? (expand-namespace rule-predicate)
                 (expand-namespace predicate (query-namespaces))))
     graph)))

(define (lookup-rule subject predicate rules)
  (and (not (null? rules))
       (or (rule-equal? subject predicate (car rules))
           (lookup-rule subject predicate (cdr rules)))))

(define (add-graph triple)
  (let ((graph (lookup-rule (car triple) (cadr triple) *rules*)))
    (if graph
        `(GRAPH ,graph ,triple)
        triple)))

(define (join-graphs groups)
  (join
   (map (lambda (sorted-group)
          (if (graph-of (car sorted-group))
              `((GRAPH ,(graph-of (car sorted-group))
                       ,@(join (map cddr sorted-group))))
              sorted-group))
        ((group-by graph-of) 
         (sort groups
               (lambda (a b)
                 (string<= (->string (or (graph-of a) ""))
                           (->string (or (graph-of b) "")))))))))

(define (rewrite group)
  (case (car group)
    ((WHERE) 
     (cons (car group) (join-graphs (map rewrite (cdr group)))))
    ((|@()| |@[]| MINUS OPTIONAL UNION)
     (cons (car group) (join-graphs (map rewrite (cdr group)))))
    ((GRAPH) (list (append (take group 2)
                           (join (map rewrite (cddr group))))))
    (else (if (pair? (car group)) ;; list of triples
              (join-graphs (join (map rewrite group)))
              (map add-graph (expand-triple group)))))) ;; triplesamesubject

(define (rewrite-query query)
  (parameterize ((query-namespaces (query-prefixes t)))
    (map (lambda (unit)
           (case (car unit)
             ((@Query) (cons '@Query (rewrite (query-where query))))
             (else unit)))
         (alist-ref '@QueryUnit query))))
