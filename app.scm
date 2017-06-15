(use s-sparql s-sparql-parser matchable)

(require-extension sort-combinators)

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 

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

(define (graph-of group)
  (and (equal? (car group) 'GRAPH)
       (cadr group)))

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

(define (rewrite-quads group)
  (case (car group)
    ((WHERE INSERT DELETE 
            |DELETE WHERE| |DELETE DATA| |INSERT WHERE|)
     (cons (car group) (join-graphs (join (map rewrite-quads (cdr group))))))
    ((|@()| |@[]| MINUS OPTIONAL UNION)
     (cons (car group) (join-graphs (join (map rewrite-quads (cdr group))))))
    ((GRAPH) (list (append (take group 2)
                           (join (map rewrite-quads (cddr group))))))
    (else (if (pair? (car group)) ;; list of triples
              (join-graphs (join (map rewrite-quads group)))
              (map add-graph (expand-triple group)))))) ;; triplesamesubject

;; transform WITH expressions into explicit Graph scope
;; (for Update only?)
(define (rewrite-query QueryUnit)
  (parameterize ((query-namespaces (query-prefixes QueryUnit)))
    (map (lambda (unit)
           (case (car unit)
             ((@Query @Update) (cons (car unit)
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

(define *rules* '(((_ mu:uuid) ?graph1)
                  ((_ _) ?defaultGraph)))
                  

