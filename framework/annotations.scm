;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Annotations
;; bug: annotation in top-level WHERE { @access x . .... }
;; breaks optimizations
(define (annotation? exp)
  (and (pair? exp)
       (equal? (car exp) '@Annotation)))

(define (nulll? block)
  (null? (remove store? (remove annotation? block))))

(define (get-annotations query bindings)
  (let ((rw (delete-duplicates (rewrite-query query (get-annotations-rules))))
        (functional-property-substitutions (get-binding/default 'functional-property-substitutions bindings '())))
    (let-values (((vals annotations) (partition values? rw))) ; generalize this
      (if (null? functional-property-substitutions) annotations
          (map (lambda (annotation) ; like update-annotation-values but keeps ?var as well
                 (match annotation
                        ((key var)
                         (let ((vals (alist-ref var functional-property-substitutions)))
                           (if vals
                               `(,key ,var ,(cons var vals))
                               annotation)))
                        (else annotation)))
               annotations)))))

;; subselects/projection?
(define (get-annotations-rules)
  `(((@QueryUnit @UpdateUnit)
     . ,(lambda (block bindings)
          (let-values (((rw b) (rewrite (cdr block) bindings)))
            (values (list rw) b))))
    ((@Query @Update)
     . ,(lambda (block bindings)
          (rewrite (list (cdr block)) bindings)))
    ((@Query @Update WHERE OPTIONAL)
     . ,(lambda (block bindings)
          (rewrite (cdr block) bindings)))
    (,annotation? 
     . ,(lambda (block bindings)
          (values
           (match block
             ((`@Annotation `access key)  (list key))
             ((`@Annotation `access key var) (list (list key var)))
             (else (error (format "Invalid annotation: ~A" block))))
           bindings)))
    (,select? . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings)
          (match block
            ((@SubSelect (label . vars) . rest)
             (rewrite rest bindings)))))
    ((GRAPH)
     . ,(lambda (block bindings)
          (rewrite (cddr block) bindings)))
    ((UNION) 
     . ,(lambda (block bindings)
          (let-values (((rw b) (rewrite (cdr block))))
            (let ((vals (apply merge-alists (map second (filter values? rw))))
                  (quads (filter (compose not values?) rw)))
            (values (append quads `((*values* ,vals)))
                    b)))))
    (,triple? . ,rw/remove)
    ((|GROUP BY| ORDER LIMIT) . ,rw/remove)
    ((VALUES)
     . ,(lambda (block bindings)
          (match block
            ((`VALUES vars . vals)
             (values
              (if (pair? vars)
                  `((*values*
                    ,(apply
                      merge-alists
                      (map (lambda (vallst)
                             (map (lambda (var val)
                                    (list var
                                          (expand-namespace val (append (*namespaces*) (query-namespaces)))))
                                  vars vallst)) 
                           vals))))
                  `((*values*
                     ,(map (lambda (val)
                             (,vars ,(expand-namespace val)))
                           vals))))
              bindings)))))
    ((@Prologue @Dataset @Using CONSTRUCT SELECT FILTER BIND |GROUP BY| OFFSET LIMIT INSERT DELETE) 
     . ,rw/remove)
    (,list?
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite block bindings)))
            (let-values (((vals annotations) (partition values? rw))) ; generalize this
              (let ((merged-vals (apply merge-alists (map second vals))))
              (values (append (map (lambda (a)
                                     (update-annotation-values a merged-vals))
                                   annotations)
                              `((*values* ,merged-vals)))
                      new-bindings))))))))

(define (update-annotation-values annotation valss)
  (match annotation
    ((key var)
     (let ((vals (alist-ref var valss)))
       (if vals
           `(,key ,var ,vals)
           annotation)))
    (else annotation)))

;; merges alists whose values must be lists
;; e.g., '((a . (1 2 3))) '((a . (4)))
(define (merge-alists #!rest alists)
  (let loop ((alists alists) (accum '()))
    (if (null? alists) accum
        (let inner ((alist (car alists)) (accum accum))
          (if (null? alist) (loop (cdr alists) accum)
              (let ((kv (car alist)))
                (inner (cdr alist)
                       (alist-update-proc (car kv) 
                                          (lambda (current)
                                            (append-unique (or current '()) (cdr kv)))
                                          accum))))))))
;; abstract with above
(define (intersect-alists #!rest alists)
  (let loop ((alists (cdr alists)) (accum (car alists)))
    (if (null? alists) accum
        (let inner ((alist (car alists)) (accum accum))
          (if (null? alist) (loop (cdr alists) accum)
              (let ((kv (car alist)))
                (inner (cdr alist)
                       (alist-update-proc (car kv) 
                                          (lambda (current)
                                            (lset-intersection equal? (or current '()) (cdr kv)))
                                          accum))))))))

(define (values? exp)
  (and (pair? exp) (equal? (car exp) '*values*)))

;; for sandbox only
(define (query-time-annotations annotations)
  (map (lambda (a)
         (match a
                ((key var vals)
                 `(,key ,(string->symbol (string-join (map symbol->string vals)))))
                (else a)))
       (remove values? annotations)))

;; filter through values with rdf-equal?
(define (annotations-query annotations query)
  (if (*calculate-annotations?*)
      (let-values (((pairs singles) (partition pair? annotations)))
        (let* ((pairs (remove values? pairs))
               (vars (filter sparql-variable?  (delete-duplicates (map second pairs)))))
          (if (or (null? pairs) (null? vars)) #f ; what about singles??
              (values (rewrite-query query (query-annotations-rules vars))
                      pairs))))))

(define (query-annotations aquery annotations-pairs)
  (and aquery
       (join
        (map (lambda (row)
               (filter pair?
                       (map (lambda (annotation binding)
                              (match binding
                                     ((var . val)
                                      (if (and (equal? (->string var)
                                                       (substring (->string (second annotation)) 1))
                                               (or (null? (cddr annotation))
                                                   (member val (third annotation))))
                                          (list (first annotation) val)
                                          '()))))
                            annotations-pairs row)))
             (sparql-select (write-sparql aquery))))))

(define (query-annotations-rules vars)
  `(((@UpdateUnit @QueryUnit) 
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings)))
            (values `((@QueryUnit ,@rw)) bindings))))
    ((@Update @Query) 
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings)))
            (values `((@Query
                       ,@(insert-child-before 'WHERE
                                              `(SELECT DISTINCT ,@vars)
                                              rw)))
                    bindings))))
    ((@Using) 
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings)))
            (values `((@Dataset ,@rw)) bindings))))
    ((@Dataset @Prologue WHERE) . ,rw/copy)
    ((INSERT DELETE) . ,rw/remove)
    ((LIMIT OFFSET |GROUP BY|) . ,rw/remove)
    (,select? . ,rw/remove)))

