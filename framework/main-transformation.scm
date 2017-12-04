;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core Transformation
(define (rewrite-block-name name)
  (case name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else name)))

(define (rewrite-quads-block block bindings)
  (parameterize ((flatten-graphs? (not (preserve-graphs?))))
    (let-values (((q1 b1) (expand-triples block bindings replace-a)))
      (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
        (let-values (((q3 b3) (parameterize ((level-quads (new-level-quads q2)))
                                (rewrite q2 b2))))
          (fail-or-null q3 b3
                        (values (filter pair? q3) b3)))))))

(define (quads-block-rule block bindings)
  (let-values (((rw new-bindings) (rewrite-quads-block (cdr block) bindings)))
    (fail-or-null rw new-bindings
                  (values `((,(rewrite-block-name (car block))
                             ,@(optimize-duplicates rw)))
                          new-bindings))))

(define triples-rules
  `((,triple? 
     . ,(lambda (triple bindings)
          (if (where?)
              (apply-read-constraint triple bindings)
              (apply-write-constraint triple bindings))))
    (,annotation? . ,rw/copy)
    (,list? . ,rw/copy)))

(define (make-dataset label graphs #!optional named?)
  (if named?
      (let ((label-named (symbol-append label '| NAMED|)))
        `(,@(map (lambda (graph) `(,label (NAMED ,graph)))
                 graphs)))
    `(,@(map (lambda (graph) `(,label ,graph))
             graphs))))

(define (select-query-rules)
  (let ((graph (gensym '?graph)))
    `(((GRAPH)
       . ,(rw/lambda (block) 
            (cddr block)))
      ((WHERE) . ,rw/quads)
      ((@SubSelect) . ,rw/subselect)
      ((@Prologue FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
      (,annotation? . ,rw/copy)
      ((@Dataset) 
       . ,(lambda (block bindings)
            (let ((graphs (or (*graphs*) (get-all-graphs (*read-constraint*)))))
              (values `((@Dataset ,@(make-dataset 'FROM graphs #f)))
                      (update-binding 'all-graphs graphs bindings)))))
      (,select? . ,rw/copy)
      ((LIMIT |GROUP BY| ORDER OFFSET) . ,rw/copy)
      (,triple? . ,rw/copy)
      (,list? . ,rw/list))))

(define (update-triples-to-quads triples constraints)
  (rewrite triples '() (update-triples-to-quads-rules constraints)))

(define (update-triples-to-quads-rules constraints)
  `((,triple?
     . ,(lambda (triple bindings)
          (let* ((graphs (find-triple-graphs triple constraints))
                 (graphs* (if (*insert-into-temp?*) graphs
                              (remove (cut equal? (*temp-graph*) <>) graphs))))
            (values (map (lambda (graph) `(GRAPH ,graph ,triple)) graphs*) bindings))))))

(define (instantiated-update-query rw new-bindings)
  (parameterize ((flatten-graphs? #t))
                (let* ((opt (compose apply-optimizations group-graph-statements reorder))
                       (delete (expand-triples (or (get-child-body 'DELETE rw) '()) '() replace-a))
                       (insert (expand-triples (or (get-child-body 'INSERT rw) '()) '() replace-a)))
                  (let-values (((where subs1) (opt (or (get-child-body 'WHERE rw) '()))))
                    (let-values (((insert-constraints subs2) (opt (get-binding/default 'insert-constraints new-bindings '()))))
                      (let-values (((delete-constraints subs3) (opt (get-binding/default 'delete-constraints new-bindings '()))))
                        (let ((instantiated-constraints (instantiate insert-constraints insert)))
                          (let-values (((new-where subs4) (opt (append instantiated-constraints delete-constraints where))))
                          (let* ((uninstantiated-where (opt (append insert-constraints delete-constraints where)))
                                 (new-delete (update-triples-to-quads delete uninstantiated-where))
                                 (new-insert (update-triples-to-quads insert uninstantiated-where)))
                            (values (replace-child-body-if 
                                     'DELETE (and (not (null? new-delete)) new-delete)
                                     (replace-child-body-if
                                      'INSERT (and (not (null? new-insert)) new-insert)
                                      (if (null? new-where)
                                          (replace-child-body 'WHERE '() (reverse rw))
                                          (replace-child-body 
                                           'WHERE `((@SubSelect (SELECT *) (WHERE ,@new-where)))
                                           (reverse rw)))))
                                    (update-binding 'functional-property-substitutions 
                                                    (apply merge-alists (filter pair? (list subs1 subs2 subs3  subs4)))
                                                    new-bindings)))))))))))
(define (all-select-variables)
  (get-vars
   (cdr
    (context-head
     ((next-sibling-axis
       (lambda (context) 
         (let ((head (context-head context)))
           (and head (equal? (car head) 'WHERE)))))
      (*context*))))))

(define (main-transformation-rules)
  `(((@QueryUnit @UpdateUnit) . ,rw/quads)
    ((@Prologue) 
     . ,(lambda (block bindings)
          (values `((@Prologue
                     ,@(append-unique 
			(*constraint-prologues*)
			(cdr block))))
                  bindings)))
    ((@Dataset) . ,rw/remove)
    ((@Using) . ,rw/remove)
    ((@Query)
     . ,(lambda (block bindings)
	  (if (rewrite-select?)
	      (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
                (let-values (((new-where subs) (apply-optimizations (clean (get-child-body 'WHERE rw)))))
                  (values `((@Query ,@(replace-child-body 'WHERE new-where rw)))
                          (update-binding 'functional-property-substitutions subs new-bindings))))
	      (let-values (((rw new-bindings) (rewrite (cdr block) bindings (select-query-rules))))
                (values `((@Query ,@rw)) new-bindings)))))
    ((@Update)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
            (let-values (((rw new-bindings) (instantiated-update-query rw new-bindings)))
              (values `((@Update ,@rw))
                      new-bindings)))))
    ((DELETE |DELETE WHERE| |DELETE DATA|)
     . ,(lambda (block bindings)
          (parameterize ((delete? #t)) (quads-block-rule block bindings))))
    ((INSERT |INSERT WHERE| |INSERT DATA|)
     . ,(lambda (block bindings)
          (parameterize ((insert? #t)) (quads-block-rule block bindings))))
    ((WHERE)
     . ,(lambda (block bindings)
	  (parameterize ((where? #t))
            (let-values (((rw new-bindings) (rewrite-quads-block (cdr block) bindings)))
              (fail-or-null rw new-bindings
                  (values `((,(rewrite-block-name (car block)) 
                             ,@(optimize-duplicates rw)))
                          new-bindings))))))
    (,select?
     . ,(lambda (block bindings)
          (if (equal? block '(SELECT *))
              (values `((SELECT ,@(all-select-variables))) bindings)
              (values (list block) bindings))))
    ((@SubSelect) . ,rw/subselect)
    ((GRAPH) . ,rw/copy)
    ((MINUS OPTIONAL) . ,quads-block-rule)
    ((UNION) . ,rw/union)
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT| |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    (,annotation? . ,rw/copy)
    (,list? 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite-quads-block block bindings)))
            (fail-or-null rw new-bindings
                (values (list (filter pair? rw)) new-bindings)))))
    (,symbol? . ,rw/copy)))

(define (apply-constraints* query)
  (parameterize ((*constraint-prologues* (constraint-prologues))
                 (*namespaces* (constraint-and-query-prefixes query)))
    (rewrite-query query (main-transformation-rules))))

(define apply-constraints (memoize apply-constraints*))