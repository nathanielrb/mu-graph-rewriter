;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core Transformation
(define (rewrite-block-name name)
  (case name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else name)))

(define (rewrite-quads-block block bindings)
  (parameterize ((flatten-graphs? (not (preserve-graphs?))))
                ;; expand triples
                (let-values (((q1 b1) (expand-triples block bindings replace-a)))
                  ;; rewrite-triples
                  (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
                    ;; continue in nested quad blocks
                    (let-values (((q3 b3)
                                  (parameterize ((level-quads (new-level-quads q2)))
                                                (rewrite q2 b2))))
                      (cond ((nulll? q3) (values '() b3))
                            ((fail? q3) (values (list #f) b3))
                            (else (values (filter pair? q3) b3))))))))

(define (quads-block-rule block bindings)
  (let-values (((rw new-bindings) (rewrite-quads-block (cdr block) bindings)))
    (cond ((nulll? rw) (values '() new-bindings))
          ((fail? rw) (values (list #f) new-bindings))
          (else (values `((,(rewrite-block-name (car block)) 
                           ,@(optimize-duplicates rw)))
                        new-bindings)))))


(define (top-rules)
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
                (let-values (((new-where subs) (apply-optimizations
                                                (group-graph-statements
                                                 (reorder
                                                  (get-child-body 'WHERE rw))))))
                  (values `((@Query 
                             ,@(replace-child-body 
                                'WHERE new-where
                                ;; (clean
                                ;;  (apply-optimizations
                                ;;   (group-graph-statements
                                ;;    (reorder
                                ;;     (get-child-body 'WHERE rw)))))
                                rw)))
                          (update-binding 'functional-property-substitutions subs new-bindings))))
	      (with-rewrite ((rw (rewrite (cdr block) bindings (select-query-rules))))
                            `((@Query ,@rw))))))
    ((@Update)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
                              
	    (if (insert-query? block) ; to do: delete like this as well
                (let-values (((rw new-bindings) (instantiated-insert-query rw new-bindings)))
                  (values
                   `((@Update ,@rw))
                   new-bindings))
		(let ((delete-constraints (get-binding/default 'delete-constraints new-bindings '())))
                  
                  ;; (new-where
                  ;;  (clean
                  ;;   (apply-optimizations 
                  ;;    (group-graph-statements
                  ;;     (reorder
                  ;;      (append (or (get-child-body 'WHERE rw) '())
                  ;;              delete-constraints))))))
                  (let-values (((new-where subs) (apply-optimizations
                                                  (group-graph-statements
                                                   (reorder
                                                    (append (get-child-body 'WHERE rw) ; really append??
                                                            delete-constraints)))))) 
                    (let* ((delete-block (triples-to-quads (get-child-body 'DELETE rw) new-where))
                           ;; (rewrite (get-child-body 'DELETE rw) new-bindings 
                           ;;          (triples-to-quads-rules new-where)))
                           (rw (replace-child-body 'DELETE delete-block rw))
                           (new-bindings (update-binding 'functional-property-substitutions subs new-bindings)))
                      (if (nulll? new-where)
                          (values `((@Update ,@(reverse rw))) new-bindings)
                          (values `((@Update ,@(replace-child-body 
                                                'WHERE 
                                                `((@SubSelect (SELECT *) (WHERE ,@new-where)))
                                                (reverse rw))))
                                  new-bindings)))))))))
    (,select? ; . ,rw/copy)
     . ,(lambda (block bindings)
          (if  (equal? block '(SELECT *))
               (let ((vars
                      (get-vars
                       (cdr
                        (context-head
                         ((next-sibling-axis
                           (lambda (context) 
                             (let ((head (context-head context)))
                               (and head (equal? (car head) 'WHERE)))))
                          (*context*)))))))
                 (values `((SELECT ,@vars)) bindings))
               (values (list block) bindings))))
    ((@SubSelect) . ,rw/subselect)
    ((GRAPH) . ,rw/copy)
    ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    ((DELETE |DELETE WHERE| |DELETE DATA|)
     . ,(lambda (block bindings)
          (parameterize ((delete? #t)) (quads-block-rule block bindings))))
    ((INSERT |INSERT WHERE| |INSERT DATA|)
     . ,(lambda (block bindings)
          (parameterize ((insert? #t)) (quads-block-rule block bindings))))
    ((MINUS OPTIONAL GRAPH) . ,quads-block-rule)
    ((WHERE)
     . ,(lambda (block bindings)
	  (parameterize ((where? #t))
                        (let-values (((rw new-bindings) (rewrite-quads-block (cdr block) bindings)))
                          (cond ((nulll? rw) (values '() new-bindings))
                                ((fail? rw) (values (list #f) new-bindings))
                                (else (values `((,(rewrite-block-name (car block)) 
                                                 ,@(optimize-duplicates rw)))
                                              new-bindings)))))))
    ;; ((WHERE)
    ;;  . ,(lambda (block bindings)
    ;;       (let-values (((rw new-bindings) (rewrite-quads-block block bindings)
    ;; (,quads-block? . ,rewrite-quads-block)
    ((UNION) . ,rw/union)
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT| |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,annotation? . ,rw/copy)
    (,list? 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite-quads-block block bindings)))
            (cond ((nulll? rw) (values '() new-bindings))
                  ((fail? rw) (values (list #f) new-bindings))
                  (else (values (list (filter pair? rw)) new-bindings))))))

    ;;(,list?  . ,rewrite-quads-block)
    ;; . ,(lambda (block bindings)
    ;;      (log-message "~A => ~A" block (expand-triples block bindings replace-a))
    ;;      (selog-message "==> "           (rw/list (expand-triples block bindings replace-a) bindings))
    ;;      (rw/list (expand-triples block bindings replace-a) bindings)))

    (,symbol? . ,rw/copy)))

(define (triples-to-quads triples constraints)
  (rewrite triples '() (triples-to-quads-rules constraints)))

(define (triples-to-quads-rules constraints)
  `((,triple?
     . ,(lambda (triple bindings)
          (let* ((graphs (find-triple-graphs triple constraints))
                 (graphs* (if (*insert-into-temp?*) graphs
                              (remove (cut equal? (*temp-graph*) <>) graphs))))
            (values (map (lambda (graph) `(GRAPH ,graph ,triple)) graphs*) bindings))))))

;; this is a bit circuitous and verbose, but works for now.
;; and a big mess. which one to use, when?? values/not values/mixing with quads/contstraint
;; (insert-block (rewrite insert-block new-bindings (triples-to-quads-rules constraints)))
;; problem: literal graph has to be in VALUES or it's not applied on instantiation
;; e.g., GRAPH ?g { s p o } VALUES (?g){ (<G>) }
(define (instantiated-insert-query rw new-bindings)
  (parameterize ((flatten-graphs? #t))
                (let* ((opt (compose apply-optimizations group-graph-statements reorder))
                       (delete (expand-triples (or (get-child-body 'DELETE rw) '()) '() replace-a))
                       (insert (expand-triples (or (get-child-body 'INSERT rw) '()) '() replace-a)))
                  (let-values (((where subs1) (opt (or (get-child-body 'WHERE rw) '()))))
                    (let-values (((insert-constraints subs2) (opt (get-binding/default 'insert-constraints new-bindings '()))))
                      (let-values (((delete-constraints subs3) (opt (get-binding/default 'delete-constraints new-bindings '()))))
                        (let ((instantiated-constraints (instantiate insert-constraints insert)))
                          (let-values (((new-where subs4) (opt (append instantiated-constraints delete-constraints where))))
                          ;; inefficient!
                          (let* ((uninstantiated-where (opt (append insert-constraints delete-constraints where)))
                                 (new-delete (triples-to-quads delete uninstantiated-where))
                                 (new-insert (triples-to-quads insert uninstantiated-where)))
                            (values (replace-child-body-if 
                                     'DELETE (and (not (null? new-delete)) new-delete)
                                     (replace-child-body-if
                                      'INSERT (and (not (null? new-insert)) new-insert)
                                      (if (null? new-where)
                                          (replace-child-body 'WHERE '() (reverse rw)) ; is this correct?
                                          (replace-child-body 
                                           'WHERE `((@SubSelect (SELECT *) (WHERE ,@new-where)))
                                           (reverse rw)))))
                                    (update-binding 'functional-property-substitutions 
                                                    (apply merge-alists (filter pair? (list subs1 subs2 subs3  subs4)))
                                                    new-bindings)))))))))))

(define (make-dataset label graphs #!optional named?)
  (let ((label-named (symbol-append label '| NAMED|)))
    `(;; (,label ,(*default-graph*))
      ,@(map (lambda (graph) `(,label ,graph))
             graphs)
      ,@(splice-when
         (and named?
              `((,label (NAMED ,(*default-graph*))) 
                ,@(map (lambda (graph) `(,label (NAMED ,graph)))
                       graphs)))))))

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

(define triples-rules
  `((,triple? 
     . ,(lambda (triple bindings)
          (let ((graphs (get-triple-graphs triple bindings)))
            (if (or #t (null? graphs)) ; this seems wrong, once we have different read/write constraints,
                (if (where?)           ; so I turned it off... but is optimize-duplicates enough?
                    (apply-read-constraint triple bindings)
                    (apply-write-constraint triple bindings))
                (values ; not used!
                 (if (where?) '() 
                     `((*REWRITTEN* ,@(list triple))))
                 bindings)))))
    (,annotation? . ,rw/copy)
    (,list? . ,rw/copy)))



(define (rewrite-constraints query)
  (parameterize ((*constraint-prologues*
                  (constraint-prologues)))
  ;; really risky!!
  (parameterize ((*namespaces*
                  (delete-duplicates
                   (append (constraint-prefixes)
                           (query-prefixes query)
                           (*namespaces*))))) ; memoize this!
      (rewrite-query query (top-rules)))))
