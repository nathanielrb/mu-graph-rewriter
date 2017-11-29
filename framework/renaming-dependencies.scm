
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Renaming dependencies
(define (constraint-dependencies constraint-where-block bindings)
  (get-dependencies 
   (list constraint-where-block) 
   (map car (dependency-substitutions)))) ;; (get-binding 'dependency-substitutions bindings))))

(define (get-dependencies query bound-vars)
  (rewrite query '() (dependency-rules bound-vars)))
    
;; (define get-dependencies (memoize get-dependencies*))

(define (minimal-dependency-paths source? sink? dependencies)
  (let-values (((sources nodes) (partition (compose source? car) dependencies)))
    (let loop ((paths (join (map (match-lambda ((head . rest) (map (lambda (r) (list r head)) rest)))
                                 sources)))
               (finished-paths '()))
      (if (null? paths) finished-paths
          (let* ((path (car paths))
                 (head (car path))
                 (neighbors (remove (lambda (n) (member n path)) ;; remove loops
                                     (delete head (or (alist-ref head nodes) '())))))
            (if (or (sink? head)
                    (null? neighbors))
                (loop (cdr paths) (cons path finished-paths))
                (loop (append (cdr paths) 
                              (filter values
                                      (map (lambda (n) 
                                             (and (not (source? n)) (cons n path)))
                                           neighbors)))
                      finished-paths)))))))
                                  
(define (concat-dependency-paths paths)
  (let loop ((paths paths) (dependencies '()))
    (if (null? paths)
        (map (lambda (pair) 
               (cons (car pair) (delete-duplicates (cdr pair))))
             dependencies)
        (match (reverse (car paths))
          ((head . rest)
           (loop (cdr paths)
                 (fold (lambda (var deps)
                         (alist-update var (cons head (or (alist-ref var deps) '())) deps))
                       dependencies
                       rest)))))))

(define constraint-graph (make-parameter #f))

(define restricted-variables (make-parameter '()))

;; To do: better use of return values & bindings
(define (dependency-rules bound-vars)
  `((,triple? 
     . ,(lambda (triple bindings)
         (let* ((dependencies (or (get-binding 'dependency-paths bindings) '()))
                (bound? (lambda (var) (member var bound-vars)))
                (vars* (filter sparql-variable? triple))
                (vars (if (and (equal? vars* bound-vars) ; matched triple
                              (null? (lset-intersection equal? (restricted-variables) vars*)))
                          vars*
                          (cons (constraint-graph) vars*))) ; this works, but doesn't seem quite right
                (update-dependency-var  ; how does this work??
                 (lambda (var deps) 
                   (alist-update        ; do this with alist-update-proc ?
                    var (delete-duplicates
                         (append (delete var vars)
                                 (or (alist-ref var deps) '())))
                    deps))))
           (let-values (((bound unbound) (partition bound? vars))) ; unused, it seems
             (values
              (list triple)
              (update-binding
               'dependency-paths
               (fold update-dependency-var dependencies vars)
               bindings))))))
    (,annotation? . ,rw/copy)
    ((GRAPH) . ,(lambda (block bindings)
                   (match block
                     ((`GRAPH graph . rest)
                      (parameterize ((constraint-graph graph))
                        (rewrite rest 
                                 (cons-binding graph 'constraint-graphs bindings)))))))
     ((UNION) . ,rw/union)
     ((OPTIONAL) . ,rw/quads)
     ((VALUES)
      . ,(lambda (block bindings)
           (match block
             ((`VALUES vars . vals)
              (if (symbol? vars)
                  ;; actually depends on other VALUES singletons (?)
                  (values '() bindings)
                  ;; abstract this!
                  (let* ((dependencies (or (get-binding 'dependency-paths bindings) '()))
                         (bound? (lambda (var) (member var bound-vars)))
                         (vars (filter sparql-variable? vars)))
                    (let-values (((bound unbound) (partition bound? vars)))
                      (values
                       '()
                       (update-binding
                        'dependency-paths
                        (fold (lambda (var deps) ; same function as above?
                                (alist-update 
                                 var (delete-duplicates
                                      (append (delete var vars)
                                              (or (alist-ref var deps) '())))
                                 deps))
                              dependencies
                              vars)
                        bindings)))))))))
     ((BIND)
      . ,(lambda (block bindings)
	   (match block
	     ((`BIND (`AS exp var))
	      (values '()
		      (fold-binding var 'dependency-paths
				    (lambda (var paths-list)
				      (fold (lambda (other-var paths)
					      (alist-update-proc other-var
					      		 (lambda (path)
					      		   (cons var (or path '())))
					      		 paths))
					    paths-list
					    (filter sparql-variable? (flatten exp))))
				    '()
				    bindings))))))
     ((@SubSelect)
      . ,(lambda (block bindings)
           (match block
             ((`@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . rest)
              (rewrite (get-child-body 'WHERE rest) bindings)))))
     ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let* ((paths (get-binding 'dependency-paths new-bindings))
                   (source? (lambda (var) (member var bound-vars)))
                   (sink? (lambda (x) (member x (get-binding/default 'constraint-graphs new-bindings '())))))
              (values
               (concat-dependency-paths (minimal-dependency-paths source? sink? paths))
               bindings)))))
     ((FILTER) . ,rw/copy) ;; ??
     (,list? 
      . ,(lambda (block bindings)
           (let ((vars (filter sparql-variable?
                               (flatten (filter (lambda (exp)
                                                  (and (pair? exp) 
                                                        (member (car exp) '(FILTER BIND VALUES))))
                                                      ;; (member (car exp) '(BIND VALUES)))) 
                                                block)))))
             (parameterize ((restricted-variables (append vars (restricted-variables))))
               (rw/list block bindings)))))))
