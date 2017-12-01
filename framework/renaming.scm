;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Bindings, Substitutions and Renamings
(define (current-substitution var bindings)
  (let ((substitutions (get-binding/default 'constraint-substitutions bindings '())))
    (a->rdf:type (alist-ref var substitutions))))

(define (current-substitution-recursive block bindings)
  (let rec ((exp block))
    (cond ((null? exp) '())
	  ((pair? exp) 
           (cons (rec (car exp))
                 (rec (cdr exp))))
	  ((sparql-variable? exp)
           (current-substitution exp bindings))
	  (else exp))))

(define (dependency-substitution var bindings)
  (let ((substitutions (dependency-substitutions))) ;; (get-binding/default 'dependency-substitutions bindings '())))
    (a->rdf:type (alist-ref var substitutions))))

(define (substitution-or-value var bindings)
  (if (sparql-variable? var)
      (current-substitution var bindings)
      var))

(define (deps var bindings)
  (let ((dependencies (renaming-dependencies))) ;; (get-binding/default 'dependencies bindings '())))
    (filter (lambda (v) (member v (or (alist-ref var dependencies) '())))
	    ;;(get-binding/default 'matched-triple bindings '()))))
	    (matched-triple))))

;; (define deps (memoize deps*))

(define (keys* var bindings)
  (map (cut dependency-substitution <> bindings) (deps var bindings)))

(define keys (memoize keys*))

(define (renaming* var bindings)
  (let ((renamings
         (get-binding/default* (keys var bindings) (deps var bindings) bindings '())))
    (alist-ref var renamings)))

(define renaming (memoize renaming*))

(define (update-renaming var substitution bindings)
  (fold-binding* substitution
                 (keys var bindings)
                 (deps var bindings)
                 (cut alist-update var <> <>)
                 '()
                 bindings))

(define (project-substitutions vars substitutions)
  (filter (lambda (substitution)
            (member (car substitution) vars))
          substitutions))

(define constraint-where (make-parameter '()))

(define matched-triple (make-parameter '()))
(define constraint-substitutions (make-parameter '()))
(define dependency-substitutions (make-parameter '()))
(define dependencies  (make-parameter '()))

(define (apply-constraint-rules a b c) ; triple)
  ;; (match triple
  ;;   ((a b c)
  `((,symbol? . ,rw/copy)
    ((@QueryUnit) 
     . ,(lambda (block bindings)
          (rewrite (cdr block) bindings)))
    (,annotation? . ,rw/copy)
    ((@Query)
     . ,(lambda (block bindings)
          (parameterize ((constraint-where (list (get-child 'WHERE (cdr block)))))
                        (rewrite (cdr block) bindings))))
    ((CONSTRUCT) 
     . ,(lambda (block bindings)
          (let-values (((triples _) 
                        (parameterize ((flatten-graphs? #t))
                                      (expand-triples (cdr block)))))
            (let-values (((rw new-bindings) (rewrite triples bindings)))
              (let* ((constraints (filter (compose not fail?) rw))
                     (constraints (if (equal? (length constraints) 1) ; what's this?
                                      (car constraints)
                                      `((UNION ,@constraints))))) ; and this?
                (let ((graphs (find-triple-graphs (list a b c) constraints))) ; is this still used?
                  (if (where?)
                      (values `((*REWRITTEN* ,@constraints)) 
                              (update-triple-graphs graphs (list a b c) 
                                                    new-bindings))
                      (values
                       `((*REWRITTEN* (,a ,b ,c)))
                       (fold-binding constraints;; `((OPTIONAL ,@constraints ))
                                     (if (insert?) 'insert-constraints 'delete-constraints)
                                     append-unique '() 
                                     (update-triple-graphs graphs (list a b c) 
                                                           new-bindings))))))))))
    ((@Dataset) . ,rw/copy)
    (,triple? 
     . ,(lambda (triple bindings)
          (match triple
                 ((s p o)
                  (let ((initial-substitutions (make-initial-substitutions a b c s p o  bindings)))
                    (if initial-substitutions
                        (parameterize ((matched-triple (list s p o))
                                       (dependency-substitutions initial-substitutions))
                                      (let-values (((rw new-bindings)
                                                    (rewrite (constraint-where)
                                                             (update-binding 'constraint-substitutions initial-substitutions bindings)
                                                             rename-constraint-rules)))
                                        (let ((cleaned-bindings #f)) ; (clean-constraint-bindings bindings new-bindings)))
                                          (values (list (get-child-body 'WHERE rw)) ; (get-child-body 'WHERE rw)
                                                  new-bindings))))
                        (values '() bindings)))))))
    (,list? . ,rw/remove)))

(define unique-variable-substitutions
  (memoize (lambda (uvs)
             (map (lambda (var)
                    `(,var . ,(gensym var)))
                  uvs))))

(define (make-initial-substitutions a b c s p o bindings)
  (let ((check (lambda (u v) (or (sparql-variable? u) (sparql-variable? v) (rdf-equal? u v)))))
    (and (check a s) (check b p) (check c o)
	 `((,s . ,a) (,p . ,b) (,o . ,c) 
           ,@(unique-variable-substitutions (*unique-variables*))))))

(define (update-renamings new-bindings)
  (let ((substitutions (get-binding 'constraint-substitutions new-bindings)))
    (fold (lambda (substitution bindings)
	    (update-renaming (car substitution) (cdr substitution) bindings))
	  (delete-binding 'constraint-substitutions new-bindings)
	  substitutions)))

;; rewrite in CPS
(define (new-substitutions exp bindings)
  (let-values (((renamed-exp substitutions)
		(let loop ((exp exp) (renamed-exp '()) (substitutions '()))
		  (if (null? exp) 
		      (values renamed-exp substitutions)
		      (let ((var (car exp)))
			(cond ((pair? var)
			       (let-values (((sub subs) (loop var '() substitutions)))
				 (loop (cdr exp) 
				       (append renamed-exp (list sub)) ;; CPS
				       subs)))
			      ((not (sparql-variable? var))
			       (loop (cdr exp) 
				     (append renamed-exp (list var)) ;; CPS
				     substitutions))
			      ((current-substitution var bindings)
			       => (lambda (v) 
				    (loop (cdr exp) 
					  (append renamed-exp (list v)) ;; CPS
					  substitutions)))
			      ((renaming var bindings)
			       => (lambda (v)
				    (loop (cdr exp)
					  (append renamed-exp (list v))
					  (cons `(,var . ,v) substitutions))))
			      (else (let ((new-var (gensym var)))
				      (loop (cdr exp)
					    (append renamed-exp (list new-var))
					    (cons `(,var . ,new-var) substitutions))))))))))
    (values renamed-exp
	    (update-binding 'constraint-substitutions
			    (append substitutions
				    (get-binding 'constraint-substitutions bindings))
			    bindings))))

(define rename-constraint-triple
  (lambda (triple bindings)
    (let ((graph (get-context-graph)))
      (let-values (((renamed-quad new-bindings) (new-substitutions (cons graph triple) bindings)))
        (values             
         (if (and (use-temp?) (update?) (where?))
             `((UNION (,(cdr renamed-quad)) ((GRAPH ,(*temp-graph*) ,(cdr renamed-quad)))))
             (list (cdr renamed-quad)))
         new-bindings)))))

(define (project-substitution-bindings vars bindings)
  (update-binding 
   'constraint-substitutions 
   (project-substitutions vars (get-binding 'constraint-substitutions bindings))
   bindings))

(define (merge-substitution-bindings vars old-bindings new-bindings)
  (update-binding 'constraint-substitutions
		  (append
		   (project-substitutions
		    vars (get-binding 'constraint-substitutions new-bindings))
		   (get-binding 'constraint-substitutions old-bindings))
		  new-bindings))

;; what about Expressions??
(define (substituted-subselect-vars vars bindings)
  (filter sparql-variable?
	  (map (lambda (var)
		 (if (equal? var '*) var
		     (current-substitution var bindings)))
	       vars)))

(define renaming-dependencies (make-parameter '()))

(define rename-constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    (,annotation? 
     . ,(lambda (block bindings)
          (match block
            ((`@Annotation label key) (values (list block) bindings))
            ((`@Annotation label key var)
             (let-values (((rw new-bindings) (new-substitutions (list var) bindings)))
               (values `((@Annotation ,label ,key ,@rw)) new-bindings))))))
    ((@SubSelect)
     . ,(lambda (block bindings) 
          (match block
	    ((`@SubSelect (label . vars) . rest)
             (let ((subselect-vars (extract-subselect-vars vars)))
             (let-values (((rw new-bindings)
			   (rewrite (get-child-body 'WHERE rest) (project-substitution-bindings subselect-vars bindings))))
               (let ((merged-bindings (merge-substitution-bindings subselect-vars bindings new-bindings)))
               (if (nulll? rw)
                   (values '() merged-bindings)
                   (values `((@SubSelect 
			      (,label ,@(substituted-subselect-vars vars merged-bindings))
			      ,@(replace-child-body 'WHERE rw rest)))
                           merged-bindings)))))))))
    ((WHERE)
     . ,(lambda (block bindings)
	  (parameterize ((renaming-dependencies (constraint-dependencies block bindings)))
	    (let-values (((rw new-bindings)
			  (rewrite (cdr block)
				   (update-binding
				    'dependencies (constraint-dependencies block bindings) 
				    bindings))))
	      (let ((new-bindings (update-renamings new-bindings)))
	      (if (fail? rw)
		  (values `((WHERE #f)) new-bindings)
                  (values `((WHERE ,@rw)) new-bindings)))))))

    ((UNION) . ,rw/union)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
	       (values
		(if (or (nulll? (cdr rw)) 
                        (fail? rw)) '() ; hacky!
                    `((GRAPH ,(substitution-or-value graph new-bindings)
                             ,@(cdr rw))))
                new-bindings))))))
    (,triple? . ,rename-constraint-triple)
    ;; ((VALUES) ;; what about new-bindings (new subs)?
    ;;  . ,(lambda (block bindings)
    ;;       (match block
    ;;         ((`VALUES vars . vals)
    ;;          (let ((renamed-vars 
    ;;                 (if (pair? vars)
    ;;                     (map (cut current-substitution <> bindings) vars)
    ;;                     (current-substitution vars bindings))))
    ;; 	       (values
    ;; 		(list (simplify-values (current-substitution-recursive vars bindings) vals bindings))
    ;; 		bindings))))))
    ((VALUES) ;; what about new-bindings (new subs)?
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (new-substitutions block bindings)))
	    (match rw
	      ((`VALUES vars . vals)
               (let ((vals (simplify-values vars vals bindings)))
                 (if (null? vals) (values '() new-bindings)
                     (values
                      (list vals)
                      new-bindings))))))))

    ((FILTER) 
     . ,(lambda (block bindings) ;; what about new-bindings (new subs)?
	  (let-values (((rw new-bindings) (new-substitutions block bindings)))
	    (validate-filter rw new-bindings))))
    ((BIND) 
     . ,(lambda (block bindings) ;; what about new-bindings (new subs)?
	  (let-values (((rw new-bindings) (new-substitutions block bindings)))
	    (match rw
	      ((`BIND (`AS exp var))
	       (if (sparql-variable? var)
		   (values (list rw) new-bindings)
		   (abort
		    (make-property-condition
		     'exn
		     'message (format "Invalid constrained BIND form: '~A'" 
				      (write-sparql rw))))))))))
    (,quads-block? . ,rw/quads)
    (,list? . ,rw/list)))

