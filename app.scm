(use matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client cjson
     memoize)

(import s-sparql mu-chicken-support)
(use s-sparql mu-chicken-support)

(require-extension sort-combinators)

(define *subscribers-file*
  (config-param "SUBSCRIBERSFILE" 
                (if (feature? 'docker)
                    "/config/subscribers.json"
                    "./config/rewriter/subscribers.json")))

(define *subscribers*
  (handle-exceptions exn '()
    (vector->list
     (alist-ref 'potentials
                (with-input-from-file (*subscribers-file*)
                  (lambda () (read-json)))))))

;; Can be a string, an s-sparql expression, 
;; or a thunk returning a string or an s-sparql expression.
;; Used by (constrain-triple) below.
(define *constraint*
  (make-parameter
   `((@QueryUnit
      ((@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE (GRAPH ,(*default-graph*) (?s ?p ?o)))))))))

(define *functional-properties* (make-parameter '()))

(define *plugin*
  (config-param "PLUGIN_PATH" 
                (if (feature? 'docker)
                    "/config/plugin.scm"
                    "./config/rewriter/plugin.scm")))
     
(define *cache* (make-hash-table))

(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define (preserve-graphs?)
  (or (header 'preserve-graph-statements)
      (($query) 'preserve-graph-statements)
      (not (*rewrite-graph-statements?*))))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #t))

(define (rewrite-select?)
  (or (equal? "true" (header 'rewrite-select-queries))
      (equal? "true" (($query) 'rewrite-select-queries))
      (*rewrite-select-queries?*)))

(log-message "~%==Query Rewriter Service==")

(when (*plugin*) 
  (log-message "~%Loading plugin: ~A " (*plugin*))
  (load (*plugin*)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; for RW library, to be cleaned up and abstracted.
(define rw/value
  (lambda (block bindings)
    (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
      (values rw new-bindings))))

(define (extract-subselect-vars vars)
  (filter values
          (map (lambda (var)
                 (if (symbol? var) var
                     (match var
                       ((`AS _ v) v)
                       (else #f))))
               vars)))

(define (subselect-bindings vars bindings)
  (let* ((subselect-vars (extract-subselect-vars vars))
         (inner-bindings (if (or (equal? subselect-vars '(*))
                                 (equal? subselect-vars '*))
                             bindings)))
    (project-bindings subselect-vars bindings)))

(define (merge-subselect-bindings vars new-bindings bindings)
  (merge-bindings
   (project-bindings (extract-subselect-vars vars) new-bindings)
   bindings))

(define (rw/subselect block bindings)
  (match block
    ((@SubSelect (label . vars) . rest)
     (print "Subselect vars: " vars)
     (print "where: " (get-child 'WHERE rest))
     (print "with bindings: " bindings)
     (let* ((subselect-vars (extract-subselect-vars vars))
            (inner-bindings (if (or (equal? subselect-vars '(*))
                                    (equal? subselect-vars '*))
                                bindings
                                (project-bindings subselect-vars bindings))))
       (print "subs vars: "  subselect-vars)
       (print "inner b: " inner-bindings)
       (let-values (((rw new-bindings) (rewrite rest inner-bindings)))
         (print "Rw: " rw)(newline)
         (values `((@SubSelect (,label ,@vars) ,@rw)) 
                 (merge-bindings 
                  (project-bindings subselect-vars new-bindings)
                  bindings)))))))

(define (rw/list block bindings)
  (let-values (((rw new-bindings) (rewrite block bindings)))
    (if (null? rw)
        (values rw new-bindings)
        (values (list rw) new-bindings))))

(define (replace-variables exp bindings)
  (if (null? exp) '()
      (let ((e (car exp))
            (rest (replace-variables (cdr exp) bindings)))
        (if (pair? e)
            (cons (replace-variables e bindings) rest)
            (cons (or (alist-ref e bindings) e) rest)))))

(define (atom-or-cdr a)
  (if (pair? a) (cdr a) a))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility transformations
;; refactor this a bit, to use #!key for mapp and bindingsp
(define flatten-graphs? (make-parameter #f))

(define (expand-triples block #!optional (bindings '()) mapp bindingsp)
  (rewrite block bindings (expand-triples-rules mapp bindingsp)))

(define (recursive-expand-triples block #!optional (bindings '()) mapp bindingsp)
  (rewrite block bindings (recursive-expand-triples-rules mapp bindingsp)))

(define (expand-triple-rule #!optional mapp bindingsp)
  (let ((mapp (or mapp values))
        (bindingsp (or bindingsp (lambda (triples bindings) bindings))))
    (lambda (triple bindings)
      (let ((triples (map mapp (expand-triple triple))))
        (values triples (bindingsp triples bindings))))))

(define (expand-triples-rules #!optional mapp bindingsp)
  `(((FILTER BIND MINUS OPTIONAL UNION
      @SubSelect |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    ((GRAPH) 
     . ,(lambda (block bindings)
	  (if (flatten-graphs?)
	      (rewrite (cddr block) bindings)
	      (values (list block) 
		      (cons-binding (second block) 'named-graphs bindings)))))
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    (,select? . ,rw/copy)
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))

(define (recursive-expand-triples-rules flatten-graph-statements? #!optional mapp bindingsp)
  `((,symbol? . ,rw/copy)
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
            (if (flatten-graphs?)
                (values rw new-bindings)
                (values `((GRAPH ,(second block) ,@rw)) new-bindings)))))
    ((FILTER BIND |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    ((MINUS OPTIONAL UNION) . ,rw/continue)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    (,list? . ,rw/continue)))

(define (expand-graphs statements #!optional (bindings '()))
  (rewrite statements bindings expand-graphs-rules))

(define expand-graphs-rules
  `((,symbol? . ,rw/copy)
    (,triple? . ,rw/copy)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . quads)
             (values (map (lambda (quad)
                            `(GRAPH ,graph ,quad))
                          quads)
                     bindings)))))
    ((FILTER BIND MINUS |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    ((OPTIONAL WHERE UNION) . ,rw/continue)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Axes
(define *in-where?* (make-parameter #f))

(define (in-where?)
  ((parent-axis
    (lambda (context) 
      (let ((head (context-head context)))
        (and head (equal? (car head) 'WHERE)))))
   (*context*)))

(define (get-context-graph)
  (match (context-head
          ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
           (*context*)))
    ((`GRAPH graph . rest) graph)
    (else #f)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core Transformation
(define replace-a
  (match-lambda ((s `a o) `(,s rdf:type ,o))
                ((s p o)  `(,s ,p ,o))))

(define (collect-functional-properties triples bindings)
  (fold (match-lambda* 
	  (((s p o) bindings) 
	   (update-binding s p 'functional-property o bindings)))
        bindings
        (filter (match-lambda ((s p o) (member p (*functional-properties*))))
                triples)))

(define (rewrite-quads-block block bindings)
  (print "rewriting " (car block))
  (parameterize ((flatten-graphs? (not (preserve-graphs?))))
    ;; expand triples
    (let-values (((q1 b1) (expand-triples (cdr block) bindings replace-a collect-functional-properties)))
      (print "q1 " q1)
      ;; rewrite-triples
      (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
        (print "q2 " q2)
        ;; continue in nested quad blocks
        (let-values (((q3 b3) (rewrite q2 b2)))
          (if (null? q3) (values '() b3)
              (values `((,(rewrite-block-name (car block)) ,@q3)) b3)))))))

(define (rewrite-block-name name)
  (case name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else name)))

(define (get-child label block)
  (assoc label block))

(define (get-child-body label block)
  (alist-ref label block))

(define (replace-child-body label replacement block)
  (alist-update label replacement block))

(define (append-child-body/before label new-statements block)
  (replace-child-body label (append new-statements (or (get-child-body label block) '())) block))

(define (append-child-body/after label new-statements block)
  (replace-child-body label (append (or (get-child-body label block) '()) new-statements) block))

(define (insert-child-before head statement statements)
  (cond ((null? statements) '())
        ((equal? (caar statements) head)
         (cons statement statements))
        (else (cons (car statements)
                    (insert-child-before head statement (cdr statements))))))

(define (insert-child-after head statement statements)
  (cond ((null? statements) '())
        ((equal? (caar statements) head)
         (cons (car statements)
	       (cons statement (cdr statements))))
        (else (cons (car statements)
                    (insert-child-after head statement (cdr statements))))))

(define top-rules
  `((,symbol? . ,rw/copy)
    ((@QueryUnit @UpdateUnit) . ,rw/continue)
    ((@Prologue)
     . ,(lambda (block bindings)
          (values `((@Prologue
                     (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
                     ,@(cdr block)))
                  bindings)))
    ((@Query)
     . ,(lambda (block bindings)
	  (if (rewrite-select?)
	      (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
		(let ((constraints (get-binding/default 'constraints new-bindings '())))
		  (values `((@Query ,@(append-child-body/before 'WHERE constraints rw)))
			  new-bindings)))
	      (with-rewrite ((rw (rewrite (cdr block) bindings select-query-rules)))
	        `((@Query ,rw))))))
    ((@Update)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
            (let ((where-block (or (alist-ref 'WHERE rw) '()))
		  (constraints (get-update-constraints block new-bindings)))
	      (let ((new-where (append constraints where-block)))
		(if (null? new-where)
		    (values `((@Update ,@(reverse rw))) new-bindings)
		    (values `((@Update 
			       ,@(replace-child-body
				  'WHERE `((@SubSelect (SELECT *) (WHERE ,@new-where)))
				  (reverse rw))))
			    new-bindings)))))))
    ((@Dataset) . ,rw/remove)
    ((@Using) . ,rw/remove)
    ((GRAPH) . ,rw/copy)
    ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    (,quads-block? . ,rewrite-quads-block)
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT| |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

(define (get-update-constraints block bindings)
  (let ((insert-block (or (get-child-body '|INSERT DATA| (cdr block))
			  (get-child-body 'INSERT (cdr block)))))
    (if insert-block
	(parameterize ((flatten-graphs? #t))
	  (let ((triples (expand-triples insert-block '() replace-a)))
	    (instantiate (get-binding/default* '() 'constraints bindings '()) triples)))
	(get-binding/default* '() 'constraints bindings '()))))

(define (make-dataset label graphs #!optional named?)
  (let ((label-named (symbol-append label '| NAMED|)))
    `((,label ,(*default-graph*))
      ,@(map (lambda (graph) `(,label ,graph))
             graphs)
      ,@(splice-when
         (and named?
              `((,label-named ,(*default-graph*))
                ,@(map (lambda (graph) `(,label-named ,graph))
                       graphs)))))))

(define select-query-rules
 `((,triple? . ,rw/copy)
   ((GRAPH)
    . ,(rw/lambda (block) 
         (cddr block)))
   ((@SubSelect FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
   ((@Dataset) 
    . ,(lambda (block bindings)
         (let ((graphs (get-all-graphs (*constraint*))))
	   (values `((@Dataset ,@(make-dataset 'FROM graphs #f))) 
		   (update-binding 'all-graphs graphs bindings)))))
   (,select? . ,rw/copy)
   (,list? . ,rw/list)))

(define (a->rdf:type b)
  (if (equal? b 'a) 'rdf:type b))

(define (gtg vars triple bindings)
  (or
   (cdr-when (assoc triple (get-binding/default* vars 'graphs bindings '())))
   '()))

(define (get-triple-graphs triple bindings)
  (let ((vars (filter sparql-variable? triple))
        (triple (map a->rdf:type triple)))
    (gtg vars triple bindings)))

(define (update-triple-graph graph triple bindings)
  (let ((vars (filter sparql-variable? triple))
        (triple (map a->rdf:type triple)))
    (fold-binding* triple
                   vars
                   'graphs
                   (lambda (triple graphs-list)
                     (alist-update triple
                                   (cons graph (gtg vars triple bindings))
                                   graphs-list))
                   '()
                   bindings)))

(define triples-rules
  `((,triple? 
     . ,(lambda (triple bindings)
          (parameterize ((*in-where?* (in-where?)))
            (let ((graphs (get-triple-graphs triple bindings)))
              (if (null? graphs)
                  (apply-constraint triple bindings)
		  (values
		   (if (*in-where?*) '() 
		       (map (lambda (graph) `(GRAPH ,graph ,triple))
			    graphs))
		   bindings))))))
    (,list? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Graphs
(define (get-all-graphs query)
  ;; (hit-hashed-cache
  ;; *cache* query
  (let ((query (if (procedure? query) (query) query)))
    (let-values (((rewritten-query bindings) (rewrite-query query extract-graphs-rules)))
      (let ((query-string (write-sparql rewritten-query)))
        (let-values (((vars iris) (partition sparql-variable? (get-binding/default 'all-graphs bindings '()))))
          (if (null? vars) iris
              (delete-duplicates
               (append iris
                       (map cdr (join (sparql-select query-string from-graph: #f)))))))))))

(define extract-graphs-rules
  `(((@QueryUnit @UpdateUnit WHERE) . ,rw/continue)
    ((@Query @Update) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let ((graphs (delete-duplicates
                           (filter sparql-variable?
                                   (get-binding/default 'all-graphs new-bindings '())))))
            (values `((@Query
                       ,@(insert-child-before 'WHERE `(|SELECT DISTINCT| ,@graphs) rw)))
                    new-bindings)))))
    ((@Prologue) . ,rw/copy)
    ((@Dataset @Using) 
     . ,(lambda (block bindings)
          (let ((graphs (map second (cdr block))))
            (values (list block) (fold-binding graphs 'all-graphs append '() bindings)))))
    ((GRAPH) . ,(lambda (block bindings)
                  (match block
                    ((`GRAPH graph . quads)
                     (let-values (((rw new-bindings) 
                                   (rewrite quads (cons-binding graph 'all-graphs bindings))))
                       (values `((GRAPH ,graph ,@rw)) new-bindings))))))
    (,select? . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings)
          (match block
            ((@SubSelect (label . vars) . rest)
	     (let* ((dataset (get-child-body '@Dataset rest))
		    (graphs (and dataset (map second dataset))))
               (let-values (((rw new-bindings) (rewrite (get-child-body 'WHERE rest) bindings)))
		 (values
		  `((@SubSelect (,label ,@vars) ,@(replace-child-body 'WHERE rw rest)))
		  (if graphs
		      (fold-binding graphs 'all-graphs append '() new-bindings)
		      new-bindings))))))))
    (,triple? . ,rw/copy)
    ((CONSTRUCT SELECT INSERT DELETE) . ,rw/remove)
    ((UNION) 
     . ,(rw/lambda (block)
          (with-rewrite ((rw (rewrite (cdr block))))
	    `((UNION ,@rw)))))
    (,quads-block? . ,rw/continue)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Apply CONSTRUCT statement as a constraint on a triple a b c
(define (parse-constraint* constraint)
  (let ((constraint
         (if (pair? constraint) constraint (parse-query constraint))))
    (recursive-expand-triples constraint)))

(define parse-constraint (memoize parse-constraint*))

(define apply-constraint
  (let ((constraint (*constraint*)))
    (if (procedure? constraint)
         (let ((C (lambda () (parse-constraint (constraint)))))
           (*constraint* C)
           (lambda (triple bindings)
             (rewrite (list (C)) bindings (apply-constraint-rules triple))))
         (let ((C (parse-constraint constraint)))
           (*constraint* C)
           (lambda (triple bindings)
             (rewrite (list C) bindings (apply-constraint-rules triple)))))))

(define (current-substitution var bindings)
  (let ((sub (alist-ref var (get-binding/default 'constraint-substitutions bindings '()))))
    (a->rdf:type sub)))
    ;;(if (equal? sub 'a) 'rdf:type sub)))

(define (dependency-substitution var bindings)
  (let ((sub (alist-ref var (get-binding/default 'dependency-substitutions bindings '()))))
    (a->rdf:type sub)))
;; (if (equal? sub 'a) 'rdf:type sub)))

(define (substitution-or-value var bindings)
  (if (sparql-variable? var)
      (current-substitution var bindings)
      var))

(define (deps* var bindings)
  (let ((dependencies (get-binding/default 'dependencies bindings '())))
    (filter (lambda (v) (member v (or (alist-ref var dependencies) '())))
            (get-binding/default 'matched-triple bindings '()))))

(define deps (memoize deps*))

(define (keys* var bindings)
  (map (cut dependency-substitution <> bindings) (deps var bindings)))

(define keys (memoize keys*))

(define (renaming* var bindings)
  (alist-ref var 
             (get-binding/default* (keys var bindings)
                                   (deps var bindings) 
                                   bindings
                                   '())))

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

;; (define (remove-renaming var bindings)
;;  (delete-binding* (keys var bindings) (deps var bindings) bindings))

;; (define (project-renamings ..) 

;; (define (merge-renamings ...)

;;(define (apply-constraint-rule 

(define (apply-constraint-rules triple)
  (match triple
    ((a (`^ b) c) (apply-constraint-rules `(,c ,b ,a)))
    ((a b c)
     `( (,symbol? . ,rw/copy)
        ((@QueryUnit) 
         . ,(lambda (block bindings)
	      (rewrite (cdr block) bindings)))
        ((@Query)
         . ,(lambda (block bindings)
              (let ((where (get-child 'WHERE (cdr block))))
                (let-values (((rw new-bindings)
                              (rewrite (cdr block) (update-binding 'constraint-where (list where) bindings))))
                  (values rw (delete-bindings (('constraint-where)) new-bindings))))))
        ((CONSTRUCT) 
         . ,(lambda (block bindings)
              (let-values (((triples _) 
			    (parameterize ((flatten-graphs? #t))
			      (expand-triples (cdr block)))))
                (rewrite triples bindings))))
	((@Dataset) . ,rw/copy)
        (,triple? 
         . ,(lambda (triple bindings)
              (match triple
                ((s p o)
		 (let-values (((rw new-bindings)
                                 (rewrite (get-binding 'constraint-where bindings)
					  (make-constraint-bindings a b c s p o bindings)
                                          rename-constraint-rules)))
		     (let ((cleaned-bindings (clean-constraint-bindings bindings new-bindings))
			   (constraint (constraint-with-fproperty-replacements rw new-bindings)))
                       (if (*in-where?*)
                           (values `((*REWRITTEN* ,@constraint)) cleaned-bindings)
                           (values
                            (map (lambda (graph) `(GRAPH ,graph (,a ,b ,c)))
                                 (constraint-graphs a b c new-bindings))
                            (fold-binding constraint 'constraints append '() cleaned-bindings)))))))))
         (,pair? . ,rw/remove)))))

(define (make-constraint-bindings a b c s p o bindings)
  (let ((initial-substitutions `((,s . ,a) (,p . ,b) (,o . ,c))))
    (update-bindings (('constraint-substitutions initial-substitutions)
		      ('dependency-substitutions initial-substitutions)
		      ('matched-triple (list s p o)))
		     bindings)))

(define (constraint-graphs a b c new-bindings)
  ;; (let ((b (if (equal? b 'a) 'rdf:type b)))
    ;; (cdr-when
    ;;  (assoc
    ;;(list a b c) 
    ;;(get-binding/default 'triple-graphs new-bindings '())))))
    (get-triple-graphs (list a b c) bindings))

(define (clean-constraint-bindings old-bindings new-bindings)
  (let ((substitutions (get-binding 'constraint-substitutions new-bindings)))
    (delete-bindings (('matched-triple) 
		      ('constraint-substitutions)
		      ('dependency-substitutions) 
		      ('dependencies))
		     (fold (lambda (substitution bindings)
			     (update-renaming (car substitution) (cdr substitution) bindings))
			   new-bindings
			   substitutions))))

(define (constraint-with-fproperty-replacements block bindings)
  (let ((fpbs (get-binding 'fproperties-bindings bindings))
	(constraint (get-child-body 'WHERE block)))
    (if fpbs
	(replace-variables constraint fpbs)
	constraint)))
                             
(define (new-substitutions graph triple bindings)
  (let loop ((vars (cons graph triple)) (substitutions '()) (new-bindings bindings))
    (if (null? vars) 
	(let ((renamed-quad (map atom-or-cdr (reverse substitutions))))
	  (values (car renamed-quad)
		  (cdr renamed-quad)
		  (update-binding 'constraint-substitutions
				  (append (filter pair? substitutions)
					  (get-binding 'constraint-substitutions bindings))
				  new-bindings)))
	(let ((var (car vars)))
	  (cond ((not (sparql-variable? var))
		 (loop (cdr vars) (cons var substitutions) new-bindings))
		((current-substitution var bindings) => (lambda (v) 
							  (loop (cdr vars) (cons v substitutions) 
								new-bindings)))
		((renaming var new-bindings) => (lambda (v)
						  (loop (cdr vars)
							(cons `(,var . ,v) substitutions)
							new-bindings)))
		(else (let ((new-var (gensym var)))
			(loop (cdr vars)
			      (cons `(,var . ,new-var) substitutions)
			      new-bindings))))))))

(define (update-fproperty-bindings triple bindings)
  (let ((fproperty (match triple ((s p o) (get-binding s p 'functional-property bindings)))))
    (if (and fproperty (sparql-variable? (third triple)))
	(cons-binding (cons (third triple) fproperty) 
		      'fproperties-bindings bindings)
	bindings)))
	      
(define rename-constraint-triple
  (lambda (triple bindings)
    (let ((graph (get-context-graph)))
      (let-values (((renamed-graph renamed-triple new-bindings) (new-substitutions graph triple bindings)))
          (if (member renamed-graph (get-triple-graphs renamed-triple new-bindings))
              (values '() new-bindings)
              (values               
               (list renamed-triple)
	       (update-triple-graph renamed-graph renamed-triple
				    (update-fproperty-bindings renamed-triple new-bindings))))))))

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

(define rename-constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings) 
          (match block
	    ((`@SubSelect (label . vars) . rest)
             (let ((subselect-vars (extract-subselect-vars vars)))
             (let-values (((rw new-bindings)
			   (rewrite (get-child-body 'WHERE rest) (project-substitution-bindings subselect-vars bindings))))
               (let ((merged-bindings (merge-substitution-bindings subselect-vars bindings new-bindings)))
               (if (null? rw)
                   (values '() merged-bindings)
                   (values `((@SubSelect 
			      (,label ,@(substituted-subselect-vars 
					 vars merged-bindings))
			      ,@(replace-child-body 'WHERE rw rest)))
                           merged-bindings)))))))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings)
                        (rewrite (cdr block)
                                 (update-binding
                                  'dependencies (constraint-dependencies block bindings)
                                  bindings))))
            (values `((WHERE ,@rw)) new-bindings))))
    ((UNION) . ,rw/continue)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
	       (values
		(if (null? (cdr rw)) '()
                    `((GRAPH ,(substitution-or-value graph new-bindings)
                             ,@(cdr rw))))
                new-bindings))))))
    (,triple? . ,rename-constraint-triple)
    (,pair? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constraint variable dependencies
(define (constraint-dependencies constraint-where-block bindings)
  (print "where: " constraint-where-block)
  (print "bound: " (map car (get-binding 'dependency-substitutions bindings)))
  (newline)
  (get-dependencies 
   (list constraint-where-block) 
   (map car (get-binding 'dependency-substitutions bindings))))

(define (get-dependencies* query bound-vars)
  (rewrite query '() (dependency-rules bound-vars)))
    
(define get-dependencies (memoize get-dependencies*))

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

(define (dependency-rules bound-vars)
  `((,triple? 
     . ,(lambda (triple bindings)
         (let* ((dependencies (or (get-binding 'dependencies bindings) '()))
                (bound? (lambda (var) (member var bound-vars)))
                (vars (filter sparql-variable? triple)))
           (let-values (((bound unbound) (partition bound? vars)))
             (values
              (list triple)
              (update-binding
               'dependencies
               (fold (lambda (var deps)
                       (alist-update 
                        var (delete-duplicates
                             (append (delete var vars)
                                     (or (alist-ref var deps) '())))
                        deps))
                     dependencies
		     vars)
               bindings))))))
     ((GRAPH) . ,(lambda (block bindings)
                   (match block
                     ((`GRAPH graph . rest)
                      (rewrite rest 
                               (cons-binding graph 'constraint-graphs bindings))))))
     ((UNION OPTIONAL) . ,rw/continue)
     ((@SubSelect)
      . ,(lambda (block bindings)
           (match block
             ((`@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) 
			   (`WHERE . quads) . rest)
              (rewrite quads bindings)))))
     ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let* ((deps (get-binding 'dependencies new-bindings))
                   (source? (lambda (var) (member var bound-vars)))
                   (sink? (lambda (x) (member x (get-binding/default 'constraint-graphs new-bindings '())))))
              (values
               (concat-dependency-paths (minimal-dependency-paths source? sink? deps))
               bindings)))))
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instantiation for INSERT and INSERT DATA
;; to do: expand namespaces before checking
(define (instantiate where-block triples)
  (let ((where-block (expand-graphs where-block)))
    (let-values (((rw new-bindings)
                  (rewrite where-block '() (get-instantiation-matches-rules triples))))
      (rewrite rw new-bindings instantiation-union-rules))))

;; matches a triple against a list of triples, and if successful returns a binding list
;; ex: (match-triple '(?s a dct:Agent) '((<a> <b> <c>) (<a> a dct:Agent)) => '((?s . <a>))
;; but should differentiate between empty success '() and failure #f
(define (match-triple triple triples)
  (if (null? triples) '()
      (let loop ((triple1 triple) (triple2 (car triples)) (match-binding '()))
        (cond ((null? triple1) 
               (if (null? match-binding)
                   (match-triple triple (cdr triples))
                   (cons match-binding (match-triple triple (cdr triples)))))
              ((sparql-variable? (car triple1))
               (loop (cdr triple1) (cdr triple2) (cons (cons (car triple1) (car triple2)) match-binding)))
              ((equal? (car triple1) (car triple2))
               (loop (cdr triple1) (cdr triple2) match-binding))
              (else (match-triple triple (cdr triples)))))))
    
(define (instantiate-triple triple binding)
  (let loop ((triple triple) (new-triple '()))
    (cond ((null? triple) (reverse new-triple))
          ((sparql-variable? (car triple))
           (loop (cdr triple) (cons (or (alist-ref (car triple) binding) (car triple)) new-triple)))
          (else (loop (cdr triple) (cons (car triple) new-triple))))))

(define (expand-instantiation graph triple match-binding)
  (let ((instantiation (instantiate-triple triple match-binding)))
    (list `((GRAPH ,graph ,triple))
	  (if (null? (filter sparql-variable? instantiation))
	      '()
	      `((GRAPH ,graph ,instantiation)))
	  match-binding)))

;; Given a list of triples from the INSERT or INSERT DATA block,
;; accumulates matched instantiations in the binding 'instantiated-quads
;; in the form '(matched-quad instantiated-quad match-binding)
;; or '(matched-quad () match-binding) if the instantiated quad is instantiated completely.
;; To do: group matches by shared variables (i.e., (map car match-binding) )
(define (get-instantiation-matches-rules triples)
  `(((@SubSelect) . ,rw/subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (if (member triple triples)
                 (values '() bindings)
                 (let ((match-bindings (match-triple triple triples)))
                   (if (null? match-bindings)
                       (values (list block) bindings)
                       (values '() 
                               (fold-binding (map (cut expand-instantiation graph triple <>) match-bindings)
					     'instantiated-quads 
					     append '() bindings)))))))))
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    ((OPTIONAL UNION) . ,rw/continue)
    (,quads-block? . ,rw/continue)
    (,list? . ,rw/list)
    ))

(define (match-instantiated-quads triple bindings)
  (filter values
	  (map (match-lambda
		 ((a b binding)
		  (let ((vars (map car binding)))
		    (and (any values (map (cut member <> vars) triple))
			 (list a b binding)))))
	       (get-binding/default 'instantiated-quads bindings '()))))

(define instantiation-union-rules
  `(((@SubSelect) 
     ;; necessary to project here?? probably yes
     . ,(lambda  (block bindings)
          (match block
            ((`@SubSelect (label . vars) . rest)
             (let-values (((rw new-bindings)
			   (rewrite (get-child-body 'WHERE rest) 
				    (subselect-bindings vars bindings))))
               (values
                `((@SubSelect (,label ,@vars)
			      ,@(replace-child-body 'WHERE (group-graph-statements rw) rest)))
                (merge-subselect-bindings vars new-bindings bindings)))))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (let ((matching-quads (match-instantiated-quads triple bindings)))
               (if (null? matching-quads)
                   (values (list block) bindings)
                   (values 
                    (union-over-instantiation block matching-quads bindings)
                    bindings)))))))
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    ((WHERE UNION OPTIONAL MINUS)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (values `((,(car block) ,@(group-graph-statements rw))) new-bindings))))
    (,list? . ,rw/list)
    ))

(define (union-over-instantiation block matching-quads bindings)
  (match block
    ((`GRAPH graph triple)
     (match (car matching-quads)
       ((matched-quad-block instantiated-quad-block binding)
        (rewrite
         `((UNION (,block ,@matched-quad-block)
                  ((GRAPH ,graph ,(instantiate-triple triple binding))
                   ,@instantiated-quad-block)))
         (update-binding 'instantiated-quads (cdr matching-quads)  bindings)
         instantiation-union-rules))))))

(define (group-graph-statements statements)
  (let loop ((statements statements) (graph #f) (statements-same-graph '()))
    (cond ((null? statements) 
           (if graph `((GRAPH ,graph ,@statements-same-graph)) '()))
          (graph
           (match (car statements)
             ((`GRAPH graph2 . rest) 
              (if (equal? graph2 graph)
                  (loop (cdr statements) graph (append statements-same-graph rest))
                  (cons `(GRAPH ,graph ,@statements-same-graph)
                        (loop (cdr statements) graph2 rest))))
             (else (cons `(GRAPH ,graph ,@statements-same-graph)
                         (cons (car statements)
                               (loop (cdr statements) #f '()))))))
          (else
           (match (car statements)
             ((`GRAPH graph . rest) 
              (loop (cdr statements) graph rest))
             (else (cons (car statements) (loop (cdr statements) #f '()))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deltas
(define (update-query? query)
  ;;((list-of? update-unit?)
  ;;(alist-ref '@UpdateUnit query)))
  (equal? (car query) '@UpdateUnit))

(define (select-query? query)
  ;;((list-of? update-unit?)
  ;;(alist-ref '@UpdateUnit query)))
  (equal? (car query) '@QueryUnit))

(define (update-unit? unit)
  (alist-ref '@Update unit))

(define (construct-statement triple #!optional graph)
  (match triple
    ((s p o)
     `((,s ,p ,o)
       ,@(splice-when
          (and graph
               `((,s ,p ,graph)
                 (rewriter:Graphs rewriter:include ,graph))))))))

(define (construct-statements statements)
  (and statements
       `(CONSTRUCT
         ,@(join
            (map (lambda (statement)
                   (match statement
                     ((`GRAPH graph . triples)
                      (join (map (cut construct-statement <> graph)
                                 (join (map expand-triple triples)))))
                     (else (join (map (cut construct-statement <>) (expand-triple statement))))))
                 statements)))))

;; better to do this with rewrite rules?
;; and for goodness sake clean this up a little
(define (query-constructs query)
  (and (update-query? query)
       (let loop ((queryunits (alist-ref '@UpdateUnit query))
                  (prologues '(@Prologue)) (constructs '()))
         (if (null? queryunits) constructs
             (let* ((queryunit (car queryunits))
                    (unit (alist-ref '@Update queryunit))
                    (prologue (append prologues (or (alist-ref '@Prologue queryunit) '())))
                    (where (assoc 'WHERE unit))
                    (delete-construct (construct-statements (alist-ref 'DELETE unit)))
                    (insert-construct (construct-statements (alist-ref 'INSERT unit)))
                    (S (lambda (c) (and c `(,prologue ,c ,(or where '(NOWHERE)))))))
               (loop (cdr queryunits) prologue
                     (cons (list (S delete-construct) (S insert-construct))
                           constructs)))))))

(define (merge-delta-triples-by-graph label quads)
  (let ((car< (lambda (a b) (string< (car a) (car b)))))
    (map (lambda (group)
           `(,(string->symbol (caar group))
             . ((,label 
                 . ,(list->vector
                     (map cdr group))))))
         (group/key car (sort quads car<)))))

(define (expand-delta-properties s propertyset graphs)
  (let* ((p (car propertyset))
         (objects (vector->list (cdr propertyset)))
         (G? (lambda (object) (member (alist-ref 'value object) graphs)))
         (graph (alist-ref 'value (car (filter G? objects)))))
    (filter values
            (map (lambda (object)
                   (let ((o (alist-ref 'value object)))
                     (and (not (member o graphs))
                          `(,graph
                            . ((s . ,(symbol->string s))
                               (p . ,(symbol->string p))
                               (o . ,o))))))
                 objects))))

(define (merge-deltas-by-graph delete-deltas insert-deltas)
  (list->vector
   (map (match-lambda
          ((graph . deltas)
           `((graph . ,(symbol->string graph))
             (delta . ,deltas))))
        (let loop ((ds delete-deltas) (diffs insert-deltas))
          (if (null? ds) diffs
              (let ((graph (caar ds)))
                (loop (cdr ds)
                      (alist-update 
                       graph (append (or (alist-ref graph diffs) '()) (cdar ds))
                       diffs))))))))

(define (run-delta query label)
  (parameterize ((*query-unpacker* string->json))
    (let* ((gkeys '(http://mu.semte.ch/graphs/Graphs http://mu.semte.ch/graphs/include))
           (results (sparql-select (write-sparql query)))
           (graphs (map (cut alist-ref 'value <>)
                        (vector->list
                         (or (nested-alist-ref* gkeys results) (vector)))))
           (D (lambda (tripleset)
                (let ((s (car tripleset))
                      (properties (cdr tripleset)))
                  (and (not (equal? s (car gkeys)))
                       (join (map (cut expand-delta-properties s <> graphs) properties)))))))
      (merge-delta-triples-by-graph
       label (join (filter values (map D results)))))))

(define (run-deltas query)
  (let ((constructs (query-constructs query))
        (R (lambda (c l) (or (and c (run-delta c l)) '()))))
    (map (match-lambda
           ((d i) 
            (merge-deltas-by-graph (R d 'deletes) (R i 'inserts))))
         constructs)))

(define (notify-subscriber subscriber deltastr)
  (let-values (((result uri response)
                (with-input-from-request 
                 (make-request method: 'POST
                               uri: (uri-reference subscriber)
                               headers: (headers '((content-type application/json))))
                 deltastr
                 read-string)))
    (close-connection! uri)
    result))

(define (notify-deltas query)
  (let ((queries-deltas (run-deltas query)))
    (thread-start!
     (make-thread
      (lambda ()
        (for-each (lambda (query-deltas)
                    (let ((deltastr (json->string query-deltas)))
                      (for-each (lambda (subscriber)
                                  (log-message "~%Deltas: notifying ~A~% " subscriber)
                                  (notify-subscriber subscriber deltastr))
                                *subscribers*)))
                  queries-deltas))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implentation
(define (virtuoso-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (when body
      (log-message "~%==Virtuoso Error==~% ~A ~%" body))
    (when response
      (log-message "~%==Reason==:~%~A~%" (response-reason response)))
    (abort exn)))

;; better : parameterize body and req-headers, and define functions...
(define $query (make-parameter (lambda (q) #f)))
(define $body (make-parameter (lambda (q) #f)))
(define $mu-session-id (make-parameter #f))
(define $mu-call-id (make-parameter #f))

(define (proxy-query rewritten-query-string endpoint)
  (with-input-from-request 
   (make-request method: 'POST
                 uri: (uri-reference endpoint)
                 headers: (headers
                           '((Content-Type application/x-www-form-urlencoded)
                             (Accept application/sparql-results+json)))) 
   `((query . , (format #f "~A" rewritten-query-string)))
   read-string))

(define (parse q) (parse-query q))

(define (log-headers) (log-message "~%==Received Headers==~%~A~%" (*request-headers*)))

(define (log-received-query query-string query)
  (log-headers)
  (log-message "~%==Rewriting Query==~%~A~%" query-string)
  (log-message "~%==Parsed As==~%~A~%" (write-sparql query)))

(define (log-rewritten-query rewritten-query-string)
  (log-message "~%==Rewritten Query==~%~A~%" rewritten-query-string))

(define (log-results result)
  (log-message "~%==Results==~%~A~%" (substring result 0 (min 1500 (string-length result)))))

;; this is a beast. clean it up.
(define (rewrite-call _)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (query (parse query-string)))
         
    (log-received-query query-string query)

    (let* ((rewritten-query 
            (parameterize (($query $$query) ($body $$body))
              (rewrite-query query top-rules)))
           (rewritten-query-string (write-sparql rewritten-query)))
      
      (log-rewritten-query rewritten-query-string)

      (handle-exceptions exn 
          (virtuoso-error exn)
        
        ;; (when (update-query? rewritten-query)
        ;;   (notify-deltas rewritten-query))

        (plet-if (not (update-query? query))
                 ((potential-graphs (get-all-graphs rewritten-query))
                  ((result response) (let-values (((result uri response)
                                                   (proxy-query rewritten-query-string
                                                                (if (update-query? query)
                                                                    (*sparql-update-endpoint*)
                                                                    (*sparql-endpoint*)))))
                                       (close-connection! uri)
                                       (list result response))))
            
            (log-message "~%==Potential Graphs==~%(Will be sent in headers)~%~A~%"  
                         potential-graphs)
            
            (let ((headers (headers->list (response-headers response))))
              (log-results result)
              (log-message "~%==Results==~%~A~%" (substring result 0 (min 1500 (string-length result))))
              (mu-headers headers)
              result))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification
(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

(define-namespace rewriter "http://mu.semte.ch/graphs/")

(log-message "~%Proxying to SPARQL endpoint: ~A~% " (*sparql-endpoint*))
(log-message "~%and SPARQL update endpoint: ~A~% " (*sparql-update-endpoint*))

(if (procedure? (*constraint*))
    (log-message "~%With constraint:~%~A" (write-sparql ((*constraint*))))
    (log-message "~%With constraint:~%~A" (write-sparql (*constraint*))))

(*port* 8890)

;; (use slime)

;; (when (not (feature? 'docker))
;;   (define *swank*
;;     (thread-start!
;;      (make-thread
;;       (lambda ()
;;         (swank-server-start 4005))))))

