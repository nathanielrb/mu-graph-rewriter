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

(define *functional-properties* (make-parameter '()))

;; Can be a string, an s-sparql expression, 
;; or a thunk returning a string or an s-sparql expression.
;; Used by (constrain-triple) below.
(define *constraint*
  (make-parameter
   `((@QueryUnit
      ((@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE (GRAPH ,(*default-graph*) (?s ?p ?o)))))))))
 
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; for RW library, to be cleaned up and abstracted.
;; (define rw/value
;;   (lambda (block bindings)
;;     (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
;;       (values rw new-bindings))))

;; what about expressions?
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
     (let* ((subselect-vars (extract-subselect-vars vars))
            (inner-bindings (if (or (equal? subselect-vars '(*))
                                    (equal? subselect-vars '*))
                                bindings
                                (project-bindings subselect-vars bindings))))
       (let-values (((rw new-bindings) (rewrite rest inner-bindings)))
         (values `((@SubSelect (,label ,@vars) ,@rw)) 
                 (merge-bindings 
                  (project-bindings subselect-vars new-bindings)
                  bindings)))))))

(define (rw/list block bindings)
  (let-values (((rw new-bindings) (rewrite block bindings)))
    (cond ((null? rw) (values rw new-bindings))
          ((fail? rw) (values (list #f) new-bindings))
          (else (values (list (filter pair? rw)) new-bindings)))))

(define (rw/quads block bindings)
  (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
    (cond ((null? rw) (values '() new-bindings))
          ((fail? rw) (values (list #f) new-bindings))
	  (else (values `((,(car block) ,@(filter pair? rw))) new-bindings)))))

(define (rw/union block bindings)
  (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
    (let ((new-blocks (filter values rw)))
      (case (length new-blocks)
	((0)  (values (list #f) new-bindings))
	((1)  (values new-blocks new-bindings))
	(else (values `((UNION ,@new-blocks)) new-bindings))))))

(define (atom-or-cdr a)
  (if (pair? a) (cdr a) a))

(define (not-node? head)
  (lambda (exp)
    (not (equal? (car exp) head))))

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

;; To do: rewrite as cps loop
(define (insert-child-before head statement statements)
  (cond ((null? statements) '())
        ((equal? (caar statements) head)
         (cons statement statements))
        (else (cons (car statements)
                    (insert-child-before head statement (cdr statements))))))

;; To do: rewrite as cps loop
(define (insert-child-after head statement statements)
  (cond ((null? statements) '())
        ((equal? (caar statements) head)
         (cons (car statements)
	       (cons statement (cdr statements))))
        (else (cons (car statements)
                    (insert-child-after head statement (cdr statements))))))

;; To do: harmonize these
(define (insert-query? query)
  (or (get-child-body '|INSERT DATA| (cdr query))
      (get-child-body 'INSERT (cdr query))))

(define (update-query? query)
  (equal? (car query) '@UpdateUnit))

(define (select-query? query)
  (equal? (car query) '@QueryUnit))

(define (update-unit? unit)
  (alist-ref '@Update unit))

(define (fail? block)
  (not (null? (filter not block))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utilities
(define (alist-update-proc key proc alist)
  (let loop ((alist alist) (cont values))
    (cond ((null? alist) (cont (list (cons key (proc #f)))))
          ((equal? (caar alist) key) 
           (cont `((,key . ,(proc (cdar alist))) 
                   ,@(cdr alist))))
          (else (loop (cdr alist) 
                      (lambda (rest)
                        (cont `(,(car alist) ,@rest))))))))


;; rewrite in CPS
(define (assoc-update-proc key proc alist)
  (cond ((null? alist) (list (cons key (proc #f))))
        ((equal? key (caar alist))
         (cons (cons key (proc (cdar alist)))
               (cdr alist)))
        (else (cons (car alist)
                    (assoc-update-proc key proc (cdr alist))))))

(define append-unique (compose delete-duplicates append))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility transformations
;; refactor this a bit, to use #!key for mapp and bindingsp
;; and mostly to NOT use a flatten-graphs? parameter, but separate functions & arg passing
(define flatten-graphs? (make-parameter #f))

(define (expand-triples block #!optional (bindings '()) mapp bindingsp)
  (rewrite block bindings (expand-triples-rules mapp bindingsp)))

;; (define (expand-triples-flatten-graphs

(define (recursive-expand-triples block #!optional (bindings '()) mapp bindingsp)
  (rewrite block bindings (recursive-expand-triples-rules mapp bindingsp)))

;; (define (recursive-expand-triples-flatten-graphs 

(define (expand-triple-rule #!optional mapp bindingsp)
  (let ((mapp (or mapp values))
        (bindingsp (or bindingsp (lambda (triples bindings) bindings))))
    (lambda (triple bindings)
      (let ((triples (map mapp (expand-triple triple))))
        (values triples (bindingsp triples bindings))))))

(define (expand-triples-rules #!optional mapp bindingsp)
  `(((@QueryUnit @UpdateUnit @Query @Update CONSTRUCT WHERE
      DELETE INSERT |DELETE WHERE| |INSERT DATA|) . ,rw/quads)
    (( @Prologue @Dataset @Using) . ,rw/copy)
    ((FILTER BIND MINUS OPTIONAL UNION VALUES
      @SubSelect |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,select? . ,rw/copy)
    ((UNION) . ,rw/union)
    ((GRAPH) 
     . ,(lambda (block bindings)
	  (if (flatten-graphs?)
	      (rewrite (cddr block) bindings)
	      (values (list block) 
		      (cons-binding (second block) 'named-graphs bindings)))))
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))

(define (recursive-expand-triples-rules #!optional mapp bindingsp)
  `(((@QueryUnit @UpdateUnit @Query @Update CONSTRUCT WHERE
      DELETE INSERT |DELETE WHERE| |INSERT DATA|) . ,rw/quads)
    ((@Prologue @Dataset @Using) . ,rw/copy)
    (,symbol? . ,rw/copy)
    ((MINUS OPTIONAL) . ,rw/quads)
    ((UNION) . ,rw/union)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
            (if (flatten-graphs?)
                (values rw new-bindings)
                (values `((GRAPH ,(second block) ,@rw)) new-bindings)))))
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    ((VALUES FILTER BIND |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

(define (expand-graphs statements #!optional (bindings '()))
  (rewrite statements bindings expand-graphs-rules))

(define expand-graphs-rules
  `(((@SubSelect) . ,rw/subselect)
    (,select? . ,rw/copy)
    ((OPTIONAL WHERE) . ,rw/quads)
    ((UNION) . ,rw/union)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . quads)
             (values (map (lambda (quad)
                            `(GRAPH ,graph ,quad))
                          quads)
                     bindings)))))
    (,triple? . ,rw/copy)
    ((VALUES FILTER BIND MINUS |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))

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
  (let ((ancestor ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
                 (*context*))))
    (and ancestor
         (match (context-head ancestor)
           ((`GRAPH graph . rest) graph)
           (else #f)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Bindings, Substitutions and Renamings
(define (a->rdf:type b)
  (if (equal? b 'a) 'rdf:type b))

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
  (print "deps of " var ": " (deps var bindings))
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Functional Properties (and other optimizations)
(define (collect-functional-properties triples bindings)
  (fold (match-lambda* 
  	  (((s p o) bindings)
  	   (if (member p (*functional-properties*))
  	       (let ((current-value (get-binding s p 'functional-property bindings)))
  		 (cond ((or (not current-value)
  			    (and (sparql-variable? current-value)
  				 (not (sparql-variable? o))))
  			(update-binding s p 'functional-property o bindings))
  		       ((or (sparql-variable? o) (equal? o current-value)) ; modulo namespace!
  			bindings)
  		       (else (abort
			      (make-property-condition
			       'exn
			       'message
			       (format "Invalid query: ~A is a declared as a functional property but two values are given for subject ~A: ~A and ~A."
				       p s o current-value))))))
  	       bindings)))
        bindings
  	triples))

(define (update-fproperty-bindings triple bindings)
  (match triple
    ((s p o)
     (let ((fpv (get-binding s p 'functional-property bindings)))
       (if fpv
	   (cond ((sparql-variable? o)
		  (cons-binding `(,o . ,fpv)
				'fproperties-substitutions bindings))
		 ((or (sparql-variable? fpv) (equal? o fpv))
		  bindings)
		 (else
		  (abort
		   (make-property-condition
		    'exn
		    'message
		    (format "Invalid query: ~A is a declared as a functional property but two values are given for subject ~A: ~A and ~A."
			    p s o current-value)))))
	   bindings)))))

(define (fproperty-replacements block bindings)
  (let ((fp-subs (get-binding 'fproperties-substitutions bindings)))
    (if fp-subs
	(replace-variables block fp-subs)
	block)))

(define (optimize block bindings)
  (let-values (((rw new-bindings)
		(rewrite (fproperty-replacements block bindings)
			 bindings
			 optimization-rules)))
    rw))

(define optimization-rules
  `(((@UpdateUnit @QueryUnit @Update @Query) . ,rw/continue)
    ((@Prologue @Dataset GROUP |GROUP BY| LIMIT ORDER |ORDER BY| OFFSET) . ,rw/copy)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((VALUES) 
     . ,(lambda (block bindings)
          (match block
            ((`VALUES vars . vals)
	     (values
	      (list (simplify-values vars vals bindings))
	      bindings)))))
    ((FILTER) 
     . ,(lambda (block bindings)
	  (validate-filter block bindings)))
    ((BIND) . ,rw/copy) ;;?
    ((GRAPH) . ,rw/copy)
    ((UNION) . ,rw/union)
    ((WHERE)
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (rw/quads block bindings)))
	    (if (fail? rw)
		(abort
		 (make-property-condition
		  'exn
		  'message (format "Invalid query: functional property conflict.")))
		(values rw new-bindings)))))
    (,quads-block? . ,rw/quads)
    (,triple? . ,rw/copy)
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Triples Graphs
(define (find-triple-graphs triple block)
  (rewrite block '() (find-triple-graphs-rules triple)))

(define (find-triple-graphs-rules triple)
  `(((GRAPH)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block))))
            (values
             (if (null? rw)
                 '()
                 (list (second block)))
             bindings))))
    ((@SubSelect)
     . ,(lambda (block bindings)
          (rewrite (cddr block))))
    ((UNION OPTIONAL WHERE)
     . ,(lambda (block bindings)
          (rewrite (cdr block))))
    ((OPTIONAL) . ,rw/quads)
    (,triple? 
     . ,(lambda (block bindings)
          (values
           (if (equal? block triple) (list #t) '())
           bindings)))
    ((FILTER VALUES BIND) . ,rw/remove)
    (,list?
     . ,(lambda (block bindings)
          (rewrite block)))))

(define (get-triple-graphs triple bindings)
  (let ((vars (filter sparql-variable? triple))
        (triple (map a->rdf:type triple)))
    (cdr-or 
     (assoc triple (get-binding/default* vars 'graphs bindings '()))
     '())))

(define (update-triple-graphs new-graphs triple bindings)
  (let ((vars (filter sparql-variable? triple))
        (triple (map a->rdf:type triple)))
    (fold-binding* triple
                   vars
                   'graphs
                   (lambda (triple graphs-list)
                     (alist-update-proc triple 
					(lambda (graphs)
					  (if graphs (append new-graphs graphs) new-graphs))
                                    graphs-list))
                   '()
                   bindings)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Graphs
(define (get-all-graphs query)
  (let ((query (if (procedure? query) (query) query)))
    (let-values (((rewritten-query bindings) (rewrite-query query extract-graphs-rules)))
      (let ((query-string (write-sparql rewritten-query)))
        (let-values (((vars iris) (partition sparql-variable? (get-binding/default 'all-graphs bindings '()))))
          (if (null? vars) iris
              (delete-duplicates
               (append iris
                       (map cdr (join (sparql-select query-string from-graph: #f)))))))))))

(define extract-graphs-rules
  `(((@QueryUnit @UpdateUnit WHERE) . ,rw/quads)
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
    ((CONSTRUCT SELECT INSERT DELETE |INSERT DATA|) . ,rw/remove)
    ((GRAPH) . ,(lambda (block bindings)
                  (match block
                    ((`GRAPH graph . quads)
                     (let-values (((rw new-bindings) 
                                   (rewrite quads (cons-binding graph 'all-graphs bindings))))
                       (values `((GRAPH ,graph ,@rw)) new-bindings))))))

    ((UNION) 
     . ,(rw/lambda (block)
          (with-rewrite ((rw (rewrite (cdr block))))
	    `((UNION ,@rw)))))
    (,quads-block? . ,rw/quads)
    (,triple? . ,rw/copy)
    ((VALUES FILTER BIND) . ,rw/copy)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core Transformation
(define replace-a
  ;; (match-lambda ((s `a o) `(,s rdf:type ,o))
  ;;               ((s p o)  `(,s ,p ,o))))
  (match-lambda ((s p o) `(,s ,(a->rdf:type p) ,o))))

(define (rewrite-block-name name)
  (case name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else name)))

(define level-quads (make-parameter '()))

(define (rewritten? block) (equal? (car block) '*REWRITTEN*))

(define (new-level-quads new-quads)
  (append 
   (level-quads)
   (join (map cdr (filter rewritten? new-quads)))))

(define (optimize-duplicates block)
  (lset-difference equal?
    (delete-duplicates (filter pair? block))
    (level-quads)))

(define (rewrite-quads-block block bindings)
  (parameterize ((flatten-graphs? (not (preserve-graphs?))))
    ;; expand triples
    (let-values (((q1 b1) (expand-triples (cdr block) bindings replace-a collect-functional-properties)))
      ;; rewrite-triples
      (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
        ;; continue in nested quad blocks
        (let-values (((q3 b3)
		      (parameterize ((level-quads (new-level-quads q2)))
			(rewrite q2 b2))))
          (cond ((null? q3) (values '() b3))
                ((fail? q3) (values (list #f) b3))
                (else (values `((,(rewrite-block-name (car block)) 
				 ,@(optimize-duplicates q3)))
                              b3))))))))

(define top-rules
  `(((@QueryUnit @UpdateUnit) . ,rw/quads)
    ((@Prologue)
     . ,(lambda (block bindings)
          (values `((@Prologue
                     (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
                     ,@(cdr block)))
                  bindings)))
    ((@Dataset) . ,rw/remove)
    ((@Using) . ,rw/remove)
    ((@Query)
     . ,(lambda (block bindings)
	  (if (rewrite-select?)
	      (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
		(let ((constraints (get-binding/default 'constraints new-bindings '())))
		  (values `((@Query ,@(optimize
				       (append-child-body/before 'WHERE constraints rw)
				       new-bindings)))
			  new-bindings)))
	      (with-rewrite ((rw (rewrite (cdr block) bindings select-query-rules)))
	        `((@Query ,rw))))))
    ((@Update)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
	      (if (insert-query? block)
		  (values
		   `((@Update
		      ,@(instantiated-insert-query rw new-bindings)))
		   new-bindings)
		  (let ((new-where ;; should do real optimization!
                         (delete-duplicates  
                          (append (get-child-body 'WHERE rw)
                                  (get-binding/default* '() 'constraints bindings '())))))
		    (if (null? new-where)
			(values `((@Update ,@(reverse rw))) new-bindings)
			(values `((@Update ,@(replace-child-body 'WHERE (optimize new-where new-bindings) (reverse rw))))
				new-bindings)))))))
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((GRAPH) . ,rw/copy)
    ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    ((WHERE DELETE |DELETE WHERE| |DELETE DATA| INSERT |INSERT WHERE| |INSERT DATA| MINUS OPTIONAL GRAPH)
     . ,rewrite-quads-block)
    ;; ((WHERE)
    ;;  . ,(lambda (block bindings)
    ;;       (let-values (((rw new-bindings) (rewrite-quads-block block bindings)
    ;; (,quads-block? . ,rewrite-quads-block)
    ((UNION) . ,rw/union)
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT| |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))   

(define (instantiated-insert-query rw new-bindings)
  (parameterize ((flatten-graphs? #t))
    (let* ((insert-block (get-child-body 'INSERT rw))
           (triples (expand-triples insert-block '() replace-a))
           (where-block (or (get-child-body 'WHERE rw) '()))
           (constraints (optimize
                          (get-binding/default* '() 'constraints new-bindings '())
                         new-bindings))) 
      ;; To do: real optimization here!
      (let-values (((new-where inst-bindings) (instantiate
                                                  (delete-duplicates (append (list constraints) where-block))
                                                  triples)))
	(let* ((instantiated-values (get-binding 'instantiated-values inst-bindings))
               (instantiated-graphs (get-binding 'instantiated-graphs inst-bindings))
	       (new-insert (if instantiated-values
			       (instantiate-values insert-block instantiated-values instantiated-graphs)
			       insert-block)))
          (replace-child-body 'INSERT new-insert 
                              (if (null? new-where)
                                  (reverse rw)
                                  (replace-child-body 'WHERE new-where 
                                                      (reverse rw)))))))))

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
 `(((GRAPH)
    . ,(rw/lambda (block) 
         (cddr block)))
   ((@SubSelect FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
   ((@Dataset) 
    . ,(lambda (block bindings)
         (let ((graphs (get-all-graphs (*constraint*))))
	   (values `((@Dataset ,@(make-dataset 'FROM graphs #f))) 
		   (update-binding 'all-graphs graphs bindings)))))
   (,select? . ,rw/copy)
   (,triple? . ,rw/copy)
   (,list? . ,rw/list)))

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
;; Constraint Renaming
(define (parse-constraint constraint)
  (let ((constraint
         (if (pair? constraint) constraint (parse-query constraint))))
    (recursive-expand-triples (list constraint))))

;; (define parse-constraint (memoize parse-constraint*))

(define (define-constraint constraint)
  (if (procedure? constraint)
      (*constraint* (lambda () (parse-constraint (constraint))))
      (*constraint* (parse-constraint constraint))))

;; Apply CONSTRUCT statement as a constraint on a triple a b c
(define (apply-constraint triple bindings)
  (parameterize ((flatten-graphs? #f))
    (let ((C (*constraint*)))
      (if (procedure? C)
          (rewrite (C) bindings (apply-constraint-rules triple))
          (rewrite C bindings (apply-constraint-rules triple))))))

;; (define (remove-renaming var bindings)
;;  (delete-binding* (keys var bindings) (deps var bindings) bindings))

;; (define (project-renamings ..) 

;; (define (merge-renamings ...)

;;(define (apply-constraint-rule 

(define constraint-where (make-parameter '()))

(define matched-triple (make-parameter '()))
(define constraint-substitutions (make-parameter '()))
(define dependency-substitutions (make-parameter '()))
(define dependencies  (make-parameter '()))

(define (apply-constraint-rules triple)
  (match triple
    ((a (`^ b) c) (apply-constraint-rules `(,c ,b ,a)))
    ((a b c)
     `((,symbol? . ,rw/copy)
       ((@QueryUnit) 
        . ,(lambda (block bindings)
             (rewrite (cdr block) bindings)))
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
                 (let ((constraints (filter (lambda (c) (and c (not (equal? c '(#f))))) rw)))
                   (let ((graphs (find-triple-graphs (list a b c) constraints)))
                     (if (*in-where?*)
                         (values `((*REWRITTEN* ,@constraints)) new-bindings)
                         (values
                          (map (lambda (graph)
                                 `(GRAPH ,graph (,a ,b ,c)))
                               graphs)
                          ;; do real optimization here
                          ;; with optimize-duplicates, for example
                          (fold-binding constraints 'constraints append-unique '() 
                                        (update-triple-graphs graphs (list a b c) 
                                                              new-bindings))))))))))
       ((@Dataset) . ,rw/copy)
       (,triple? 
        . ,(lambda (triple bindings)
             (match triple
               ((s p o)
                (let ((initial-substitutions (make-initial-substitutions a b c s p o bindings)))
                  (if initial-substitutions
		      (parameterize ((matched-triple (list s p o))
				     (dependency-substitutions initial-substitutions))
			(let-values (((rw new-bindings)
				      (rewrite (constraint-where)
					       (update-binding 'constraint-substitutions initial-substitutions bindings)
					       rename-constraint-rules)))
			  (let ((cleaned-bindings #f)) ; (clean-constraint-bindings bindings new-bindings)))
			    (values (get-child-body 'WHERE rw) new-bindings))))
                      (values '() bindings)))))))
       (,list? . ,rw/remove)))))

(define (make-initial-substitutions a b c s p o bindings)
  (let ((check (lambda (u v) (or (sparql-variable? u) (sparql-variable? v) (equal? u v)))))
    (and (check a s) (check b p) (check c o)
	 `((,s . ,a) (,p . ,b) (,o . ,c)))))

(define (update-renamings new-bindings)
  (let ((substitutions (get-binding 'constraint-substitutions new-bindings)))
    (fold (lambda (substitution bindings)
	    (update-renaming (car substitution) (cdr substitution) bindings))
	  (delete-binding 'constraint-substitutions new-bindings)
	  substitutions)))

;; ;; need to do this for FILTER, VALUES and BIND as well
;; (define (new-substitutions graph triple bindings)
;;   (let loop ((vars (cons graph triple)) (substitutions '()) (new-bindings bindings))
;;     (if (null? vars) 
;; 	(let ((renamed-quad (map atom-or-cdr (reverse substitutions))))
;; 	  (values (car renamed-quad)
;; 		  (cdr renamed-quad)
;; 		  (update-binding 'constraint-substitutions
;; 				  (append (filter pair? substitutions)
;; 					  (get-binding 'constraint-substitutions bindings))
;; 				  new-bindings)))
;; 	(let ((var (car vars)))
;; 	  (cond ((not (sparql-variable? var))
;; 		 (loop (cdr vars) (cons var substitutions) new-bindings))
;; 		((current-substitution var bindings)
;; 		 => (lambda (v) 
;; 		      (loop (cdr vars) (cons v substitutions) 
;; 			    new-bindings)))
;; 		((renaming var new-bindings)
;; 		 => (lambda (v)
;; 		      (loop (cdr vars)
;; 			    (cons `(,var . ,v) substitutions)
;; 			    new-bindings)))
;; 		(else (let ((new-var (gensym var)))
;; 			(loop (cdr vars)
;; 			      (cons `(,var . ,new-var) substitutions)
;; 			      new-bindings))))))))

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
      ;; (let-values (((renamed-graph renamed-triple new-bindings) (new-substitutions graph triple bindings)))
      (let-values (((renamed-quad new-bindings) (new-substitutions (cons graph triple) bindings)))
        (values               
	 (list (cdr renamed-quad)) ;         (list renamed-triple)
         ;; (update-fproperty-bindings renamed-triple new-bindings))))))
	 (update-fproperty-bindings (cdr renamed-quad) new-bindings))))))

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
	  (parameterize ((renaming-dependencies (constraint-dependencies block bindings)))
	    (let-values (((rw new-bindings)
			  (rewrite (cdr block)
				   (update-binding
				    'dependencies (constraint-dependencies block bindings) 
				    bindings))))
	      (let ((new-bindings (update-renamings new-bindings)))
	      (if (null? (filter not rw)) ;; abstract (already as fail? ?)
		  (values `((WHERE ,@rw)) new-bindings)
		  (values `((WHERE #f)) new-bindings)))))))
    ((UNION) . ,rw/union)
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
	       (values
		(list (simplify-values vars vals bindings))
		new-bindings))))))

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Solving and Simplifying
(define (replace-variables exp substitutions)
  (let rec ((exp exp))
    (if (null? exp) '()
	(let ((e (car exp))
	      (rest (rec (cdr exp))))
	  (if (pair? e)
	      (cons (rec e) rest)
	      (cons (or (alist-ref e substitutions) e) rest))))))
                             
(define (simplify-values vars vals bindings)
  (call/cc
   (lambda (out)
     (let loop ((vars vars) (new-vars '()) 
		(vals vals) (new-vals (make-list (length vals) '())))
       (cond ((null? vars) 
	      (if (null? new-vars) '()
		  `(VALUES ,(reverse new-vars) ,@(map reverse new-vals))))
	     ((pair? vars)
	      (cond ((sparql-variable? (car vars))
		     (loop (cdr vars) 
			   (cons (car vars) new-vars)
			   (map cdr vals) 
			   (map cons (map car vals) new-vals)))
		    (else
		     (let ((matches (map (cut equal? <> (car vars)) (map car vals))))
		       (if (null? (filter values matches))
			   (out #f)
			   (loop (cdr vars) new-vars
				 (filter values (map (lambda (match? val) (and match? val)) matches vals))
				 (filter values (map (lambda (match? val) (and match? val)) matches new-vals))))))))
	     (else (cond ((sparql-variable? vars) `(VALUES ,vars ,@vals))
			 (else
			  (if (null? (filter not (map (cut equal? <> vars) vals)))
			      '()
			      (out #f))))))))))

(define (validate-filter statement bindings)
  (let ((check (validate-constraint (second statement) bindings)))
    (case check
      ((#t) (values '() bindings))
      ((?) (values (list statement) bindings))
      ((#f)
       (values (list #f) bindings)))))

(define (validate-constraint constraint bindings)
  (match constraint
    ((`= a b) (unify a b bindings))
    ((`!= a b) (disunify a b bindings))
    ((`IN a b) (constraint-member a b bindings))
    ((`|NOT IN| a b)  (constraint-not-member a b bindings))
    (else '?)))

;; These are way too simple.
(define (unify a b bindings)
  (if (or (sparql-variable? a) (sparql-variable? b))
      '?
      (equal? a b)))

(define (disunify a b bindings)
  (if (or (sparql-variable? a) (sparql-variable? b))
      '?
      (not (equal? a b))))

(define (constraint-member a b bindings)
  (let ((possible? (lambda (x) (or (sparql-variable? x) (equal? a x)))))
    (cond ((sparql-variable? a) '?)
	  ((member a b) #t)
	  ((null? (filter sparql-variable? b)) #f)
	  (else '?))))

(define (constraint-not-member a b bindings)
  (cond ((sparql-variable? a) '?)
	((member a b) #f)
	((null? (filter sparql-variable? b)) #t)
	(else '?)))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Renaming dependencies
(define (constraint-dependencies constraint-where-block bindings)
  (get-dependencies 
   (list constraint-where-block) 
   (map car (dependency-substitutions)))) ;; (get-binding 'dependency-substitutions bindings))))

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

;; To do: better use of return values & bindings
(define (dependency-rules bound-vars)
  `((,triple? 
     . ,(lambda (triple bindings)
         (let* ((dependencies (or (get-binding 'dependency-paths bindings) '()))
                (bound? (lambda (var) (member var bound-vars)))
                (vars (filter sparql-variable? triple)))
           (let-values (((bound unbound) (partition bound? vars)))
             (values
              (list triple)
              (update-binding
               'dependency-paths
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
     ((UNION) . ,rw/union)
     ((OPTIONAL) . ,rw/quads)
     ((VALUES)
      . ,(lambda (block bindings)
           (match block
             ((`VALUES vars . vals)
              (if (symbol? vars)
                  ;; actually depends on other VALUES singletons
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
                        (fold (lambda (var deps)
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
	      (print "paths: " paths)
              (values
               (concat-dependency-paths (minimal-dependency-paths source? sink? paths))
               bindings)))))
     ((FILTER) . ,rw/copy) ;; ??
     (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instantiation
;; for INSERT and INSERT DATA
;; To do: expand namespaces before checking
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
                 (values `((*graph* . ,graph)) bindings)
                 (let ((match-bindings (match-triple triple triples)))
                   (if (null? match-bindings)
                       (values (list block) bindings)
                       (values `((*graph* . ,graph))
                               (fold-binding (map (cut expand-instantiation graph triple <>) match-bindings)
					     'instantiated-quads 
					     append '() 
                                             bindings)))))))))
    ((OPTIONAL) . ,rw/quads)
    ((UNION) . ,rw/union)
    (,quads-block? 
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (rw/quads block bindings)))
	    ;; ** unused **
	    (values rw new-bindings))))
    ((VALUES FILTER BIND) . ,rw/copy) ;; ???
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    (,list? 
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (rw/list block bindings)))
            ;; Remove blocks that only contain VALUES and FILTER statements.
	    ;; Assumes length = 1. Is this warranted?
            ;; Only handles 1 VALUES statement.
            ;; 'instantiated-values SHOULD be used as following:
            ;; - separate alists for each VALUES tuple
            ;; - graphs kept separately, and instantiated only if the corresponding VALUES is used
            ;; and what about the filters?
            ;; and can/should this be abstracted & used elsewhere?
            (if (null? rw)
                (values '() new-bindings)
                (let loop ((statements (car rw)) 
                           (filter-statements '()) (values-statements '()) 
                           (graphs '()) (quads '()))
                  (if (null? statements)
                      (if (null? quads)
                          (match (car values-statements) ;; ** only 1!!
                            ((`VALUES vars . vals)
                             (let ((new-instantiated-values
                                    (if (pair? vars)
                                        (map reverse
                                             (fold (lambda (n r) (map cons n r))
                                                   (map list vars)
                                                   vals))
                                        `((,vars . ,vals)))))
                               (values '()
                                       (update-instantiated-values new-instantiated-values new-bindings)))))
                          (values (list (filter (not-node? '*graph*) (car rw)))
                                  (if (null? graphs)
                                      new-bindings
                                      ;; (update-instantiated-values 
                                      ;;  (map (lambda (graph) (list graph graph)) graphs)
                                      ;;  new-bindings))))   
                                      (fold-binding graphs 'instantiated-graphs append-unique '() new-bindings))))
                      (case (caar statements)
                        ((*graph*) (loop (cdr statements) filter-statements
					 values-statements (cons (cdar statements) graphs) quads))
                        ((FILTER) (loop (cdr statements) (cons (car statements) filter-statements) 
					values-statements graphs quads))
                        ((VALUES) (loop (cdr statements) filter-statements 
					(cons (car statements) values-statements) graphs quads))
			((BIND) (loop (cdr statements) filter-statements values-statements graphs quads)) ;; ??
                        (else (loop (cdr statements) filter-statements
				    values-statements graphs (cons (car statements) quads))))))))))))

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
     . ,(lambda  (block bindings)
          (match block
            ((`@SubSelect (label . vars) . rest)
             (let-values (((rw new-bindings)
			   (rewrite (get-child-body 'WHERE rest) 
				    (subselect-bindings vars bindings))))
               (values
                `((@SubSelect (,label ,@vars)
			      ;;,@(replace-child-body 'WHERE (group-graph-statements rw) rest)))
                              ,@(replace-child-body 'WHERE rw rest)))
                (merge-subselect-bindings vars new-bindings bindings)))))))
    ;; ((WHERE OPTIONAL MINUS)
    ;;  . ,(lambda (block bindings)
    ;;       (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
    ;;         ;; (values `((,(car block) ,@(group-graph-statements rw))) new-bindings))))
    ;;         (values `((,(car block) ,@rw)) new-bindings))))
    ((WHERE OPTIONAL MINUS) . ,rw/quads)
    ((UNION)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
	    (let ((new-blocks (filter values rw)))
	      (case (length new-blocks)
		((0)  (values '() new-bindings))
		;; ((1)  (values  (group-graph-statements (car new-blocks)) new-bindings))
		;; (else (values `((UNION ,@(group-graph-statements new-blocks))) new-bindings)))))))
                ((1)  (values  new-blocks new-bindings))
		(else (values `((UNION ,@new-blocks)) new-bindings)))))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (let ((matching-quads (match-instantiated-quads triple bindings)))
               (if (null? matching-quads)
                   (values (list block) bindings)
		   (union-over-instantiation block matching-quads bindings)))))))
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    ((VALUES). ,rw/copy) ;; ??
    ((*graph*) . ,rw/remove)    ;; can we remove this?
    ((FILTER) ;; ??
     . ,(lambda (block bindings)
          (values (list block) bindings)))
    (,list? 
     ;;. ,rw/list)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rw/list block bindings)))
            (values (list (group-graph-statements (car rw))) new-bindings))))
    ))

;; didn't I already do this somewhere else?
;; and at least do it efficiently...
(define (update-instantiated-values kvs bindings)
  (if (null? kvs) bindings
      (let ((key (caar kvs)) (vals (cdar kvs)))
        (fold-binding key 'instantiated-values
                      (lambda (key ivals)
                        (alist-update key (delete-duplicates (append vals (or (alist-ref key ivals) '()))) ivals))
                      '()
                      (update-instantiated-values (cdr kvs) bindings)))))

(define (update-instantiated-value key val bindings)
  (fold-binding key 'instantiated-values
                (lambda (key ivals)
                  (alist-update key (delete-duplicates (cons val (or (alist-ref key ivals) '()))) ivals))
                '()
                bindings))

(define (union-over-instantiation block matching-quads bindings)
  (match block
    ((`GRAPH graph triple)
     (match (car matching-quads)
       ((matched-quad-block instantiated-quad-block binding)
        (rewrite
         `((UNION (,block ,@matched-quad-block)
                  ((GRAPH ,graph ,(instantiate-triple triple binding))
                   ,@instantiated-quad-block)))
         (update-binding 'instantiated-quads (cdr matching-quads) bindings)
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

(define (instantiate-values block substitutions graphs)
  (let ((substitutions (if (not graphs)
                           substitutions
                           (fold (lambda (graph substitutions)
                                   (alist-update-proc graph (lambda (graphs) (cons graph graphs)) substitutions))
                                 substitutions
                                 graphs))))
    (rewrite block '() (instantiate-values-rules substitutions))))

(define (instantiate-values-rules substitutions)
  `(((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . triples)
             (let ((new-graphs (alist-ref graph substitutions)))
               (if new-graphs
                   (let-values (((rw _) (rewrite triples)))
                     (values (map (lambda (new-graph)
                                    `(GRAPH ,new-graph ,@triples))
                                  new-graphs)
                             bindings))
                   (values (list block) bindings)))))))
    (,triple? 
     . ,(lambda (triple bindings)
          (values
           (let loop ((triple triple) (cont list))
             (if (null? triple) (cont '())
                 (let ((subs (alist-ref (car triple) substitutions)))
                   (if subs
                       (loop (cdr triple)
                             (lambda (rest)
                               (join
                                (map
                                 (lambda (s) (cont (cons s rest)))
                                 subs))))
                       (loop (cdr triple)
                             (lambda (rest)
                               (cont (cons (car triple) rest))))))))
           bindings)))

    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deltas
;; To do: refactor this using rewrite rules, and clean up.
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
    (abort 'virtuoso-error)))

(define (rewriter-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (log-message "~%" exn)
    (when body
      (log-message "~%==Rewriter Error==~% ~A ~%" body))
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
  (log-message "~%==With Constraint==~%~A~%" (write-sparql (if (procedure? (*constraint*)) ((*constraint*)) (*constraint*))))
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
              (handle-exceptions exn 
                  (begin (log-message "~%==Rewriting Error==~%") 
                         (log-message "~%~A~%" ((condition-property-accessor 'exn 'message) exn))
                         (print-error-message exn (current-error-port))
                         (print-call-chain (current-error-port))
                         (abort exn))
                (rewrite-query query top-rules))))
           (rewritten-query-string (write-sparql rewritten-query)))
      
      (log-rewritten-query rewritten-query-string)

      (handle-exceptions exn 
          (virtuoso-error exn)
        
        ;; (when (update-query? rewritten-query)
        ;;   (notify-deltas rewritten-query))

        (plet-if (not (update-query? query))
                 ((potential-graphs (get-all-graphs rewritten-query))
                  ((result response)
                   (let-values (((result uri response)
                                 (proxy-query rewritten-query-string
                                              (if (update-query? query)
                                                  (*sparql-update-endpoint*)
                                                  (*sparql-endpoint*)))))
                     (close-connection! uri)
                     (list result response))))
            
                 (log-message "~%==Potential Graphs==~%(Will be sent in headers)~%~A~%"  potential-graphs)
            
          (let ((headers (headers->list (response-headers response))))
            (log-results result)
            (mu-headers headers)
            result))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification
(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

(define-namespace rewriter "http://mu.semte.ch/graphs/")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load
(log-message "~%==Query Rewriter Service==")

(when (*plugin*) 
  (log-message "~%Loading plugin: ~A " (*plugin*))
  (load (*plugin*)))

(log-message "~%Proxying to SPARQL endpoint: ~A " (*sparql-endpoint*))
(log-message "~%and SPARQL update endpoint: ~A " (*sparql-update-endpoint*))

(if (procedure? (*constraint*))
    (log-message "~%with constraint:~%~A" (write-sparql ((*constraint*))))
    (log-message "~%with constraint:~%~A" (write-sparql (*constraint*))))

(*port* 8890)

;; (use slime)

;; (when (not (feature? 'docker))
;;   (define *swank*
;;     (thread-start!
;;      (make-thread
;;       (lambda ()
;;         (swank-server-start 4005))))))


