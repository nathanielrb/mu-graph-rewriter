(use s-sparql s-sparql-parser
     mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client cjson
     memoize)

(require-extension sort-combinators)

(define *subscribers-file*
  (config-param "SUBSCRIBERSFILE" "subscribers.json"))

(define *subscribers*
  (handle-exceptions exn '()
    (vector->list
     (alist-ref 'potentials
                (with-input-from-file (*subscribers-file*)
                  (lambda () (read-json)))))))

(define *plugin*
  (config-param "PLUGIN_PATH" #f))

(define *constraint*
  (make-parameter
   `((@Unit
      ((@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE (GRAPH ?g (?s ?p ?o)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Graph Rewriter
(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #f))

(define *cache* (make-hash-table))

(define select-query-rules (make-parameter (lambda () '())))

(define (preserve-graphs?)
  (or (header 'preserve-graph-statements)
      (($query) 'preserve-graph-statements)
      (not (*rewrite-graph-statements?*))))

(define (rewrite-select?)
  (or (equal? "true" (header 'rewrite-select-queries))
      (equal? "true" (($query) 'rewrite-select-queries))
      (*rewrite-select-queries?*)))

(define (rewrite-quads-block block bindings)
  (let ((rewrite-graph-statements? (not (preserve-graphs?))))
    ;; expand triples
    (let-values (((q1 b1) (rewrite (cdr block) bindings (%expand-triples-rules rewrite-graph-statements?))))
      ;; rewrite-triples
      (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
        ;; continue in nested quad blocks
        (let-values (((q3 b3) (rewrite q2 b2)))
          (values `((,(rewrite-block-name (car block)) ,@q3)) b3))))))

(define (%expand-triples-rules rewrite-graph-statements? #!optional replace-a?)
  `((,symbol? . ,rw/copy)
    (,triple? . ,(expand-triple-rule replace-a?))
    ((GRAPH) . ,(flatten-graph-rule rewrite-graph-statements?))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)))

(define (expand-triple-rule #!optional replace-a?)
  (lambda (block bindings)
    (if replace-a?
        (values 
         (map (match-lambda ((s `a o) `(,s rdf:type ,o))
                            ((s p o) `(,s ,p ,o)))
              (expand-triple block))
         bindings)
    (values (expand-triple block) bindings))))

(define (flatten-graph-rule rewrite-graph-statements?)
  (lambda (block bindings)
    (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
      (if rewrite-graph-statements?
          (values rw new-bindings)
          (values (list block) 
                  (cons-binding (second-block) 'named-graphs new-bindings))))))

(define (extract-all-variables where-clause)
  (delete-duplicates (filter sparql-variable? (flatten where-clause))))

(define (extract-subselect-vars vars)
  (filter values
          (map (lambda (var)
                 (if (symbol? var) var
                     (match var
                       ((`AS _ v) v)
                       (else #f))))
               vars)))

(define (rewrite-block-name part-name)
  (case part-name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else part-name)))

(define (rewrite-subselect block bindings)
  (match block
    ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . quads)
     (let* ((subselect-vars (extract-subselect-vars vars))
            (inner-bindings (if (or (equal? subselect-vars '(*))
                              (equal? subselect-vars '*))
                          bindings
                          (project-bindings subselect-vars bindings))))
       (let-values (((rw b) (rewrite quads inner-bindings)))
         (values (list (cons (car block) rw)) (merge-bindings b bindings)))))))

(define rules
  (make-parameter
   `((,symbol? . ,rw/copy)
     ((@Unit) . ,rw/continue)
     ((@Prologue)
      . ,(lambda (block bindings)
           (values `((@Prologue
                      (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
                      ,@(cdr block)))
                   bindings)))
     ((@Query)
      . ,(lambda (block bindings)
           (let ((rewrite-select-queries? (rewrite-select?)))
             (if rewrite-select-queries?
                 (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
                   (let ((constraints (delete-duplicates ; ** only works on top level; should be generalized **
                                       (get-binding 'constraints new-bindings '()))))
                     (values `((,(car block)
                                ,@(alist-update 'WHERE (append constraints (alist-ref 'WHERE rw)) rw)))
                             new-bindings)))
                 (with-rewrite ((rw (rewrite (cdr block) bindings (select-query-rules))))
                   `((@Query ,rw)))))))
     ((@Update)
      . ,(lambda (block bindings)
           (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
             (let ((where-block (or (alist-ref 'WHERE rw) '()))
                   (constraints (delete-duplicates ; ** only works on top level; should be generalized **
                                 (get-binding/default* '() 'constraints new-bindings '()))))
               (let ((insert (or (alist-ref '|INSERT DATA| (cdr block))
                                 (alist-ref 'INSERT (cdr block)))))
                 (let ((constraints (if insert
                                        (let ((triples (rewrite insert '() (%expand-triples-rules #t #t))))
                                          (instantiate constraints triples))
                                        constraints)))
                   (values `((@Update . ,(alist-update
                                          'WHERE
                                          `((SELECT *) (WHERE ,@constraints ,@where-block))
                                          (reverse rw))))
                           new-bindings)))))))
     ((@Dataset) . ,rw/remove)
     ((@Using) . ,rw/remove)
     ((GRAPH) . ,rw/copy)
     (,select? . ,rw/copy)
     (,subselect? . ,rewrite-subselect)        
     (,quads-block? . ,rewrite-quads-block)
     ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
     `((,where-subselect?
        . ,(lambda (block bindings)
             (let-values (((rw b) (rewrite-subselect (cdr block) bindings)))
               (values `((WHERE ,@rw))
                       (merge-bindings b bindings))))))
      (,pair?
       . ,(lambda (block bindings)
            (with-rewrite ((rw (rewrite block bindings)))
             (list rw)))))))

 ;; nested for backward compatibility
(default-rules (lambda () (rules)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query for graphs
(define (get-all-graphs #!optional (query (*constraint*)))
  (hit-hashed-cache
   *cache* query
   (let-values (((query bindings) (rewrite query '() extract-graphs-rules)))
     (let ((query-string (write-sparql query)))
       (let-values (((vars iris)  (partition sparql-variable? (get-binding 'graphs bindings))))
         (delete-duplicates
          (append iris
                  (map cdr (join (sparql-select query-string from-graph: #f))))))))))

(define rw/value
  (lambda (block bindings)
    (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
      (values rw new-bindings))))

(define extract-graphs-rules
  `(((GRAPH) . ,(lambda (block bindings)
                  (match block
                    ((`GRAPH graph . quads)
                     (let-values (((rw new-bindings) 
                                   (rewrite quads
                                            (cons-binding graph 'graphs  bindings))))
                       (values `((GRAPH ,graph ,@rw)) new-bindings))))))
    ((@Query @Update) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (values `((@Query
                       (|SELECT DISTINCT| ,@(delete-duplicates
                                             (filter sparql-variable?
                                                     (get-binding 'graphs new-bindings))))
                       ,@rw))
                    new-bindings))))
    (,subselect? . ,(lambda (block bindings)
                      (match block
                        ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
                         (let-values (((rw new-bindings) (rewrite quads bindings)))
                           (values
                            `((,(car block) (WHERE ,@rw) ,@rest))
                            new-bindings))))))
    (,where-subselect?
       . ,(lambda (block bindings)
            (match block
              ((`WHERE select-clause (`WHERE . quads))
               (let-values (((rw new-bindings) (rewrite quads bindings)))
                 (values `((WHERE ,select-clause (WHERE ,@rw)))
                         new-bindings ))))))
    (,triple? . ,rw/copy)
    (,symbol? . ,rw/copy)
    ((CONSTRUCT SELECT INSERT DELETE) . ,rw/remove)
    ((@Unit WHERE) . ,rw/continue)
    ((@Prologue) . ,rw/copy)
    (,pair? . ,(lambda (block bindings)
               (with-rewrite ((rw (rewrite block bindings)))
                 (list rw))))))

(define (dataset label graphs #!optional named?)
  (let ((label-named (symbol-append label '| NAMED|)))
    `((,label ,(*default-graph*))
      ,@(map (lambda (graph) 
               `(,label ,graph))
             graphs)
      ,@(splice-when
         (and named?
              `((,label-named ,(*default-graph*))
                ,@(map (lambda (graph) 
                         `(,label-named ,graph))
                       graphs)))))))

(select-query-rules
 `((,triple? . ,rw/copy)
   ((GRAPH)
    . ,(lambda (block bindings)
         (values (cddr block) bindings)))
   ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
   ((@Dataset) 
    . ,(lambda (block bindings)
         (let ((graphs (get-all-graphs)))
         (values `((@Dataset ,@(dataset 'FROM graphs #f))) 
                 (update-binding 'graphs graphs bindings)))))
   (,select? . ,rw/copy)
   (,subselect? . ,rw/copy)
   (,pair? . ,rw/continue)))

(define *in-where?* (make-parameter #f))

(define (in-where?)
  ((parent-axis
    (lambda (context) 
      (let ((head (context-head context)))
        (and head (equal? (car head) 'WHERE)))))
   (*context*)))

(define (in-insertdata?)
  ((parent-axis
    (lambda (context) 
      (let ((head (context-head context)))
        (and head (equal? (car head) '|INSERT DATA|)))))
   (*context*)))

(define triples-rules
  `((,triple? 
     . ,(lambda (triple bindings)
          (parameterize ((*in-where?* (in-where?)))
            (constrain-triple triple bindings))))
    ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Apply constraints
(define (%rewrite-constraint-rules triple)
  (match triple
    ((a b c)
     `( (,symbol? . ,rw/copy)
        ((@Unit) 
         . ,(lambda (block bindings)
              (let ((QueryUnit (car (cdr block)))) ;; ** too specific
                (rewrite QueryUnit bindings))))
        ((@Query)
         . ,(lambda (block bindings)
              (let ((where (list (assoc 'WHERE (cdr block)))))
                (let-values (((rw new-bindings)
                              (rewrite (cdr block) (update-binding 'constraint-where where bindings))))
                  (values rw (delete-bindings (('constraint-where)) new-bindings))))))
        ((WHERE) . ,rw/remove)
        ((CONSTRUCT) 
         . ,(lambda (block bindings)
              (let-values (((triples new-bindings) (rewrite (cdr block) '() (%expand-triples-rules #t))))
                (rewrite triples bindings))))
        (,triple? 
         . ,(lambda (triple bindings)
              (match triple
                ((s p o)
                 ;; if there are multiple triples? ... UNION
                 ;; and this should match a b c, otherwise '()
                 ;; (if (and (or (sparql-variable? a) (equal? 
                 (let ((constraint-renamings `((,s . ,a) (,p . ,b) (,o . ,c))))
                   (let-values (((rw new-bindings)
                                 (rewrite (get-binding 'constraint-where bindings)
                                          (update-bindings
                                           (('constraint-renamings constraint-renamings) ('matched-triple (list s p o)))
                                           bindings)
                                          constraint-rules)))
                     (let ((graph (get-binding a (if (equal? b 'a) 'rdf:type b) c 'graph new-bindings)))
                       (values (if (*in-where?*) '() `((GRAPH ,graph (,a ,b ,c))))
                               (update-bindings
                                (('constraints (append (alist-ref 'WHERE rw) (get-binding/default 'constraints bindings '()))))
                                (delete-bindings (('matched-triple) ('constraint-renamings))
                                                 new-bindings))))))))))
        (,pair? . ,rw/remove)))))

(define (get-context-graph)
  (match (context-head
          ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
           (*context*)))
    ((`GRAPH graph . rest) graph)
    (else #f)))

(define unify-constraint-triple
  (lambda (triple bindings)
    (let* ((matched-triple? (equal? triple (get-binding 'matched-triple bindings)))
           (graph (and matched-triple? (get-context-graph)))
           (constraint-renamings (or (get-binding 'constraint-renamings bindings) '()))
           (rename (lambda (x)
                    (let ((n (alist-ref x constraint-renamings)))
                      (if (equal? n 'a) 'rdf:type n))))
           (dependencies (or (get-binding 'dependencies bindings) '()))
           (deps (lambda (x)
                  (filter (lambda (v) (member v (or (alist-ref x dependencies) '())))
                          (get-binding 'matched-triple bindings))))
           (keys (lambda (x) (map rename (deps x)))))
      (let-values (((assignments new-bindings)
                    (let loop ((vars (cons graph triple)) (assignments '()) (new-bindings bindings))
                      (if (null? vars) (values (reverse assignments)
                                               (update-binding 'constraint-renamings
                                                               (append (filter pair? assignments)
                                                                       (get-binding 'constraint-renamings bindings))
                                                               new-bindings))
                          (let ((var (car vars)))
                            (cond ((not (sparql-variable? var))
                                   (loop (cdr vars) (cons var assignments) new-bindings))
                                  ((rename var) => (lambda (v) (loop (cdr vars) (cons v assignments) new-bindings)))
                                  (else
                                   (let ((v-assignments (get-binding/default* (keys var) (deps var) new-bindings '())))
                                     (if (alist-ref var v-assignments)
                                         (loop (cdr vars)
                                               (cons `(,var . ,(alist-ref var v-assignments)) assignments)
                                               new-bindings)
                                         (let ((new-var (gensym var)))
                                           (loop (cdr vars)
                                                 (cons `(,var . ,new-var) assignments)
                                                 (update-binding* (keys var) (deps var)
                                                                  (alist-update var new-var v-assignments)
                                                                  new-bindings))))))))))))
        (values (list (map (lambda (a) (if (pair? a) (cdr a) a)) (cdr assignments)))
                (if matched-triple?
                    (update-binding*
                     (map rename (get-binding 'matched-triple bindings))
                     'graph (car assignments) new-bindings)
                    new-bindings))))))

(define (get-bound-var var bindings)
  (if (sparql-variable? var)
      (or (alist-ref var (get-binding/default 'constraint-renamings bindings '()))
          (alist-ref var (get-binding/default (alist-ref var (get-binding 'dependencies bindings))
                                              bindings '())))
      var))

(define constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    (,subselect?  
     . ,(lambda (block bindings) 
          (match block
            ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
             ;; ** should we be projecting bindings here?... it masks important things...
             (let-values (((rw new-bindings) (rewrite quads bindings)))
               (values `(((,(caar block) 
                           ,@(map (lambda (var) 
                                    (if (equal? var '*) var
                                        (get-bound-var var new-bindings)))
                                  vars))
                          (WHERE ,@rw) ,@rest))
                       new-bindings))))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings)
                        (rewrite (cdr block)
                                 (update-binding
                                  'dependencies
                                  (get-dependencies (list block) (map car (get-binding 'constraint-renamings bindings)))
                                  bindings))))
            (values `((WHERE ,@rw)) (delete-binding 'dependencies new-bindings)))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
               (values
                `((GRAPH ,(get-bound-var graph new-bindings)
                         ,@(cdr rw)))
                new-bindings))))))
    (,triple? . ,unify-constraint-triple)
    (,pair? . ,rw/continue)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constraint variable dependencies
(define (get-dependencies* query renamings)
  (rewrite query '() (dependency-rules renamings)))

(define get-dependencies (memoize get-dependencies*))

(define (minimal-dependencies source? sink? dependencies)
  (let-values (((sources nodes) (partition (compose source? car) dependencies)))
    (let loop ((paths (join (map (lambda (b)
                             (match b ((head . rest) (map (lambda (r) (list r head)) rest))))
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
                                  
(define (concat-dependencies paths)
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
                     dependencies vars)
               bindings))))))
     ((GRAPH) . ,(lambda (block bindings)
                   (match block
                     ((`GRAPH graph . rest)
                      (rewrite rest 
                               (cons-binding graph 'constraint-graphs bindings))))))
     (,subselect? 
      . ,(lambda (block bindings)
           (match block
             ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
              (rewrite quads bindings)))))
     ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let* ((deps (get-binding 'dependencies new-bindings))
                   (source? (lambda (var) (member var bound-vars)))
                   (sink? (lambda (x) (member x (get-binding/default 'constraint-graphs new-bindings '())))))
              (values
               (concat-dependencies (minimal-dependencies source? sink? deps))
               bindings)))))
    (,pair? . ,rw/continue)))

;; (use slime)

;; (when (not (feature? 'docker))
;;   (define *swank*
;;     (thread-start!
;;      (make-thread
;;       (lambda ()
;;         (swank-server-start 4005))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instantiate (for INSERT DATA)
(define (instantiate where triples)
  (let ((wherexp (rewrite where '() expand-graph-rules)))
    (let-values (((rw new-bindings) (rewrite wherexp '() (instantiation-match-rules triples))))
      (rewrite rw new-bindings instantiation-rules))))

(define (rw/continue-subselect block bindings)
  (match block
    ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
     (let-values (((rw new-bindings) (rewrite quads bindings)))
       (values
        `((,(car block) (WHERE ,@rw) ,@rest))
        new-bindings)))))

(define (rw/continue-where-subselect block bindings)
  (match block
    ((`WHERE select-clause (`WHERE . quads))
     (let-values (((rw new-bindings) (rewrite quads bindings)))
       (values `((WHERE ,select-clause (WHERE ,@rw)))
               new-bindings )))))

(define expand-graph-rules
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
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/continue-subselect)
    (,where-subselect? . ,rw/continue-where-subselect)
    (,pair? . ,rw/continue)))

(define (match-triple triple triples)
  (if (null? triples) #f
      (let loop ((triple1 triple) (triple2 (car triples)) (match-binding '()))
        (cond ((null? triple1) match-binding)
              ((sparql-variable? (car triple1))
               (loop (cdr triple1) (cdr triple2) (cons (cons (car triple1) (car triple2))
                                                       match-binding)))
              ((equal? (car triple1) (car triple2)) (loop (cdr triple1) (cdr triple2) match-binding))
              (else (match-triple triple (cdr triples)))))))

(define (instantiation-match-rules triples)
  `((,where-subselect? . ,rw/continue-where-subselect)
    (,subselect? . ,rw/continue-subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple) ; ** too specific
             (if (member triple triples)
                 (values '() bindings)
                 (let ((match-binding (match-triple triple triples)))
                   (if match-binding
                       (let ((instantiation (instantiate-triple triple match-binding)))
                         (values '() (cons-binding  (list (list block )
                                                          (if (null? (filter sparql-variable? instantiation))
                                                              '()
                                                              `((GRAPH ,graph ,instantiation)))
                                                          match-binding)
                                                    'instantiated-quads bindings)))
                       (values (list block) bindings))))))))
    (,symbol? . ,rw/copy)
    (,pair? . ,rw/continue)
    ))
    
(define (instantiate-triple triple binding)
  (let loop ((triple triple) (new-triple '()))
    (cond ((null? triple) (reverse new-triple))
          ((sparql-variable? (car triple))
           (loop (cdr triple) (cons (or (alist-ref (car triple) binding) (car triple)) new-triple)))
          (else (loop (cdr triple) (cons (car triple) new-triple))))))

(define instantiation-rules
  `((,where-subselect? . ,rw/continue-where-subselect)
    (,subselect? . ,rw/continue-subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple) ; ** too specific
             (let* ((shared-variable-triples 
                     (filter values
                             (map (match-lambda ((a b binding)
                                                 (let ((vars (map car binding)))
                                                   (and (any values (map (cut member <> vars) triple))
                                                        (list a b binding)))))
                                  (get-binding 'instantiated-quads bindings)))))
               (if (null? shared-variable-triples)
                   (values (list block) bindings)
                   (values 
                    (map (match-lambda ((mquad imquad  binding)
                                        `(UNION (,block ,@mquad) ; ** correct? **
                                                ((GRAPH ,graph ,(instantiate-triple triple binding))
                                                 ,@imquad))))
                         shared-variable-triples)
                    bindings)))))))
    (,symbol? . ,rw/copy)
    (,pair? . ,rw/continue)
    ))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deltas
(define (update-query? query)
  ((list-of? update-unit?)
   (alist-ref '@Unit query)))

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
       (let loop ((queryunits (alist-ref '@Unit query))
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
                      ;; (format (current-error-port) "~%==Deltas==~%~A" deltastr)
                      (for-each (lambda (subscriber)
                                  (print "Notifying " subscriber)
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

;; this is a beast. clean it up.
(define (rewrite-call _)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (query (parse-query query-string)))
         
    (log-message "~%==Received Headers==~%~A~%" (*request-headers*))
    (log-message "~%==Rewriting Query==~%~A~%" query-string)

    (let ((rewritten-query 
           (parameterize (($query $$query) ($body $$body))
             (rewrite-query query))))

      (log-message "~%==Parsed As==~%~A~%" (write-sparql query))
      (log-message "~%==Rewritten Query==~%~A~%" (write-sparql rewritten-query))

      (handle-exceptions exn 
          (virtuoso-error exn)
        
        (when (update-query? rewritten-query)
          (notify-deltas rewritten-query))

        (let-values (((result uri response)
                      (with-input-from-request 
                       (make-request method: 'POST
                                     uri: (uri-reference (*sparql-endpoint*))
                                     headers: (headers
                                               '((Content-Type application/x-www-form-urlencoded)
                                                 (Accept application/sparql-results+json)))) 
                       `((query . , (format #f "~A" (write-sparql rewritten-query))))
                       read-string)))
          (close-connection! uri)

          (let ((headers (headers->list (response-headers response))))
            (log-message "~%==Results==~%~A~%" 
                         (substring result 0 (min 1500 (string-length result))))
            (mu-headers headers)
            (format #f "~A~" result)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification

(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

(when (*plugin*) (load (*plugin*)))

(format #t "==Rewriter Constraint==~%~A" (write-sparql (*constraint*)))

(define constrain-triple
  (let ((constraint (*constraint*)))
    (cond ((pair? constraint)
           (lambda (triple bindings)
             (rewrite constraint bindings (%rewrite-constraint-rules triple))))
           ((string? constraint)
            (let ((constraint (parse-query constraint)))
              (lambda (triple bindings)
                (rewrite constraint bindings (%rewrite-constraint-rules triple)))))
           ((procedure? constraint)
            (lambda (triple bindings)
              (rewrite (constraint) bindings (%rewrite-constraint-rules triple)))))))

(*port* 8890)



