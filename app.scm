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

(define *plugin*
  (config-param "PLUGIN_PATH" 
                (if (feature? 'docker)
                    "/config/plugin.scm"
                    "./config/rewriter/plugin.scm")))

(when (*plugin*) 
  (log-message "Loading plugin: ~A " (*plugin*))
  (load (*plugin*)))
       
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
(define rw/value
  (lambda (block bindings)
    (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
      (values rw new-bindings))))

;; project...?
(define (rw/subselect block bindings)
  (match block
    ;;((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`@Dataset . dataset)
    ;;    (`WHERE . quads) . rest)
    ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
     (let-values (((rw new-bindings) (rewrite quads bindings)))
       (values
        `((@SubSelect ,(second block) (WHERE ,@rw) ,@rest))
        new-bindings)))))

(define (extract-subselect-vars vars)
  (filter values
          (map (lambda (var)
                 (if (symbol? var) var
                     (match var
                       ((`AS _ v) v)
                       (else #f))))
               vars)))

;; should be:
;; just project bindings
;;    ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . rest) 
;; (rewrite (cdr block))
(define (rewrite-subselect block bindings)
  (match block
    ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . quads)
     (let* ((subselect-vars (extract-subselect-vars vars))
            (inner-bindings (if (or (equal? subselect-vars '(*))
                              (equal? subselect-vars '*))
                          bindings
                          (project-bindings subselect-vars bindings))))
       (let-values (((rw b) (rewrite quads inner-bindings)))
         (values `((@SubSelect ,(second block) ,@rw)) (merge-bindings b bindings)))))))

;; (define (rw/where-subselect block bindings)
;;   (match block
;;     ((`WHERE select-clause (`WHERE . quads))
;;      (let-values (((rw new-bindings) (rewrite quads bindings)))
;;        (values `((WHERE ,select-clause (WHERE ,@rw)))
;;                new-bindings )))))

(define (rw/list block bindings)
  (let-values (((rw new-bindings) (rewrite block bindings)))
    (if (null? rw)
        (values rw new-bindings)
        (values (list rw) new-bindings))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility transformations
(define (expand-triples block #!optional (bindings '()) flatten-graph-statements? replace-a?)
  (rewrite block bindings (expand-triples-rules flatten-graph-statements? replace-a?)))

(define (recursive-expand-triples block #!optional (bindings '()) flatten-graph-statements? replace-a?)
  (rewrite block bindings (recursive-expand-triples-rules flatten-graph-statements?)))

(define (expand-triple-rule #!optional expand-a?)
  (lambda (triple bindings)
    (if expand-a?
        (values 
         (map (match-lambda ((s `a o) `(,s rdf:type ,o))
                            ((s p o) `(,s ,p ,o)))
              (expand-triple triple))
         bindings)
    (values (expand-triple triple) bindings))))

(define (expand-triples-rules flatten-graph-statements? #!optional replace-a?)
  `((,symbol? . ,rw/copy)
    (,triple? . ,(expand-triple-rule replace-a?))
    ((GRAPH) . ,(flatten-graph-rule flatten-graph-statements?))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    ((@SubSelect |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,pair? . ,rw/continue)))

(define (recursive-expand-triples-rules flatten-graph-statements? #!optional replace-a?)
  `((,symbol? . ,rw/copy)
    (,triple? . ,(expand-triple-rule replace-a?))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
            (if flatten-graph-statements?
                (values rw new-bindings)
                (values `((GRAPH ,(second block) ,@rw)) new-bindings)))))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,pair? . ,rw/continue)))

(define (flatten-graph-rule rewrite-graph-statements?)
  (lambda (block bindings)
    (if rewrite-graph-statements?
        (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
          (values rw new-bindings))
        (values (list block) 
                (cons-binding (second block) 'named-graphs new-bindings)))))

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
    ((FILTER BIND MINUS) . ,rw/copy)
    ((OPTIONAL WHERE UNION) . ,rw/continue)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,pair? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core Transformation
(define (rewrite-quads-block block bindings)
  (let ((rewrite-graph-statements? (not (preserve-graphs?))))
    ;; expand triples
    (let-values (((q1 b1);; (rewrite (cdr block) bindings (expand-triples-rules rewrite-graph-statements?))))
                  (expand-triples (cdr block) bindings rewrite-graph-statements?)))
      ;; rewrite-triples
      (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
        ;; continue in nested quad blocks
        (let-values (((q3 b3) (rewrite q2 b2)))
          (values `((,(rewrite-block-name (car block)) ,@q3)) b3))))))

(define (rewrite-block-name part-name)
  (case part-name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else part-name)))

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
          (let ((rewrite-select-queries? (rewrite-select?)))
            (if rewrite-select-queries?
                (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
                  (let ((constraints (get-binding/default 'constraints new-bindings '())))
                    (values `((,(car block)
                               ,@(alist-update 'WHERE
                                               (delete-duplicates
                                                (append constraints (or (alist-ref 'WHERE rw) '())))
                                               rw)))
                            new-bindings)))
                (with-rewrite ((rw (rewrite (cdr block) bindings select-query-rules)))
                              `((@Query ,rw)))))))
    ((@Update)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
            (let ((where-block (or (alist-ref 'WHERE rw) '()))
                  (constraints (get-binding/default* '() 'constraints new-bindings '())))
              (let ((insert (or (alist-ref '|INSERT DATA| (cdr block))
                                (alist-ref 'INSERT (cdr block)))))
                (let* ((constraints (if insert
                                       (let ((triples ;;(rewrite insert '() (expand-triples-rules #t #t))))
                                              (expand-triples insert '() #t #t)))
                                         (instantiate (delete-duplicates constraints) triples))
                                       constraints))
                      (new-where (delete-duplicates (append constraints where-block))))
                  (if (null? new-where)
                      (values `((@Update ,@(reverse rw))) new-bindings)
                      (values `((@Update . ,(alist-update
                                             'WHERE
                                             `((@SubSelect (SELECT *) (WHERE ,@new-where)))
                                             (reverse rw))))
                          new-bindings))))))))
    ((@Dataset) . ,rw/remove)
    ((@Using) . ,rw/remove)
    ((GRAPH) . ,rw/copy)
    ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rewrite-subselect)
    (,quads-block? . ,rewrite-quads-block)
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

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

(define select-query-rules
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
   ((@SubSelect) . ,rw/copy)
   (,pair? . ,rw/continue)))

(define *in-where?* (make-parameter #f))

(define (in-where?)
  ((parent-axis
    (lambda (context) 
      (let ((head (context-head context)))
        (and head (equal? (car head) 'WHERE)))))
   (*context*)))

(define triples-rules
  `((,triple? 
     . ,(lambda (triple bindings)
          (parameterize ((*in-where?* (in-where?)))
            (let ((graphs (get-binding* triple 'graphs bindings)))
              (if graphs
                  (if (*in-where?*)
                      (values '() bindings)
                      (values ;;`((GRAPH ,graph ,triple)) 
                       (map (lambda (graph)
                              `(GRAPH ,graph ,triple))
                            graphs)
                       bindings))
                  (constrain-triple triple bindings))))))
    ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/copy)
    (,subselect? . ,rw/copy)
    (,list? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Graphs
(define (get-all-graphs #!optional (query (*constraint*)))
  ;; (hit-hashed-cache
  ;; *cache* query
  (let ((query (if (procedure? query) (query) query)))
    (let-values (((rewritten-query bindings) (rewrite-query query extract-graphs-rules)))
      (let ((query-string (write-sparql rewritten-query)))
        (let-values (((vars iris) (partition sparql-variable? (get-binding/default 'graphs bindings '()))))
          (if (null? vars) iris
              (delete-duplicates
               (append iris
                       (map cdr (join (sparql-select query-string from-graph: #f)))))))))))

(define (insert-before statement head statements)
  (cond ((null? statements) '())
        ((equal? (caar statements) head)
         (cons statement statements))
        (else (cons (car statements)
                    (insert-before statement head (cdr statements))))))

(define extract-graphs-rules
  `(((@QueryUnit @UpdateUnit WHERE) . ,rw/continue)
    ((GRAPH) . ,(lambda (block bindings)
                  (match block
                    ((`GRAPH graph . quads)
                     (let-values (((rw new-bindings) 
                                   (rewrite quads (cons-binding graph 'graphs bindings))))
                       (values `((GRAPH ,graph ,@rw)) new-bindings))))))
    ((@Dataset @Using) 
     . ,(lambda (block bindings)
          (let ((graphs (map second (cdr block))))
            (values (list block) (fold-binding graphs 'graphs append '() bindings)))))
    ((@Query @Update) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let ((graphs (delete-duplicates
                           (filter sparql-variable?
                                   (get-binding/default 'graphs new-bindings '())))))
            (values `((@Query
                       ,@(insert-before `(|SELECT DISTINCT| ,@graphs) 'WHERE rw)))
                    new-bindings)))))
    (,select? . ,rw/remove)
    ((@SubSelect)
     ;; ((@SubSelect (head . vars) . rest)
     ;; (let ((graphs (alist-ref '@Dataset rest)) (where (alist-ref 'WHERE rest))) ...
     . ,(lambda (block bindings)
          (match block
            ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars)
                         (`@Dataset . dataset) (`WHERE . quads) . rest)
             (let ((graphs (map second dataset)))
               (let-values (((rw new-bindings) (rewrite quads bindings)))
                 (values
                  `((@SubSelect ,(second block) (WHERE ,@rw) ,@rest))
                  (fold-binding graphs 'graphs append '() new-bindings)))))
            ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars)
                         (`WHERE . quads) . rest)
             (let-values (((rw new-bindings) (rewrite quads bindings)))
               (values
                `((@SubSelect ,(second block) (WHERE ,@rw) ,@rest))
                new-bindings))))))
    (,triple? . ,rw/copy)
    ((CONSTRUCT SELECT INSERT DELETE) . ,rw/remove)
    ((UNION) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (values `((UNION ,@rw)) new-bindings))))
    (,quads-block? . ,rw/continue)
    ((@Prologue) . ,rw/copy)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Apply CONSTRUCT statement as a constraint on a triple a b c
(define (parse-constraint* constraint)
  (let ((constraint
         (if (pair? constraint) constraint (parse-query constraint))))
    (recursive-expand-triples constraint '() #f)))

(define parse-constraint (memoize parse-constraint*))

(define constrain-triple
  (let ((constraint (*constraint*)))
    (if (procedure? constraint)
         (let ((C (lambda () (parse-constraint (constraint)))))
           (*constraint* C)
           (lambda (triple bindings)
             (rewrite (list (C)) bindings (constrain-triple-rules triple))))
         (let ((C (parse-constraint constraint)))
           (*constraint* C)
           (lambda (triple bindings)
             (rewrite (list C) bindings (constrain-triple-rules triple)))))))

(define (current-substitution var bindings)
  (let ((sub (alist-ref var (get-binding/default 'constraint-substitutions bindings '()))))
    (if (equal? sub 'a) 'rdf:type sub)))

(define (substitution-or-value var bindings)
  (if (sparql-variable? var)
      (current-substitution var bindings)
      var))

(define (deps* var bindings)
  (let ((dependencies (get-binding/default 'dependencies bindings '())))
    (filter (lambda (v) (member v (or (alist-ref var dependencies) '())))
            (get-binding 'matched-triple bindings))))

(define deps (memoize deps*))

(define (keys* var bindings)
  (map (cut current-substitution <> bindings) (deps var bindings)))

(define keys (memoize keys*))

(define (renaming* var bindings)
  (alist-ref var (get-binding/default* (keys var bindings) (deps var bindings) bindings '())))

(define renaming (memoize renaming*))

(define (update-renaming var substitution bindings)
  (fold-binding* substitution
                 (keys var bindings)
                 (deps var bindings)
                 (cut alist-update var <> <>)
                '()
                bindings))

;; (define (project-renamings ..) 

;; (define (merge-renamings ...)

(define (constrain-triple-rules triple)
  (match triple
    ((a (`^ b) c) (constrain-triple-rules `(,c ,b ,a)))
    ((a b c)
     `( (,symbol? . ,rw/copy)
        ((@QueryUnit) 
         . ,(lambda (block bindings)
              (let ((Query (cdr block))) ;; ** too specific
                (rewrite Query bindings))))
        ((@Query)
         . ,(lambda (block bindings)
              (let ((where (list (assoc 'WHERE (cdr block)))))
                (let-values (((rw new-bindings)
                              (rewrite (cdr block) (update-binding 'constraint-where where bindings))))
                  (values rw (delete-bindings (('constraint-where)) new-bindings))))))
        ((CONSTRUCT) 
         . ,(lambda (block bindings)
              (let-values (((triples _) (expand-triples (cdr block) '() #t)))
                (rewrite triples bindings))))
        (,triple?
         . ,(lambda (triple bindings)
              (match triple
                ((s p o)
                 (let ((constraint-substitutions `((,s . ,a) (,p . ,b) (,o . ,c))))
                   (let-values (((rw new-bindings)
                                 (rewrite (get-binding 'constraint-where bindings)
                                          (update-bindings (('constraint-substitutions constraint-substitutions)
                                                            ('matched-triple (list s p o)))
                                                           bindings)
                                          constraint-rules)))
                     (let ((graphs (get-binding a (if (equal? b 'a) 'rdf:type b) c 'graphs new-bindings))
                           (constraint (alist-ref 'WHERE rw))
                           (clean-bindings (delete-bindings (('matched-triple) 
                                                             ('constraint-substitutions)) 
                                                            new-bindings)))
                       (if (*in-where?*)
                           (values `((*REWRITTEN* ,@constraint)) clean-bindings)
                           (values
                            (map (lambda (graph)
                                   `(GRAPH ,graph (,a ,b ,c)))
                                 graphs)
                            (fold-binding constraint 'constraints append '() clean-bindings))))))))))
        (,pair? . ,rw/remove)))))

(define (get-context-graph)
  (match (context-head
          ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
           (*context*)))
    ((`GRAPH graph . rest) graph)
    (else #f)))

(define rename-constraint-triple
  (lambda (triple bindings)
    (let* ((matched-triple? (equal? triple (get-binding 'matched-triple bindings)))
           (graph (and matched-triple? (get-context-graph))))
      (let-values (((substitutions new-bindings)
                    (let loop ((vars (cons graph triple)) (substitutions '()) (new-bindings bindings)) ;; new-bindings => bindings
                      (if (null? vars) 
                          (values (reverse substitutions)
                                  (update-binding 'constraint-substitutions ;; fold-binding append
                                                  (append (filter pair? substitutions)
                                                          (get-binding 'constraint-substitutions bindings))
                                                  new-bindings))
                          (let ((var (car vars)))
                            (cond ((not (sparql-variable? var))
                                   (loop (cdr vars) (cons var substitutions) new-bindings))
                                  ((current-substitution var bindings) => (lambda (v) (loop (cdr vars) (cons v substitutions) new-bindings)))
                                  ((renaming var new-bindings) => (lambda (v)
                                                                    (loop (cdr vars)
                                                                          (cons `(,var . ,v) substitutions)
                                                                          new-bindings)))
                                  (else (let ((new-var (gensym var)))
                                          (loop (cdr vars)
                                                (cons `(,var . ,new-var) substitutions)
                                                (update-renaming var new-var new-bindings))))))))))
        (values (list (map (lambda (a) (if (pair? a) (cdr a) a)) (cdr substitutions)))
                (if matched-triple?
                    (cons-binding*
                     (let ((sub (car substitutions)))
                       (if (pair? sub) (cdr sub) sub))
                     (map (cut current-substitution <> bindings) (get-binding 'matched-triple bindings))
                     'graphs
                     ;;(car substitutions) 
                     new-bindings)
                    new-bindings))))))

(define constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings) 
          (match block
            ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
             (let-values (((rw new-bindings) (rewrite quads bindings)))
               (values `((@SubSelect (,(car (second block) )
                           ,@(map (lambda (var) 
                                    (if (equal? var '*) var
                                        (current-substitution var new-bindings)))
                                  vars))
                          (WHERE ,@rw) ,@rest))
                       new-bindings))))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings)
                        (rewrite (cdr block)
                                 (update-binding
                                  'dependencies (constraint-dependencies block bindings)
                                  bindings))))
            (values `((WHERE ,@rw)) (delete-binding 'dependencies new-bindings)))))
    ((UNION) . ,rw/continue)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
               (values
                `((GRAPH ,(substitution-or-value graph new-bindings)
                         ,@(cdr rw)))
                new-bindings))))))
    (,triple? . ,rename-constraint-triple)
    (,pair? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constraint variable dependencies
(define (constraint-dependencies constraint-where-block bindings)
  (get-dependencies 
   (list constraint-where-block) 
   (map car (get-binding 'constraint-substitutions bindings))))

(define (get-dependencies* query bound-vars)
  (rewrite query '() (dependency-rules bound-vars)))

(define get-dependencies (memoize get-dependencies*))

(define (minimal-dependencies source? sink? dependencies)
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
     ((UNION OPTIONAL) . ,rw/continue)
     ((@SubSelect)
      . ,(lambda (block bindings)
           (match block
             ((`@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
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
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instantiation for INSERT and INSERT DATA
;; to do: expand namespaces before checking
(define (instantiate where-block triples)
  (let ((where-block (rewrite where-block '() expand-graph-rules)))
    (let-values (((rw new-bindings)
                  (rewrite where-block '() (get-instantiation-matches-rules triples))))
      (rewrite rw new-bindings instantiation-union-rules))))

(define (instantiation-match-triple triple triples)
  (if (null? triples) '()
      (let loop ((triple1 triple) (triple2 (car triples)) (match-binding '()))
        (cond ((null? triple1) 
               (if (null? match-binding)
                   (instantiation-match-triple triple (cdr triples))
                   (cons match-binding (instantiation-match-triple triple (cdr triples)))))
              ((sparql-variable? (car triple1))
               (loop (cdr triple1) (cdr triple2) (cons (cons (car triple1) (car triple2))
                                                       match-binding)))
              ((equal? (car triple1) (car triple2)) (loop (cdr triple1) (cdr triple2) match-binding))
              (else (instantiation-match-triple triple (cdr triples)))))))
    
(define (instantiate-triple triple binding)
  (let loop ((triple triple) (new-triple '()))
    (cond ((null? triple) (reverse new-triple))
          ((sparql-variable? (car triple))
           (loop (cdr triple) (cons (or (alist-ref (car triple) binding) (car triple)) new-triple)))
          (else (loop (cdr triple) (cons (car triple) new-triple))))))

(define (get-instantiation-matches-rules triples)
  `(((@SubSelect) . ,rw/subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (if (member triple triples)
                 (values '() bindings)
                 (let ((match-bindings (instantiation-match-triple triple triples)))
                   (if (null? match-bindings)
                       (values (list block) bindings)
                       (values '() 
                               ;; to do:  group matches by shared variables, i.e., (map car binding)
                               (fold-binding (map (lambda (match-binding)
                                                        (let ((instantiation (instantiate-triple triple match-binding)))
                                                          (list (list block)
                                                                (if (null? (filter sparql-variable? instantiation))
                                                                    '()
                                                                    `((GRAPH ,graph ,instantiation)))
                                                                match-binding)))
                                                      match-bindings)
                                                 'instantiated-quads append '() bindings)))))))))
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    ((OPTIONAL UNION) . ,rw/continue)
    (,list? . ,rw/list)
    ))

(define instantiation-union-rules
  `(((@SubSelect) . ,rw/subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (let* ((shared-variable-triples 
                     (filter values
                             (map (match-lambda ((a b binding)
                                                 (let ((vars (map car binding)))
                                                   (and (any values (map (cut member <> vars) triple))
                                                        (list a b binding)))))
                                  (get-binding/default 'instantiated-quads bindings '())))))
               (if (null? shared-variable-triples)
                   (values (list block) bindings)
                   (values 
                    (union-over-instantiation block shared-variable-triples bindings)
                    bindings)))))))
    ((UNION) . ,rw/continue)
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    (,list? . ,rw/list)
    ))

(define (union-over-instantiation block shared-triples bindings)
  (match block
    ((`GRAPH graph triple)
     (match (car shared-triples)
       ((matched-quad-block instantiated-quad-block binding)
        (rewrite
         `((UNION (,block ,@matched-quad-block)
                  ((GRAPH ,graph ,(instantiate-triple triple binding))
                   ,@instantiated-quad-block)))
         (update-binding 'instantiated-quads (cdr shared-triples)  bindings)
         instantiation-union-rules))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deltas
(define (update-query? query)
  ;;((list-of? update-unit?)
  ;;(alist-ref '@UpdateUnit query)))
  (equal? (car query) '@UpdateUnit))

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
    (log-message "~%==Parsed As==~%~A~%" (write-sparql query))

    (let ((rewritten-query 
           (parameterize (($query $$query) ($body $$body))
             (rewrite-query query top-rules))))

      (log-message "~%==Rewritten Query==~%~A~%" (write-sparql rewritten-query))

      (handle-exceptions exn 
          (virtuoso-error exn)
        
        (log-message "~%==Potential Graphs==~%(Will be in headers, and done in parallel for performance)~%~A~%" (get-all-graphs rewritten-query))

        ;; (when (update-query? rewritten-query)
        ;;   (notify-deltas rewritten-query))

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

(if (procedure? (*constraint*))
    (log-message "==Rewriter Constraint==~%~A" (write-sparql ((*constraint*))))
    (log-message "==Rewriter Constraint==~%~A" (write-sparql (*constraint*))))

(define-namespace rewriter "http://mu.semte.ch/graphs/")

(*port* 8890)

;; (use slime)

;; (when (not (feature? 'docker))
;;   (define *swank*
;;     (thread-start!
;;      (make-thread
;;       (lambda ()
;;         (swank-server-start 4005))))))

