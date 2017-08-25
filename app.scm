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

(*functional-properties* '(rdf:type))

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

(log-message "~%Proxying to SPARQL endpoint: ~A~% " (*sparql-endpoint*))
(log-message "~%and SPARQL update endpoint: ~A~% " (*sparql-update-endpoint*))

(if (procedure? (*constraint*))
    (log-message "~%With constraint:~%~A" (write-sparql ((*constraint*))))
    (log-message "~%With constraint:~%~A" (write-sparql (*constraint*))))

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
       (let-values (((rw new-bindings) (rewrite quads inner-bindings)))
         (values `((@SubSelect ,(second block) ,@rw)) 
                 (merge-bindings 
                  (project-bindings subselect-vars new-bindings)
                  bindings)))))))

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
;; refactor this a bit, to use #!key for mapp and bindingsp
(define (expand-triples block #!optional (bindings '()) flatten-graph-statements? mapp bindingsp)
  (rewrite block bindings (expand-triples-rules flatten-graph-statements? mapp bindingsp)))

(define (recursive-expand-triples block #!optional (bindings '()) flatten-graph-statements? mapp bindingsp)
  (rewrite block bindings (recursive-expand-triples-rules flatten-graph-statements? mapp bindingsp)))

(define replace-a
  (match-lambda ((s `a o) `(,s rdf:type ,o))
                ((s p o)  `(,s ,p ,o))))

(define (expand-triple-rule #!optional mapp bindingsp)
  (let ((mapp (or mapp values))
        (bindingsp (or bindingsp (lambda (triples bindings) bindings))))
    (lambda (triple bindings)
      (let ((triples (map mapp (expand-triple triple))))
        (values triples (bindingsp triples bindings))))))

(define (expand-triples-rules flatten-graph-statements? #!optional mapp bindingsp)
  `((,symbol? . ,rw/copy)
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    ((GRAPH) . ,(flatten-graph-rule flatten-graph-statements?))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    ((@SubSelect |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,pair? . ,rw/continue)))

(define (recursive-expand-triples-rules flatten-graph-statements? #!optional mapp bindingsp)
  `((,symbol? . ,rw/copy)
    (,triple? . ,(expand-triple-rule mapp bindingsp))
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

(define expand-quads-rules
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
(define (get-functional-properties triples bindings)
  (fold (lambda (triple bindings)
          (match triple
            ((s p o)
             (update-binding s p 'functional-property o bindings))))
        bindings
        (filter (lambda (triple)
                  (match triple
                    ((s p o) (member p (*functional-properties*)))))
                triples)))

(define (rewrite-quads-block block bindings)
  (let ((rewrite-graph-statements? (not (preserve-graphs?))))
    ;; expand triples
    (let-values (((q1 b1) (expand-triples (cdr block) bindings rewrite-graph-statements? replace-a get-functional-properties)))
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
                                               (append constraints (or (alist-ref 'WHERE rw) '()))
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
                                       (let ((triples (expand-triples insert '() #t replace-a)))
                                         (instantiate constraints triples))
                                       constraints))
                       (new-where (append constraints where-block)))
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
            (let ((graphs (cdr-when (assoc triple (get-binding/default 'triple-graphs bindings '())))))
              (if graphs
                  (if (*in-where?*)
                      (values '() bindings)
                      (values
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

(define (dependency-substitution var bindings)
  (let ((sub (alist-ref var (get-binding/default 'dependency-substitutions bindings '()))))
    (if (equal? sub 'a) 'rdf:type sub)))

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

;; (define (remove-renaming var bindings)
;;  (delete-binding* (keys var bindings) (deps var bindings) bindings))

;; (define (project-renamings ..) 

;; (define (merge-renamings ...)

;;(define (constrain-triple-rule 

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
                                                            ('dependency-substitutions constraint-substitutions)
                                                            ('matched-triple (list s p o)))
                                                           bindings)
                                          constraint-rules)))
                     
                     (let* ((graphs (cdr-when (assoc (list a (if (equal? b 'a) 'rdf:type b) c) 
                                               (get-binding/default 'triple-graphs new-bindings '()))))
                           (constraint (alist-ref 'WHERE rw))
                           (substitutions (get-binding 'constraint-substitutions new-bindings))
                           (updated-bindings (fold (lambda (substitution bindings)
                                                     (update-renaming (car substitution) (cdr substitution) bindings))
                                                   new-bindings
                                                   substitutions))
                           (fpbs (get-binding 'fproperties-bindings new-bindings)))
                       (let ((constraint (if fpbs (replace-variables constraint fpbs) constraint))
                             (cleaned-bindings (delete-bindings (('matched-triple) 
                                                                 ('constraint-substitutions)
                                                                 ('dependency-substitutions) ('dependencies))
                                                                updated-bindings)))
                       (if (*in-where?*)
                           (values `((*REWRITTEN* ,@constraint)) cleaned-bindings)
                           (values
                            (map (lambda (graph)
                                   `(GRAPH ,graph (,a ,b ,c)))
                                 graphs)
                            (fold-binding constraint 'constraints append '() cleaned-bindings)))))))))))
         (,pair? . ,rw/remove)))))

(define (replace-variables exp bindings)
  (if (null? exp) '()
      (let ((e (car exp))
            (rest (replace-variables (cdr exp) bindings)))
        (if (pair? e)
            (cons (replace-variables e bindings) rest)
            (cons (or (alist-ref e bindings) e) rest)))))

(define (get-context-graph)
  (match (context-head
          ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
           (*context*)))
    ((`GRAPH graph . rest) graph)
    (else #f)))

(define (atom-or-cdr a)
  (if (pair? a) (cdr a) a))

;; this one needs real refactoring and renaming
(define rename-constraint-triple
  (lambda (triple bindings)

    (let* ((graph (get-context-graph)))
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
                                                new-bindings
                                                ;; (update-renaming var new-var new-bindings)
                                                )))))))))
        (let* ((graph (car substitutions))
               (renamed-triple (map atom-or-cdr (cdr substitutions)))
               (current-graphs (or (cdr-when (assoc renamed-triple (get-binding/default 'triple-graphs bindings '()))) '()))

               ;; is this done in the right place???
               ;; and what about bindings/substitutions
               ;; and what about namespaces?
               ;; and what about a validity check?
               (fproperty (match renamed-triple ((s p o) (get-binding s p 'functional-property bindings)))))

          (if (member graph current-graphs)
              (values '() new-bindings)
              (values               
               (list renamed-triple)
               (fold-binding renamed-triple
                             'triple-graphs
                             (lambda (triple graphs-list)
                               (alist-update triple
                                             (cons (atom-or-cdr graph)
                                                   current-graphs)
                                             graphs-list))
                             '()
                             (if (and fproperty (sparql-variable? (third renamed-triple)))
                                 (cons-binding (cons (third renamed-triple) fproperty) 'fproperties-bindings new-bindings)
                                 new-bindings)))))))))

(define (project-substitutions vars substitutions)
  (filter (lambda (substitution)
            (member (car substitution) vars))
          substitutions))

(define constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings) 
          (match block
            ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
             (let* ((subselect-vars (extract-subselect-vars vars))
                    (projected-substitutions (project-substitutions subselect-vars
                                                                    (get-binding 'constraint-substitutions bindings))))
             (let-values (((rw new-bindings) (rewrite quads (update-binding 'constraint-substitutions projected-substitutions
                                                                            bindings))))
               (let ((projected-bindings (update-binding 'constraint-substitutions
                                                         (append
                                                          (project-substitutions
                                                           subselect-vars (get-binding 'constraint-substitutions new-bindings))
                                                          (get-binding 'constraint-substitutions bindings))
                                                         new-bindings)))
               (if (null? rw)
                   (values '() projected-bindings)
                   (values `((@SubSelect (,(car (second block) )
                                          ,@(filter sparql-variable?
                                                    (map (lambda (var)  ;; what about Expressions??
                                                           (if (equal? var '*) var
                                                               (current-substitution var projected-bindings)))
                                                         vars)))
                                         (WHERE ,@rw) ,@rest))
                           projected-bindings)))))))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings)
                        (rewrite (cdr block)
                                 (update-binding
                                  'dependencies (constraint-dependencies block bindings)
                                  bindings))))
            (values `((WHERE ,@rw)) new-bindings)))) ;; (delete-binding 'dependencies new-bindings)))))
    ((UNION) . ,rw/continue)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
               (if (null? (cdr rw))
                   (values '() new-bindings)
                   (values
                    `((GRAPH ,(substitution-or-value graph new-bindings)
                             ,@(cdr rw)))
                new-bindings)))))))
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
  (let ((where-block (rewrite where-block '() expand-quads-rules)))
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
  `(((@SubSelect) 
     ;; . ,rw/subselect)
     ;; necessary to project here?? probably yes
     . ,(lambda  (block bindings)
          (match block
            ((@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) (`WHERE . quads) . rest)
             (let-values (((rw new-bindings) (rewrite quads (subselect-bindings vars bindings))))
               (values
                `((@SubSelect ,(second block) (WHERE ,@(group-graph-statements rw)) ,@rest))
                (merge-subselect-bindings vars new-bindings bindings)))))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (let* ((shared-variable-triples 
                     (filter values
                             (map (match-lambda
                                    ((a b binding)
                                     (let ((vars (map car binding)))
                                       (and (any values (map (cut member <> vars) triple))
                                            (list a b binding)))))
                                  (get-binding/default 'instantiated-quads bindings '())))))
               (if (null? shared-variable-triples)
                   (values (list block) bindings)
                   (values 
                    (union-over-instantiation block shared-variable-triples bindings)
                    bindings)))))))
    (,symbol? . ,rw/copy)
    (,null? . ,rw/remove)
    ((WHERE UNION OPTIONAL MINUS)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (values `((,(car block) ,@(group-graph-statements rw))) new-bindings))))
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

(*port* 8890)

;; (use slime)

;; (when (not (feature? 'docker))
;;   (define *swank*
;;     (thread-start!
;;      (make-thread
;;       (lambda ()
;;         (swank-server-start 4005))))))

