(use s-sparql s-sparql-parser
     mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client cjson)

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Graph Rewriter
(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #f))

(define *cache* (make-hash-table))

(define triples-rules (make-parameter (lambda () '())))

(define select-query-rules (make-parameter (lambda () '())))

(define (rewrite-graphs?)
  (or (header 'preserve-graph-statements)
      (($query) 'preserve-graph-statements)))

(define (rewrite-select?)
  (or (equal? "true" (header 'rewrite-select-queries))
      (equal? "true" (($query) 'rewrite-select-queries))
      (*rewrite-select-queries?*)))

(define (rewrite-quads-block block bindings)
  (let ((rewrite-graph-statements? (not (rewrite-graphs?))))
    (let-values (((q1 b1) (rewrite (cdr block) bindings
                                   (%expand-triples-rules rewrite-graph-statements?))))
      (let-values (((q2 b2) (rewrite q1 b1 ((triples-rules)))))
        (let-values (((q3 b3) (rewrite q2 b2)))
          ;; => rules
          (values `((,(rewrite-block-name (car block)) ,@q3)) b3))))))

(define (%expand-triples-rules rewrite-graph-statements?)
  `((,symbol? . ,rw/copy)
    (,triple? . ,expand-triple-rule)
    ((GRAPH) . ,(%expand-graph-rule rewrite-graph-statements?))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)))

;; expands triple paths and collects bindings
(define (expand-triple-rule block bindings)
  (let ((triples (expand-triple block)))
    (values triples bindings)))

(define (%expand-graph-rule rewrite-graph-statements?)
  (lambda (block bindings)
    (let-values (((rw b) (rewrite (cddr block)
                                  bindings
                                  (%expand-triples-rules rewrite-graph-statements?))))
      (if rewrite-graph-statements?
          (values rw b)
          (values (list block) 
                  (update-binding* '() 'named-graphs 
                                   (cons (second block)
                                         (get-binding/default* '() 'named-graphs b '()))
                                   b))))))

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
                   (let ((constraints (or (get-binding 'constraints new-bindings) '())))
                     (values `((,(car block)
                                ,@(alist-update 'WHERE (append constraints (alist-ref 'WHERE rw)) rw)))
                             new-bindings)))
                 (with-rewrite ((rw (rewrite (cdr block) bindings (select-query-rules))))
                               `((@Query ,rw)))))))
     ((@Update)
      . ,(lambda (block bindings)
           (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
             (let ((where-block (alist-ref 'WHERE rw))
                   (constraints (or (get-binding* '() 'constraints new-bindings) '())))
               (values `((@Update . ,(alist-update
                                      'WHERE
                                      `((SELECT *)
                                        (WHERE ,@constraints ,@where-block))
                                      (reverse rw))))
                       new-bindings)))))
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

(triples-rules
 (lambda ()
   `((,triple? . ,rewrite-triple-rule)
     ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)
     (,select? . ,rw/copy)
     (,subselect? . ,rw/copy))))

(define rewrite-triple-rule
  (lambda (triple bindings)
    (let ((in-where? ((parent-axis (lambda (context) 
                                     (let ((head (context-head context)))
                                       (and head (equal? (car head) 'WHERE))))) (*context*))))
      (let-values (((rw b) (apply-constraint triple bindings)))
        (values rw b)))))

(define *constraint*
  (make-parameter
   `((@Unit
      (@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE
        (GRAPH <http://data.europa.eu/eurostat/graphs>
               (?rule a rewriter:GraphRule)
               (?graph a rewriter:Graph)
               (?rule rewriter:graph ?graph)
               (?rule rewriter:predicate ?p)
               (?rule rewriter:subjectType ?type)
               (?allGraphs a rewriter:Graph))
        (GRAPH ?allGraphs (?s a ?type))
        (GRAPH ?graph (?s ?p ?o))))))))

(define apply-constraint
  (let ((c (*constraint*)))
    (cond ((pair? c)
           (lambda (triple bindings)
             (rewrite c bindings (apply constraint-query-rules triple))))
           ((string? c)
           (let ((constraint (parse-query c)))
             (lambda (triple bindings)
               (rewrite constraint bindings (apply constraint-query-rules triple)))))
          ((procedure? c)
           (lambda (triple bindings)
             (rewrite (c) bindings (apply constraint-query-rules triple)))))))

(define rw/value
  (lambda (block bindings)
    (let-values (((rw new-bindings) (rewrite block bindings)))
      (values (cdr rw) new-bindings))))

(define (rewrite-node node bindings)
  (rewrite (cdr node) bindings))

(define (constraint-query-rules a b c)
  `( (,symbol? . ,rw/copy)
     ((@Unit) . ,rw/value)
     ((@Query) 
      . ,(lambda (block bindings)
           (let ((where (list (assoc 'WHERE (cdr block)))))
             (rewrite-node block (update-binding 'constraint-where where bindings)))))
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
              (let ((cbs `((,s . ,a) (,p . ,b) (,o . ,c))))
                (let-values (((rw new-bindings)
                              (rewrite (get-binding 'constraint-where bindings)
                                       (update-bindings (('constraint-bindings cbs) ('constraint-triple (list s p o)))
                                                        bindings)
                                       constraint-rules)))
                  (let ((graph (get-binding a b c 'graph new-bindings))) ;; = #f!
                  (values (if graph `((GRAPH ,graph (,a ,b ,c)))
                              (list triple))
                          (update-bindings
                           (('constraints (append rw (or (get-binding 'constraints bindings) '()))))
                           (delete-bindings
                            (('dependencies) ('constraint-bindings)
                             ('constraint-triple) ('constraint-where))
                           new-bindings))))))))))
     (,pair? . ,rw/remove)))

(define constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    ((WHERE)
     . ,(lambda (block bindings)
          (rewrite (cdr block)
                   (update-binding
                    'dependencies (get-dependencies (list block) bindings) bindings))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
               (values
                `((GRAPH
                   ,(if (sparql-variable? graph)
                        (or (alist-ref graph (or (get-binding 'constraint-bindings new-bindings) '()))
                            (alist-ref graph (or (get-binding (alist-ref graph (get-binding 'dependencies new-bindings)) 
                                                              new-bindings) '())))
                        graph)
                   ,@(cdr rw)))
                new-bindings))))))
    (,triple? . ,(lambda (triple bindings)
                   (let ((graph
                          (if (equal? triple (get-binding 'constraint-triple bindings))
                              (match (context-head
                                      ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
                                       (*context*)))
                                ((`GRAPH graph . rest) graph)
                                (else #f))
                              #f)))
                   (match triple
                     ((s p o)
                      (let* ((sv (and (not (sparql-variable? s)) s))
                             (pv (and (not (sparql-variable? p)) p))
                             (ov (and (not (sparql-variable? o)) o))
                             (graphv (and graph (not (sparql-variable? graph)) graph))
                             (constraint-bindings (get-binding 'constraint-bindings bindings))
                             (name (lambda (x) (alist-ref x constraint-bindings)))
                             (dependencies (get-binding 'dependencies bindings))
                             (dep (lambda (x) (alist-ref x dependencies))))
                      (let ((s* (and (not sv) (name s)))
                            (p* (and (not pv) (name p)))
                            (o* (and (not ov) (name o)))
                            (graph* (and graph (not graphv) (name graph))))
                        (let* ((keys (lambda (x) (map name (or (dep x) '()))))
                               (lookup (lambda (x)
                                         (alist-ref x (or (get-binding* (keys x) (dep x) bindings) '())))))
                          (let ((s** (and (not sv) (or s* (lookup s))))
                                (p**  (and (not pv) (or p* (lookup p))))
                                (o**  (and (not ov) (or o* (lookup o))))
                                (graph** (and graph (not graphv) (or graph* (lookup graph))))) ;; ** do this
                            (let ((s*** (or s** (gensym s)))
                                  (p*** (or p** (gensym p)))
                                  (o*** (or o** (gensym o)))
                                  (graph*** (and graph (or graph** (gensym graph)))))
                              (values `((,(or sv s***) ,(or pv p***) ,(or ov o***)))
                                      (update-binding
                                       'constraint-bindings
                                       (append
                                        (filter values
                                                (map (match-lambda
                                                       ((x xv x* x** x***) 
                                                        (and (not xv) (not x*) (not x**)
                                                             `(,x . ,x*** ))))
                                                     `((,s ,sv ,s* ,s** ,s***)
                                                       (,p ,pv ,p* ,p** ,p***)
                                                       (,o ,ov ,o* ,o** ,o***)
                                                       (,graph ,graphv ,graph* ,graph** ,graph***))))
                                        (get-binding 'constraint-bindings bindings))
                                       (update-bindings*
                                        (append
                                         (filter values
                                                 (map (match-lambda
                                                        ((x xv x** x***) 
                                                         (and (not xv) (not x**) x*** 
                                                              `(,(keys x) ,(dep x)
                                                                ,(alist-update
                                                                  x x*** (or (get-binding* (keys x) (dep x) bindings) '()))))))
                                                      `((,s ,sv ,s** ,s***)
                                                        (,p ,pv ,p** ,p***)
                                                        (,o ,ov ,o** ,o***)
                                                        (,graph ,graphv ,graph** ,graph***)))))
                                        (if graph
                                            (update-binding* (map name (get-binding 'constraint-triple bindings)) 'graph graph*** bindings)
                                            bindings))))))))))))))
    (,pair? . ,rw/continue)))

(define (get-dependencies query bindings)
  (rewrite query bindings dependency-rules))

(define dependency-rules
  `((,triple? 
     . ,(lambda (triple bindings)
         (let* ((dependencies (or (get-binding 'dependencies bindings) '()))
                (constraint-bindings (get-binding 'constraint-bindings bindings))
                (bound? (lambda (x) (alist-ref x constraint-bindings)))
                (vars (filter sparql-variable? triple)))
           (let-values (((bound unbound) (partition bound? vars)))
             (values
              (list triple)
              (update-binding
               'dependencies
              (fold
               (lambda (v deps)
                 (alist-update v
                               (delete-duplicates
                                (append (delete v vars) (or (alist-ref v deps) '())))
                               deps))
               dependencies
               unbound)
              bindings))))))
     ((GRAPH) . ,(lambda (block bindings)
                   (match block
                     ((`GRAPH graph . rest)
                      (rewrite rest bindings)))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let* ((deps (get-binding 'dependencies new-bindings))
                   (constraint-bindings (get-binding 'constraint-bindings bindings))
                   (bs (map car constraint-bindings))
                   (bound? (lambda (x) (alist-ref x constraint-bindings))))
              (print "deps ")(print deps)
              (values
               (map (match-lambda 
                      ((v . ds)
                       (letrec ((all-deps (lambda (ds deps)
                                            (let-values (((bound unbound) (partition bound? ds)))
                                              ;;(print v ": " ds " == " bound " + " unbound " from " deps)
                                              (if (null? unbound) bound
                                                  (all-deps (delete-duplicates
                                                             (append bound 
                                                                     (delete v
                                                                             (join (map (lambda (u) (or (alist-ref u deps) '())) 
                                                                                        unbound)))))
                                                            (remove (lambda (x) (member (car x) unbound)) deps)))))))
                         (cons v (delete-duplicates
                                  (filter values (map (lambda (b) (and (member b (all-deps ds deps)) b)) bs)))))))
                    deps)
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

(*port* 8890)



