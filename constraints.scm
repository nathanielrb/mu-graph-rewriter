(use matchable s-sparql s-sparql-parser)
(load "app.scm")

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


;; to do
;; test if (s p o) = (a b c) and set 'graph binding
;; to do this, fix context threading

;;;
;;; testing

(define C
  `((@Query 
     (WHERE
      (GRAPH ?g
             (?s ?p ?o)
             (?s a ?type)
             (?type mu:about <Aad>))))))

(define D
  `((WHERE
      (GRAPH ?g
             (?s <knows> ?person)
             (?x <employs> ?person)
             (?x <knows> ?o)))))

(define initial-bindings
  '((@bindings . ((constraint-bindings . ((?s . ?a) (?p . ?b) (?o . ?c)))))))


;; (rewrite E '() (top-rules '?a '?b '?c))
