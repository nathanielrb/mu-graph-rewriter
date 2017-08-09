(use matchable s-sparql s-sparql-parser)
(load "app.scm")
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
              (values
               (map (match-lambda 
                      ((v . ds)
                       (let-values (((bound unbound) (partition bound? ds)))
                         (let ((all-dps
                                (append bound
                                        (delete v
                                                (join (map (lambda (u) (or (alist-ref u deps) '())) 
                                                           unbound))))))
                         (cons v (delete-duplicates
                                  (filter values (map (lambda (b) (and (member b all-dps) b)) bs))))))))
                    deps)
              bindings)))))
    (,pair? . ,rw/continue)))

(define (get-dependencies query bindings)
  (rewrite query bindings dependency-rules))

(define constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) 
                        (rewrite (cdr block)
                                 (update-binding 'dependencies (get-dependencies (list block) bindings) bindings))))
            (values
             `((WHERE ,@rw))
             new-bindings))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite `(,graph ,@rest) bindings)))
             (values `((GRAPH ,graph ,@rw))
                     new-bindings))))))
    (,triple? . ,(lambda (triple bindings)
                   (let ((graph
                          (match (context-head
                                  ((parent-axis (lambda (context) (equal? (car (context-head context)) 'GRAPH)))
                                   (*context*)))
                            ((`GRAPH graph . rest) graph)
                            (else #f))))
                   (match triple
                     ((s p o)
                      (let* ((sv (and (not (sparql-variable? s)) s))
                             (pv (and (not (sparql-variable? p)) p))
                             (ov (and (not (sparql-variable? o)) o))
                             (constraint-bindings (get-binding 'constraint-bindings bindings))
                             (name (lambda (x) (alist-ref x constraint-bindings)))
                             (dependencies (get-binding 'dependencies bindings))
                             (dep (lambda (x) (alist-ref x dependencies))))
                      (let ((s* (and (not sv) (name s)))
                            (p* (and (not pv) (name p)))
                            (o* (and (not ov) (name o))))
                        (let* ((keys (lambda (x) (map name (or (dep x) '()))))
                               (lookup (lambda (x)
                                         (alist-ref x (or (get-binding* (keys x) (dep x) bindings) '())))))
                          (let ((s** (and (not sv) (or s* (lookup s))))
                                (p**  (and (not pv) (or p* (lookup p))))
                                (o**  (and (not ov) (or o* (lookup o)))))
                            (let ((s*** (or s** (gensym s)))
                                  (p*** (or p** (gensym p)))
                                  (o*** (or o** (gensym o))))
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
                                                       (,o ,ov ,o* ,o** ,o***))))
                                        (get-binding 'constraint-bindings bindings))
                                      (update-bindings*
                                       (append
                                        (if (equal? `(,s ,p ,o) (get-binding 'constraint-triple bindings))
                                            `((,(list (name s) (name p) (name o)) graph ,graph))
                                            '())
                                       (filter values
                                               (map (match-lambda
                                                      ((x xv x** x***) 
                                                       (and (not xv) (not x**) x*** 
                                                              `(,(keys x) ,(dep x)
                                                                ,(alist-update
                                                                  x x*** (or (get-binding* (keys x) (dep x) bindings) '()))))))
                                                    `((,s ,sv ,s** ,s***)
                                                      (,p ,pv ,p** ,p***)
                                                      (,o ,ov ,o** ,o***)))))
                                       bindings)))))))))))))
    (,pair? . ,rw/continue)))

;; to do
;; test if (s p o) = (a b c) and set 'graph binding
;; to do this, fix context threading

(define (top-rules a b c)
  `( ((@Unit) . ,rw/continue)
     ((@Query) 
      . ,(lambda (block bindings)
           (let ((where (list (assoc 'WHERE (cdr block)))))
             (let-values (((rw new-bindings) (rewrite (cdr block) 
                                                      (update-binding 'constraint-where where bindings))))
               (values `((@Query ,@rw)) new-bindings)))))
     ((CONSTRUCT) 
      . ,(lambda (block bindings)
           (let-values (((triples _)
                         (rewrite (cdr block) '() (%expand-triples-rules #t))))
             (rewrite triples bindings))))
     (,triple? 
      . ,(lambda (triple bindings)
           (match triple
             ((s p o)
              (let ((cbs `((,s . ,a) (,p . ,b) (,o . ,c))))
                (let-values (((rw new-bindings)
                              (rewrite (get-binding 'constraint-where bindings)
                                       (update-bindings ('constraint-bindings cbs)
                                                        ('constraint-triple (list s p o))
                                                        bindings)
                                       constraint-rules)))
                  (values `((GRAPH ,(get-binding a b c 'graph new-bindings) ,triple))
                          ;; also delete un-used bindings:
                          ;; constraint-where constraint-triples ...
                          (update-bindings
                           ('constraints (append rw (get-binding 'constraints bindings)))
                           new-bindings))))))))
     (,pair? . ,rw/remove)))

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

(define E
  `((@Unit
     (@Query
      (CONSTRUCT (?s ?p ?o))
      (WHERE
       (GRAPH ?g
              (?s ?p ?o)
              (?s <knows> ?person)
              (?x <employs> ?person)
              (?x <knows> ?o)))))))

;; (rewrite E '() (top-rules '?a '?b '?c))
