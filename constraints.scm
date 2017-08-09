(use matchable s-sparql s-sparql-parser)



(define constraint-rules
  `(((GRAPH) 
     . ,(lambda (block bindings)
          (print (context-head (*context*)))
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite rest bindings)))
             (values `((GRAPH ,graph ,@rw))
                     new-bindings))))))
    (,triple? . ,(lambda (triple bindings)
                   ;; (print "here " (context-head  (*context*)))
                   ;; (print "parent context: " (context-head (context-parent (*context*))))

                   ;; (print "parent: " (context-head (parent-context (*context*))))
                   ;; ;;(print "parent "  ((parent-axis pair?) (*context*)))
                   ;; (print "GRAPH " ((parent-axis
                   ;;                   (lambda (context)
                   ;;                     (print "parent head " (context-head context))
                   ;;                     (match (context-head context)
                   ;;                       ;;((`GRAPH graph . rest) graph) (else #f)))) 
                   ;;                       (`GRAPH 'its-a-graph)
                   ;;                       (else #f))))
                                          
                   ;;                  (*context*)))
                   (newline)
                   (match triple
                     ((s p o)
                      (let ((sv (and (not (sparql-variable? s)) s))
                            (pv (and (not (sparql-variable? p)) p))
                            (ov (and (not (sparql-variable? o)) o))
                            (constraint-bindings (get-binding 'constraint-bindings bindings)))
                      (let ((s* (and (not sv) (alist-ref s constraint-bindings)))
                            (p* (and (not pv) (alist-ref p constraint-bindings)))
                            (o* (and (not ov) (alist-ref o constraint-bindings))))
                        (let* ((keys (filter values `(,s* ,p* ,o*)))
                               (lookup (lambda (x) (get-binding* keys x bindings))))
                          (let ((s** (or s* (lookup s*)))
                                (p** (or p* (lookup p*)))
                                (o** (or o* (lookup o*))))
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
                                       (filter values
                                               (map (match-lambda
                                                      ((x xv x** x***) 
                                                       (and (not xv) (not x**) x*** 
                                                            `(,keys ,x ,x***))))
                                                    `((,s ,sv ,s** ,s***)
                                                      (,p ,pv ,p** ,p***)
                                                      (,o ,ov ,o** ,o***))))
                               bindings))))))))))))
    (,pair? . ,rw/continue)))

(define C
  `((@Query (WHERE (GRAPH ?g (?s ?p ?o) (?s a ?type) (?type mu:about <Aad>))))))

(define initial-bindings
  '((@bindings . ((constraint-bindings . ((?s . ?a) (?p . ?b) (?o . ?c)))))))


(let-values (((a b) (rewrite C initial-bindings constraint-rules)))
  (print a)
  (print b))
