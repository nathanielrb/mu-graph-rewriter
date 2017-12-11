;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instantiation
;; for INSERT and INSERT DATA
;; To do: expand namespaces before checking
(define (instantiate where-block triples)
  (if (nulll? where-block) where-block
      (let ((where-block (expand-graphs where-block)))
        (rewrite (list where-block) '() (get-instantiation-matches-rules triples)))))

;; matches a triple against a list of triples, and if successful returns a binding list
;; ex: (match-triple '(?s a dct:Agent) '((<a> <b> <c>) (<a> a dct:Agent)) => '((?s . <a>))
;; but should differentiate between empty success '() and failure #f
(define (match-triple triple triples #!optional success?)
  (if (nulll? triples) 
      (and success? '())
      (let loop ((triple1 triple) (triple2 (car triples)) (match-binding '()))
        (cond ((null? triple1) 
               (if (null? match-binding)
                   (match-triple triple (cdr triples) #t) ; success
                   (cons match-binding (match-triple triple (cdr triples) #t))))
              ((and (sparql-variable? (car triple1))
                    (not (equal? (car triple1) (car triple2)))) ; no substitutions '(?s . ?s)
               (loop (cdr triple1) (cdr triple2) (cons (cons (car triple1) (car triple2)) match-binding)))
              ((rdf-equal? (car triple1) (car triple2))
               (loop (cdr triple1) (cdr triple2) match-binding))
              (else (match-triple triple (cdr triples) success?))))))

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
;; To do: abstract out matching for more complex quads patterns

;; but now is all this *graph* etc. necessary??
(define (get-instantiation-matches-rules triples)
  `(((@SubSelect) . ,rw/subselect)
    (,annotation? . ,rw/copy)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (let ((match-bindings (match-triple triple triples)))
               (cond ((not match-bindings) (values (list block) bindings))
                     ((null? match-bindings)  (values `((*graph* . ,graph)) bindings))
                     (else (values `((UNION (,block) ;; ????
                                            ,(map (lambda (bindings) 
                                                    `(VALUES ,(map car bindings) 
                                                             ,(map cdr bindings)))
                                                  match-bindings)))
                             bindings))))))))
    ((UNION)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let ((new-blocks (filter values rw))) ; (compose not fail?) 
              (case (length new-blocks)
                ((0)  (values (list new-blocks) new-bindings))
                ((1)  (values `((OPTIONAL ,@(car new-blocks))) new-bindings))
                (else (values `((UNION ,@new-blocks)) new-bindings)))))))
    (,quads-block? 
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (rewrite (list (cdr block)) bindings)))
            (if (fail? rw) (values (list #f) new-bindings)
                (values `((,(car block) ,@(join rw))) new-bindings)))))
    ((FILTER)
     . ,(lambda (block bindings)
    	  (match block
    	    ((`FILTER (`|NOT EXISTS| . quads)) ; way too specific
    	     (let ((not-triples (expand-triples quads))) ; generalize this for real quads!
	       (let ((trs
		      (remove 
		       null?
		       (map (lambda (ntriple)
			      (let ((match-bindings (match-triple ntriple triples)))
				(cond ((null? match-bindings) '())
				      ((not match-bindings) ntriple)
				      (else '()))))  ; constraint?
			    not-triples))))
		 (values (if (null? trs) (list #f)
			     `((FILTER (|NOT EXISTS| ,@trs))))
			 bindings))))
    	    (else 
	     (values (list block) bindings)))))
    ((VALUES FILTER BIND) . ,rw/copy) ;; ???
    (,symbol? . ,rw/copy)
    (,nulll? . ,rw/remove)
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
            (cond ((nulll? rw)
                   (values '() new-bindings))
                  ((fail? rw) (values (list #f) new-bindings))
                  (else
                   (let loop ((statements (car rw)) 
                              (filter-statements '()) (values-statements '()) 
                              (graphs '()) (quads '()))
                     (if (null? statements)
			 ;; ! really approximate...
                         ;; (if (and #f (null? quads) (null? filter-statements) (pair? values-statements))
                         ;; ;; (if (and (null? quads) (pair? values-statements))
                         ;;     (match (car values-statements) ;; ** only 1!!
                         ;;       ((`VALUES vars . vals)
                         ;;        (let ((new-instantiated-values
                         ;;               (if (pair? vars)
                         ;;                   (map reverse
                         ;;                        (fold (lambda (n r) (map cons n r))
                         ;;                              (map list vars)
                         ;;                              vals))
                         ;;                   `((,vars . ,vals)))))
                         ;;          (values '()
                         ;;                  (update-instantiated-values new-instantiated-values new-bindings)))))
                             (let ((rw-non-graphs (filter (not-node? '*graph*) (car rw))))
                               (values (if (null? rw-non-graphs) '() (list rw-non-graphs))
                                       (if (null? graphs)
                                           new-bindings
                                           (fold-binding graphs 'instantiated-graphs append-unique '() new-bindings))))  ;)
                         (case (caar statements)
                           ((*graph*) (loop (cdr statements) filter-statements
                                            values-statements (cons (cdar statements) graphs) quads))
                           ((FILTER) (loop (cdr statements) (cons (car statements) filter-statements) 
                                           values-statements graphs quads))
                           ((VALUES) (loop (cdr statements) filter-statements 
                                           (cons (car statements) values-statements) graphs quads))
                           ((BIND) (loop (cdr statements) filter-statements values-statements graphs quads)) ;; ??
                           (else (loop (cdr statements) filter-statements
                                       values-statements graphs (cons (car statements) quads)))))))))))))

  ;; (define (match-instantiated-quads quad bindings)
  ;;   (filter values
  ;;           (map (match-lambda
  ;;       	 ((a b binding)
  ;;       	  (let ((vars (map car binding)))
  ;;       	    (and (any values (map (cut member <> vars) quad))
  ;;       		 (list a b binding)))))
  ;;                (get-binding/default 'instantiated-quads bindings '()))))

;; (define instantiation-union-rules
;;   `(((@SubSelect) 
;;      . ,(lambda  (block bindings)
;;           (match block
;;             ((`@SubSelect (label . vars) . rest)
;;              (let-values (((rw new-bindings)
;; 			   (rewrite (get-child-body 'WHERE rest) 
;; 				    (subselect-bindings vars bindings))))
;;                (values
;;                 `((@SubSelect (,label ,@vars)
;;                               ,@(replace-child-body 'WHERE rw rest)))
;;                 (merge-subselect-bindings vars new-bindings bindings)))))))
;;     ((WHERE OPTIONAL MINUS) . ,rw/quads)
;;     (,annotation? . ,rw/copy)
;;     ((UNION)
;;      . ,(lambda (block bindings)
;;           (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
;; 	    (let ((new-blocks (filter values rw)))
;; 	      (case (length new-blocks)
;; 		((0)  (values '() new-bindings))
;;                 ((1)  (values new-blocks new-bindings))
;; 		(else (values `((UNION ,@new-blocks)) new-bindings)))))))
;;     ((GRAPH) 
;;      . ,(lambda (block bindings)
;;           (match block
;;             ((`GRAPH graph triple)
;;              (let ((matching-quads (match-instantiated-quads (cons graph triple) bindings)))
;;                (log-message "~%matching ~A => ~A~%" triple matching-quads)
;;                (if (null? matching-quads)
;;                    (values (list block) bindings)
;;                    (let-values (((rw _)
;;                                  (union-over-instantiation block matching-quads bindings)))
;;                      (values rw bindings))))))))
;;     (,symbol? . ,rw/copy)
;;     (,nulll? . ,rw/remove)
;;     ((VALUES). ,rw/copy) ;; ??
;;     ((*graph*) . ,rw/remove)    ;; can we remove this?
;;     ((FILTER) ;; ??
;;      . ,(lambda (block bindings)
;;           (values (list block) bindings)))
;;     (,list? 
;;      . ,(lambda (block bindings)
;;           (let-values (((rw new-bindings) (rw/list block bindings)))
;;             (values (list (group-graph-statements (car rw))) new-bindings))))
;;     ))

;; didn't I already do this somewhere else?
;; and at least do it efficiently...
;; (define (update-instantiated-values kvs bindings)
;;   (if (null? kvs) bindings
;;       (let ((key (caar kvs)) (vals (cdar kvs)))
;;         (fold-binding key 'instantiated-values
;;                       (lambda (key ivals)
;;                         (alist-update key (delete-duplicates (append vals (or (alist-ref key ivals) '()))) ivals))
;;                       '()
;;                       (update-instantiated-values (cdr kvs) bindings)))))

;; (define (union-over-instantiation block matching-quads bindings)
;;   (match block
;;     ((`GRAPH graph triple)
;;      (match (car matching-quads)
;;        ((matched-quad-block instantiated-quad-block binding)
;;         (rewrite
;;          `((UNION (,block ,@matched-quad-block)
;;                   ((GRAPH ,graph ,(instantiate-triple triple binding))
;;                    ,@instantiated-quad-block)))
;;          (update-binding 'instantiated-quads (cdr matching-quads) bindings)
;;          instantiation-union-rules))))))

;; (define (group-graph-statements statements)
;;   ;;   (let loop ((statements statements) (graph #f) (statements-same-graph '()))
;;   (let ((sss (compose symbol->string second)))
;;     (let loop ((statements (sort statements
;;                                  (lambda (a b)
;;                                    (and (equal? (car a) 'GRAPH)
;;                                         (or (not (equal? (car b) 'GRAPH))
;;                                             (and (equal? (car b) 'GRAPH)
;;                                                  (string<=? (sss a) (sss b))))))))
;;            (graph #f) (statements-same-graph '()))
;;     (cond ((nulll? statements)
;;            (if graph `((GRAPH ,graph ,@statements-same-graph)) '()))
;;           (graph
;;            (match (car statements)
;;              ((`GRAPH graph2 . rest) 
;;               (if (equal? graph2 graph)
;;                   (loop (cdr statements) graph (append-unique statements-same-graph rest))
;;                   (cons `(GRAPH ,graph ,@statements-same-graph)
;;                         (loop (cdr statements) graph2 rest))))
;;              (else (cons `(GRAPH ,graph ,@statements-same-graph)
;;                          (cons (car statements)
;;                                (loop (cdr statements) #f '()))))))
;;           (else
;;            (match (car statements)
;;              ((`GRAPH graph . rest) 
;;               (loop (cdr statements) graph rest))
;;              (else (cons (car statements) (loop (cdr statements) #f '()))))))))  )

;; ;; This isn't quite correct. Should be done per-tuple, right?
;; (define (instantiate-values block substitutions graphs)
;;   (let ((substitutions (if (or #t (not graphs)) ; b/c graphs shouldn't be variables...??
;;                            substitutions
;;                            (fold (lambda (graph substitutions)
;;                                    (alist-update-proc graph (lambda (graphs) (cons graph (or graphs '()))) substitutions))
;;                                  substitutions
;;                                  graphs))))
;;     (rewrite block '() (instantiate-values-rules substitutions))))

;; (define (instantiate-values-rules substitutions)
;;   `(((GRAPH) 
;;      . ,(lambda (block bindings)
;;           (match block
;;             ((`GRAPH graph . triples)
;;              (let ((new-graphs (alist-ref graph substitutions)))
;;                (if new-graphs
;;                    (let-values (((rw _) (rewrite triples)))
;;                      (values (map (lambda (new-graph)
;;                                     `(GRAPH ,new-graph ,@triples))
;;                                   new-graphs)
;;                              bindings))
;;                    (values (list block) bindings)))))))
;;     (,triple? 
;;      . ,(lambda (triple bindings)
;;           (values
;;            (let loop ((triple triple) (k list))
;;              (if (null? triple) (k '())
;;                  (let ((subs (alist-ref (car triple) substitutions)))
;;                    (if subs
;;                        (loop (cdr triple)
;;                              (lambda (rest)
;;                                (join
;;                                 (map
;;                                  (lambda (s) (k (cons s rest)))
;;                                  subs))))
;;                        (loop (cdr triple)
;;                              (lambda (rest)
;;                                (k (cons (car triple) rest))))))))
;;            bindings)))

;;     (,list? . ,rw/list)))
