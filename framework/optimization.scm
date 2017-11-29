
(define level-quads (make-parameter '()))

(define (rewritten? block) (equal? (car block) '*REWRITTEN*))

(define (new-level-quads new-quads)
  (append 
   (level-quads)
   (join (map cdr (filter rewritten? new-quads)))))

;; Virtuoso bug (?) when VALUES statement is not after all statements
;; whose variables it binds. Maybe not a bug at all. To think about.
(define (reorder block)
  (let loop ((statements block) (new-statements '())
             (values-statements '()) (annotations '()))
    (cond ((null? statements) (delete-duplicates
                               (append 
                                (reverse annotations)
                                (reverse new-statements)
                                (reverse values-statements))))
          ((equal? (caar statements) 'VALUES)
           (loop (cdr statements) new-statements 
                 (cons (car statements) values-statements) annotations))
          ((annotation? (car statements))
           (loop (cdr statements) new-statements
                 values-statements (cons (car statements) annotations)))
          (else
           (loop (cdr statements) (cons (car statements) new-statements) 
                 values-statements annotations)))))

(define (optimize-duplicates block)
  (lset-difference equal?
                   (delete-duplicates 
                    (group-graph-statements
                     (reorder
                      (filter pair? block))))
                   (level-quads)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Functional Properties (and other optimizations)
(define (apply-optimizations block)
  (if (nulll? block) (values '() '())
      (let-values (((rw new-bindings) (rewrite (list block) '() (optimization-rules))))
        (if (equal? rw '(#f))
            (error (format "Invalid query block (optimizations):~%~A" (write-sparql block)))
            (let-values (((subs quads) (partition subs? rw)))
              (values (group-graph-statements (reorder (delete-duplicates (join quads))))
                      (get-binding/default 'functional-property-substitutions new-bindings '())))))))
                     

(define fprops (make-parameter '((props) (subs))))

(define (replace-fprop elt)
  (if (sparql-variable? elt)
      (let ((subs (alist-ref 'subs (fprops))))
        (or (alist-ref elt subs) elt))
        elt))

(define (subs? exp)
  (and (pair? exp)
       (equal? (car exp) '*subs*)))

(define quads? (compose not subs?))

(define (append-subs rw)
  (let-values (((subs quads) (partition subs? rw)))
    (let* ((top-subs (alist-ref 'subs (fprops)))
          (new-subs (delete-duplicates (filter pair? (append top-subs (join (map second subs)))))))
      (if (null? new-subs) quads
          (cons `(*subs* ,new-subs)
                (map (compose group-graph-statements reorder) quads)))))) ; abstract this!

(define (collect-fprops block #!optional rec?)
  (let-values (((subs quads) (partition subs? block)))
    (let loop ((quads quads)
               (props (alist-ref 'props (fprops)))
               (subs (append (join (map second subs)) (alist-ref 'subs (fprops)))))
    (cond ((null? quads) `((props . ,props) (subs . ,subs)))
          ((triple? (car quads))
	   (match (car quads)
	     ((s p o) 
	      (if (rdf-member p (*functional-properties*))
		  (let ((current-value (cdr-when (assoc `(,s ,(a->rdf:type p)) props))))
		    (cond ((not current-value)
			   (loop (cdr quads) (cons `((,s ,(a->rdf:type p)) . ,o) props) subs))
			  ((and (sparql-variable? current-value)
				(not (sparql-variable? o)))
			   (loop (cdr quads) (cons `((,s ,(a->rdf:type p)) . ,o) props) 
				 (assoc-update current-value o subs)))
			  ((rdf-equal? o current-value) (loop (cdr quads) props subs))
			  ((sparql-variable? o)
			   (loop (cdr quads) props 
				 (assoc-update o current-value subs)))
			  (else #f)))
		  (loop (cdr quads) props subs)))))
          ((and (not rec?) (graph? (car quads)))
           (let ((inner (collect-fprops (cddr (car quads)) #t)))
             (if inner (loop (cdr quads) (append (alist-ref 'props inner) props) (append (alist-ref 'subs inner) subs)) #f)))
          (else (loop (cdr quads) props subs))))))

(define collect-level-quads-rules
  `(((GRAPH)
     . ,(lambda (block bindings)
          (match block
           ((`GRAPH graph . rest)
            (values
             (map (lambda (triple)
                    (cons graph triple))
                  (filter triple? rest)) ; not quite right; what about GRAPH ?g1 { GRAPH ?g2 { s p o } } ?
             bindings)))))
    ((VALUES BIND FILTER) . ,rw/copy)
    (,list? . ,rw/remove)))

(define (collect-level-quads block)
  (rewrite block '() collect-level-quads-rules))

(define this-level-quads (make-parameter '()))

(define last-level-quads (make-parameter '()))

(define (optimize-list block bindings)
  (let ((saved-llq (last-level-quads)) ; a bit... awkward
        (saved-tlq (this-level-quads))) 
  (parameterize ((fprops (collect-fprops block))
                 (last-level-quads (this-level-quads))
                 (this-level-quads (append (last-level-quads)  (collect-level-quads block))))
                ;; (log-message "~%Optimize list(0)~%~A~%~%with fprops~%~A~%" block (fprops))
     (if (fprops)
        (let-values (((rw new-bindings) (rw/list (filter quads? block) bindings)))
          ;; (log-message "~%Optimize list(1) ~A~%~%" rw)
          (cond ((fail? rw) (values (list #f) new-bindings)) ; duplicate #f check... which one is correct?
                ((or (equal? (map (cut filter quads? <>) rw)
                             (list (filter quads? block)))
                     (equal? rw '(#f)))
                 (values rw new-bindings))
                (else (let ((fp (fprops))) ; ?
                        (parameterize ((fprops (collect-fprops rw)) 
                                       (this-level-quads saved-tlq)
                                       (last-level-quads saved-llq))
                          (let-values (((rw2 nb2) (rewrite rw new-bindings)))
                            ;; (log-message "~%Optimize list(2)~%~A~%~%" rw2)
                                (values (append-subs rw2) nb2)))))))
        (values (list #f) bindings))) ) )

(define optimizations-graph (make-parameter #f))

(define (optimization-rules)
  `(((@UpdateUnit @QueryUnit @Update @Query) . ,rw/continue)
    ((@Prologue @Dataset GROUP |GROUP BY| LIMIT ORDER |ORDER BY| OFFSET) . ,rw/copy)
    (,select? . ,rw/copy)
    (,annotation? 
     . ,(lambda (block bindings)
          (match block
            ((`@Annotation `access key var) 
             (values `((@Annotation access ,key ,(replace-fprop var))) bindings))
            (else (values (list block) bindings)))))
    ((@SubSelect)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rw/subselect block bindings)))
            (values rw new-bindings))))
    ((*subs*) . ,rw/copy)
    ((VALUES) 
     . ,(lambda (block bindings)
          (if (member block (last-level-quads))
              (values '() bindings)
              (match block
                     ((`VALUES vars . vals)
                      (let ((simplified (simplify-values (map replace-fprop vars) vals bindings)))
                      (values
                       (if (null? simplified) '() (list simplified))
                       bindings)))))))
    ((FILTER) 
     . ,(lambda (block bindings)
          (if (member block (last-level-quads))
              (values '() bindings)
              (let ((subs (alist-ref 'subs (fprops))))
                (validate-filter (replace-variables block subs) bindings)))))
    ((BIND) 
     . ,(lambda (block bindings)
          (if (member block (last-level-quads))
              (values '() bindings)
              (values (list block) bindings)))) ; enough?
    ((GRAPH)
     . ,(lambda (block bindings)
          (parameterize ((optimizations-graph (second block)))
            (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
          (if (fail? rw) (values '(#f) new-bindings)
              (let ((rw (delete-duplicates rw)))
                (let-values (((subs quads) (partition subs? rw)))
                  (if (nulll? quads) (values '() new-bindings)
                    (values `(,@(if (null? subs) '()
                                    `((*subs* ,(delete-duplicates (join (map second subs))))))
                              (GRAPH ,(second block) ,@quads))
                            new-bindings)))))))))
    ((UNION)
     . ,(lambda (block bindings)
          ;; this is a beast: abstract it!
          (let-values (((rw new-bindings)
                        (let loop ((rw-bindings (map (lambda (block) ; do this with real let-values
                                                       (let-values (((rw new-bindings) (optimize-list block bindings)))
                                                         (list rw new-bindings)))
                                                     (cdr block)))
                                   (rw '())
                                   (new-bindings '()))
                          (if (null? rw-bindings) (values rw new-bindings)
                              (match (car rw-bindings)
                                ((r b)
                                 (loop (cdr rw-bindings)
                                    (append (filter (compose not fail?) rw) (list r))
                                    (fold-binding (get-binding/default 'functional-property-substitutions b '())
                                                  'functional-property-substitutions merge-alists '() new-bindings))))))))
            (let* ((nss (map second (map car (filter pair? (map (cut filter subs? <>) rw)))))
                   (new-subs (if (null? nss) '() (apply lset-intersection equal? nss)))
                   (new-blocks (filter (compose not fail?)  (map (cut filter quads? <>) rw))) ; * !!
                   (new-subs-block (if (null? new-subs) '() `((*subs* ,new-subs)))))
            (case (length new-blocks)
              ((0)  (values (list #f) bindings))
              ((1) (if (equal? new-blocks '(()))
                       (values '() new-bindings)
                       (values (append new-subs-block (caar new-blocks)) new-bindings))) ; * caar?
              (else (values (append new-subs-block `((UNION ,@(join new-blocks)))) new-bindings))))))  ) ; ? join
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (optimize-list (cdr block) bindings)))
	    (if (fail? rw)
		(abort
		 (make-property-condition
		  'exn
		  'message (format "Invalid query: functional property conflict.")))
		(values `((WHERE ,@(group-graph-statements (join (filter quads? rw))))) new-bindings)))))
    (,quads-block? 
     . ,(lambda (block bindings)
          (match block
            ((`OPTIONAL (`GRAPH graph . rest))
             (values `((GRAPH ,graph (OPTIONAL ,@rest))) bindings))
            (else
             (let-values (((rw new-bindings) (optimize-list (cdr block) bindings)))
               ;; (let-values (((subs quads) (partition subs? (join rw)))
               ;;   (log-message "~%for ~A got:~% ~A~%~A~%" (car block) subs quads)
               ;;   (cond ((fail? quads) (values (list #f) new-bindings))
               ;;         ((nulll? quads) (values '() new-bindings))
               ;;         (else
               ;;          (values `(;; ,@(if (null? subs) '()
               ;;                    ;;       `((*subs* ,(map second subs))))
               ;;                    (,(car block) ,@quads))
               ;;                new-bindings)))))))

               (cond ((fail? rw) (values (list #f) new-bindings))
                     ((nulll? rw) (values '() new-bindings))
                     (else
                      ;; (values `((,(car block) ,@(group-graph-statements (join (filter quads? rw)))))
                      (values `((,(car block) ,@(delete-duplicates (join (filter quads? rw)))))
                              new-bindings))))))))
    (,triple? 
     . ,(lambda (triple bindings)
          (if (member (cons (optimizations-graph) triple) (last-level-quads))
              (values '() bindings)
              ;; (values (list (map replace-fprop triple)) bindings))))

              (match (map replace-fprop triple)
                ((s p o)
                 (cond ((and (*query-functional-properties?*)
                             (not (sparql-variable? s))
                             (rdf-member p (*functional-properties*))
                             (sparql-variable? o))
                        (let ((o* (query-functional-property s p o)))
                          (if o*
                              (values `((*subs* ((,o . ,o*)))
                                        (,s ,p ,o*)) 
                                      (fold-binding `((,o ,o*)) 'functional-property-substitutions 
                                                    merge-alists '() bindings))
                              (values `((,s ,p ,o)) bindings))))
                       ((and (not (sparql-variable? s))
                             (not (sparql-variable? o))
                             (rdf-member p (*queried-properties*)))
                        (if (check-queried-property s p o)
                            (values `((,s ,p ,o)) bindings)
                            (values '(#f) bindings)))
                       (else (values `((,s ,p ,o)) bindings))))))))
                     
    (,list? . ,optimize-list)
    (,symbol? . ,rw/copy)))

(define (query-functional-property s p o)
  (hit-property-cache s p
    (let ((results (sparql-select-unique "SELECT ~A  WHERE { ~A ~A ~A }" o s p o)))
      (and results (alist-ref (sparql-variable-name o) results)))))

(define (sparql-variable-name var)
  (string->symbol (substring (symbol->string var) 1)))

(define (query-property s p)
  (hit-property-cache s p
    (let ((results (sparql-select "SELECT ?o  WHERE { ~A ~A ?o }" s p)))
      (if results (map (cut alist-ref 'o <>) results)
          '()))))

(define (check-queried-property s p o)
  (member o (query-property s p)))


(define (group-graph-statements statements)
  (let loop ((statements statements))
    (if (nulll? statements) '()
        (match (car statements)
          ((`GRAPH graph . rest)
            (let-values (((peers others) (partition (lambda (statement)
                                                      (and (equal? (car statement) 'GRAPH)
                                                           (equal? (cadr statement) graph)))
                                                    (cdr statements))))
              (cons `(GRAPH ,graph ,@(append rest (join (map cddr peers))))
                    (loop others))))
          (s (cons s (loop (cdr statements))))))))

(define clean (compose delete-duplicates group-graph-statements reorder))
