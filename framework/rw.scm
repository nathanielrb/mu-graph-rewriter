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

;; duplicate?
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
    ((`@SubSelect (label . vars) . rest)
     (let* ((subselect-vars (extract-subselect-vars vars))
            (inner-bindings (if (or (equal? subselect-vars '(*))
                                    (equal? subselect-vars '*))
                                bindings
                                (project-bindings subselect-vars bindings)))
            (vars (if (equal? vars '(*))
                      (get-vars rest)
                      vars)))
       (let-values (((rw new-bindings) (rewrite rest inner-bindings)))
         ;; (values `((@SubSelect (,label ,@(filter sparql-variable? vars)) ,@rw)) 
         (values `((@SubSelect (,label ,@vars) ,@rw))
                 (merge-bindings 
                  (project-bindings subselect-vars new-bindings)
                  bindings)))))))

;; (define (fail-or-null rw new-bindings)
;;   (cond ((nulll? rw) (values '() new-bindings))
;;         ((fail? rw) (values '(#f) new-bindings))
;;         (else #f)))

(define-syntax fail-or-null
  (syntax-rules ()
    ((_ rw new-bindings body)
     (cond ((nulll? rw) (values '() new-bindings))
           ((fail? rw) (values '(#f) new-bindings))
           (else body)))))

(define (rw/list block bindings)
  (let-values (((rw new-bindings) (rewrite block bindings)))
    (fail-or-null rw new-bindings
                  (values (list (filter pair? rw)) new-bindings))))

(define (rw/quads block bindings)
  (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
    (cond ((nulll? rw) (values '() new-bindings))
          ((fail? rw) (values (list #f) new-bindings))
	  (else (values `((,(car block) ,@(filter pair? rw))) new-bindings)))))

;; this is still a little incorrect (?) wrt failure and empty ()
(define (rw/union block bindings)
  (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
    (let ((new-blocks (filter values rw))) ; (compose not fail?)  ?
      (case (length new-blocks)
	((0)  (values (list #f) new-bindings))
        ((1)  (values (car new-blocks) new-bindings))
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

(define (replace-child-body-if label replacement block)
  (if replacement
      (alist-update label replacement block)
      block))

(define (append-child-body/before label new-statements block)
  (replace-child-body label (append new-statements (or (get-child-body label block) '())) block))

(define (append-child-body/after label new-statements block)
  (replace-child-body label (append (or (get-child-body label block) '()) new-statements) block))

;; To do: rewrite as cps loop
(define (insert-child-before head statement statements)
  (cond ((nulll? statements) '())
        ((equal? (caar statements) head)
         (cons statement statements))
        (else (cons (car statements)
                    (insert-child-before head statement (cdr statements))))))

;; To do: rewrite as cps loop
(define (insert-child-after head statement statements)
  (cond ((nulll? statements) '())
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

(define (a->rdf:type b)
  (if (equal? b 'a) 'rdf:type b))

(define *constraint-prologues* (make-parameter '()))

(define (rdf-equal?* a b)
  ;; (let ((nss (append-unique (constraint-prefixes)
  ;;                           (query-namespaces)
  ;;                           (*namespaces*)))) ; memoize this
    (if (and (symbol? a) (symbol? b))
        (equal? (expand-namespace (a->rdf:type a) (*namespaces*))
                (expand-namespace (a->rdf:type b) (*namespaces*)))
        (equal? a b)))

(define rdf-equal? (memoize rdf-equal?*)) ; needs to take namespaces as param as well

(define (rdf-member a bs)
  (let ((r (filter (cut rdf-equal? a <>) bs)))
    (if (null? r) #f r)))

(define (literal-triple-equal? a b)
  (null? (filter not (map rdf-equal? a b))))

(define replace-a
  (match-lambda ((s p o) `(,s ,(a->rdf:type p) ,o))))

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
;; Axes
;; but isn't this a bit inefficient and backwards?
;; better just use dynamic parameters, I think.
(define delete? (make-parameter #f))

(define insert? (make-parameter #f))

(define where? (make-parameter #f))

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

(define (update?)
  ((parent-axis
    (lambda (context) 
      (let ((head (context-head context)))
        (and head (equal? (car head) '@Update)))))
   (*context*)))




(define all-namespaces (make-parameter '()))

(define (get-constraint-prefixes* read write)
  (append
   (query-prefixes (call-if read))
   (query-prefixes (call-if write))))

(define get-constraint-prefixes (memoize get-constraint-prefixes*))

(define (constraint-prefixes)
  (get-constraint-prefixes (*read-constraint*) (*write-constraint*)))

(define (constraint-and-query-prefixes query)
  (delete-duplicates
   (append (constraint-prefixes)
           (query-prefixes query)
           (*namespaces*))))

(define (get-constraint-prologues* read write)
  (append-unique
   (all-prologues (cdr (call-if read)))
   (all-prologues (cdr (call-if write)))))

(define get-constraint-prologues (memoize get-constraint-prologues*))

(define (constraint-prologues)
  (get-constraint-prologues (*read-constraint*)  (*write-constraint*)))

  ;; (let ((nss (append-unique (constraint-prefixes)
  ;;                           (query-namespaces)
  ;;                           (*namespaces*)))) ; memoize this
