;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constraint Renaming
(define *replace-session-id?* (make-parameter #t))

(define (with-session-id str)
  (lambda ()
    (let ((sid (header 'mu-session-id)))
      (parse-constraint
       (if (and (*replace-session-id?*) sid)
           (irregex-replace/all "<SESSION>" str sid)
           str)))))

;; (define (parse-constraint* constraint)
;;   (let ((constraint
;;          (if (pair? constraint)
;;              constraint
;;              (parse-query
;;               (let ((sid (header 'mu-session-id)))
;;                 (if (and (*replace-session-id?*) sid)
;;                     (irregex-replace/all "<SESSION>" constraint (sparql-escape-uri sid))
;;                     constraint))))))
;;     (car (recursive-expand-triples (list constraint) '() replace-a))))

;; (define parse-constraint (memoize parse-constraint*))

(define (parse-constraint** constraint sid)
  (let ((constraint
         (if (pair? constraint)
             constraint
             (parse-query
              (if (and (*replace-session-id?*) sid)
                  (irregex-replace/all "<SESSION>" constraint (sparql-escape-uri sid))
                  constraint)))))
    (car (recursive-expand-triples (list constraint) '() replace-a))))

(define parse-constraint* (memoize parse-constraint**))

(define (parse-constraint constraint)
  (parse-constraint* constraint (header 'mu-session-id)))

(define (define-constraint key constraint)
  (case key
    ((read) (define-constraint* *read-constraint* constraint))
    ((write) (define-constraint* *write-constraint* constraint))
    ((read/write) 
     (begin
       (define-constraint* *read-constraint* constraint)
       (define-constraint* *write-constraint* constraint)))))

(define (define-constraint* C constraint)
  (if (procedure? constraint)
      (C (lambda () (parse-constraint (constraint))))
      (C (parse-constraint constraint))))

;; Apply CONSTRUCT statement as a constraint on a triple a b c
(define (apply-constraint triple bindings C)
  (parameterize ((flatten-graphs? #f))
                (let* ((C* (if (procedure? C) (C) C)))
                  (match triple
                         ((a (`^ b) c)
                          (rewrite (list C*) bindings (apply-constraint-rules c b a)))
                         ((a ((or `! `? `* `+) b) c)
                          (let-values (((rw new-bindings) (rewrite (list C*) bindings (apply-constraint-rules a b c))))
                            (values (replace-triple rw  `(,a ,b ,c) triple)
                                    new-bindings)))
                         ((a b c)
                          (rewrite (list C*) bindings (apply-constraint-rules a b c)))))))

(define (apply-read-constraint triple bindings)
  (apply-constraint triple bindings (*read-constraint*)))

(define (apply-write-constraint triple bindings)
  (apply-constraint triple bindings (*write-constraint*)))

;; (define (remove-renaming var bindings)
;;  (delete-binding* (keys var bindings) (deps var bindings) bindings))

;; (define (project-renamings ..) 

;; (define (merge-renamings ...)

;;(define (apply-constraint-rule 
