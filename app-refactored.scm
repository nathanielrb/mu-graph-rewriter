(use s-sparql s-sparql-parser mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client)

(define (nested-alist-ref alist #!rest keys)
  (nested-alist-ref* keys alist))

(define (nested-alist-ref* keys alist)
  (and keys       
       (if (null? keys)
           alist
           (let ((nested (alist-ref (car keys) alist)))
             (and nested
                  (nested-alist-ref* (cdr keys) nested))))))

(define (nested-alist-update* keys val alist)
  (let ((key (car keys)))
    (if (null? (cdr keys))
        (alist-update key val alist)
        (alist-update 
         key
         (nested-alist-update*
          (cdr keys) val (or (alist-ref key alist) '()))
          alist))))

(define (nested-alist-update alist val #!rest keys)
  (nested-alist-update* keys val alist))

(define (nested-alist-replace* keys proc alist)
  (nested-alist-update* keys (proc (nested-alist-ref* keys alist)) alist))

(define (nested-alist-replace alist proc #!rest keys)
  (nested-alist-replace* keys proc alist))

(define (update-bindings* vars key val bindings)
  (nested-alist-update* (append vars (list '@bindings key)) val bindings))

(define (get-binding* vars key bindings)
  (nested-alist-ref* (append vars (list '@bindings key)) bindings))

(define (project-bindings vars bindings)
    (let loop ((bindings bindings) (projected-bindings '()))
      (if (null? bindings) projected-bindings
	  (let ((binding (car bindings)))
	    (cond ((member (car binding) vars)
		   (loop (cdr bindings)
			 (append projected-bindings
				 (list (cons (car binding)
					     (project-bindings vars (cdr binding)))))))
		  ((equal? (car binding) '@bindings)
		   (loop (cdr bindings)
			 (append projected-bindings (list binding))))
		  (else
		   (loop (cdr bindings) projected-bindings)))))))

;; should be (lambda ()) thunk, for dynamic parameters...
(define rules (make-parameter '()))

(define (rewrite blocks bindings rules)
  (let loop ((blocks blocks) (statements '())
	     (bindings bindings) (constraints '()))
    (if (null? blocks)
	(values statements bindings constraints)
	(let-values (((new-statements updated-bindings new-constraints)
		      (apply-rules (car blocks) bindings rules)))
	  (loop (cdr blocks)
		(append statements new-statements)
		updated-bindings
		(append constraints new-constraints))))))

(define (apply-rules block bindings rules)
  (let ((rule-match? (lambda (rule)
		      (or (and (symbol? rule) (equal? rule block))
			  (and (pair? rule) (member (car block) rule))
			  (and (procedure? rule) (rule block))))))
    (let loop ((remaining-rules rules))
      (if (null? remaining-rules) (abort (format #f "No matching rule for ~A" block))
	  (match (car remaining-rules)
	    ((rule . proc) 
	     (if (rule-match? rule)
		 (proc block bindings rules)
		 (loop (cdr remaining-rules)))))))))
	
(define (triple? expr)
  (and (pair? expr)
       (or (iri? (car expr))
	   (sparql-variable? (car expr))
	   (blank-node? (car expr)))))

;; subselect?

(define (select? expr)
  (and (pair? expr)
       (member (car expr)
	       `(SELECT |SELECT DISTINCT| |SELECT REDUCED|))))

(define (subselect? group)
  (and (pair? block)
       (select? (car block))))

(define (block? expr)
  (member (car expr)
	  '(WHERE
	    SELECT |SELECT DISTINCT| |SELECT REDUCED| 
	    DELETE |DELETE WHERE| |DELETE DATA|
	    INSERT |INSERT WHERE| |INSERT DATA|
	    |@()| |@[]| MINUS OPTIONAL UNION FILTER BIND GRAPH)))

(define-syntax with-rewrite-values
  (syntax-rules ()
    ((_ ((var expr)) body)
     (let-values (((var updated-bindings new-constraints) expr))
       (values body updated-bindings new-constraints)))))

(define (continue block bindings rules)
  (with-rewrite-values ((new-statements (rewrite (cdr block) bindings rules)))
    `((,(car block) ,new-statements))))

(define (rew/identity block bindings rules)
  (values (list block) bindings '()))

(rules
 `(((@Unit) . ,continue)
   ((@Query) . ,continue) ;; *rewrite-select-queries?*
   ((@Prologue) . ,(lambda (block bindings rules)
		     (values `((@Prologue
			       (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
			       ,@(cdr block)))
			     bindings '())))
   (,select? . ,rew/identity)
   ((WHERE @Dataset) . ,rew/identity)
   (,pair? . ,(lambda (block bindings rules)
		(with-rewrite-values ((rw (rewrite block bindings rules)))
				     (list rw))))))
	       
