(use s-sparql s-sparql-parser mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Expand triples

(define (expand-sequence-path-triple triple)
  (and (= (length triple) 3)
       (sequence-path? (cadr triple))
       (match triple
         ((s (`/ . ps) o)
          (let loop ((s s)
                     (ps ps))
            (if (= (length ps) 1)
                (expand-triple (list s (car ps) o))
                (let ((object (new-blank-node)))
                  (append (expand-triple (list s (car ps) object))
                          (loop object (cdr ps))))))))))

(define (expand-expanded-triple s p o)
  (cond  ((blank-node-path? o)
          (expand-special (list s p o)))
         ((sequence-path? p)
          (expand-sequence-path-triple (list s p o)))
         (else
          (list (list s p o)))))

(define (expand-blank-node-path triple)
  (cond ((blank-node-path? (car triple))
         (let ((subject (new-blank-node)))
	   (append (expand-triple (cons subject (cdr triple)))
		   (expand-triple (cons subject (cdar triple))))))
        ((and (= (length triple) 3)
              (blank-node-path? (caddr triple)))
         (let ((object (new-blank-node)))
           (match triple
             ((s p (_ . rest))
               (append (expand-triple (list s p object))
                       (expand-triple (cons object rest)))))))        
        (else #f)))

(define (expand-triple triple)
  (or (expand-blank-node-path triple)
      (match triple
	((subject predicates)
	 (let ((subject (car triple)))
	   (join
	    (map (lambda (po-list)
		   (let ((predicate (car po-list))
			 (object (cadr po-list)))
		     (if (and (list? object) (not (blank-node-path? object)))
			 (join
			  (map (lambda (object)
				 (expand-expanded-triple subject predicate object))
			       (cadr po-list)))
			 (expand-expanded-triple subject predicate object))))
		 predicates))))
	((subject predicate objects)
	 (if (list? objects)
	     (join
	      (map (lambda (object)
		     (expand-expanded-triple subject predicate object))
		   objects))
	     (expand-expanded-triple subject predicate objects))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(define rules (make-parameter (lambda () '())))

(define (nested-alist-ref keys alist)
  (if (null? keys)
      alist
      (let ((nested (alist-ref (car keys) alist)))
	(and nested
	     (nested-alist-ref (cdr keys) nested)))))

(define (nested-alist-update keys val alist)
  (let ((key (car keys)))
    (if (null? (cdr keys))
        (alist-update key val alist)
        (alist-update 
         key
         (nested-alist-update
          (cdr keys) val (or (alist-ref key alist) '()))
          alist))))

(define (nested-alist-replace keys proc alist)
  (nested-alist-update keys (proc (nested-alist-ref keys alist)) alist))

(define (update-binding vars key val bindings)
  (nested-alist-update (append vars (list '@bindings key)) val bindings))

(define (get-binding vars key bindings)
  (nested-alist-ref (append vars (list '@bindings key)) bindings))

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

(define (rewrite blocks bindings rules)
  (let loop ((blocks blocks)
	     (statements '()) (constraints '())
	     (bindings bindings))
    (if (null? blocks)
	(values statements constraints bindings)
	(let-values (((new-statements new-constraints updated-bindings)
		      (apply-rules (car blocks) bindings rules)))
	  (loop (cdr blocks)
		(append statements new-statements)
		(append constraints new-constraints)
		updated-bindings)))))

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

(define-syntax with-rewrite-values
  (syntax-rules ()
    ((_ ((var expr)) body)
     (let-values (((var new-constraints updated-bindings) expr))
       (values body new-constraints updated-bindings)))))

;; (define-syntax rew/lambda
;;   (syntax-rules ()
;;     ((_ ...

(define (rew/continue block bindings rules)
  (with-rewrite-values ((new-statements (rewrite (cdr block) bindings rules)))
    `((,(car block) ,new-statements))))

(define (rew/copy block bindings rules)
  (values (list block) '() bindings))

;;;;;;;;;;;;;;;;
;;; -> s-sparql

(define (triple? expr)
  (and (pair? expr)
       (or (iri? (car expr))
	   (sparql-variable? (car expr))
	   (blank-node? (car expr)))))

(define (select? expr)
  (and (pair? expr)
       (member (car expr)
	       `(SELECT |SELECT DISTINCT| |SELECT REDUCED|))))

(define (subselect? group)
  (and (pair? block)
       (select? (car block))))

;; |@()| |@[]|

(define (quads-block? expr)
  (member (car expr)
	  '(WHERE
	    DELETE |DELETE WHERE| |DELETE DATA|
	    INSERT |INSERT WHERE| |INSERT DATA|
	    MINUS OPTIONAL UNION FILTER BIND GRAPH)))


(define expand-triples-rules
  `((,triple? . ,(lambda (block bindings rules)
		  (let ((triples (expand-triple block)))
		    (let loop ((ts triples) (bindings bindings))
		      (if (null? ts)
			  (values triples  '() bindings)
			  (match (car ts)
			    ((s `a o)
			     (loop (cdr ts)
				   (update-binding (list s) 'stype o bindings)))
			    ((s p o)
			     (if (get-binding (list s) 'stype bindings)
				 (loop (cdr ts) bindings)
				 (let ((stype (new-sparql-variable "stype")))
				   ;;(or (and (iri? s) (get-type (expand-namespace s (query-namespaces))))
				   (loop (cdr ts)
					 (update-binding
					  (list s) 'stype stype bindings)))))))))))
    (,quads-block? . ,rew/copy)))
					    

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
		      
(define triples-rules
  `((,triple? . ,(lambda (triple bindings rules)
		   (match triple
		     ; (s 'a t)
		     ((s p o)
		      (values `((*REWRITTEN* (GRAPH ?gr123 ,triple)))
			      '((GRAPH <graphs> (?gr123 a <Graph>)))
			      (update-binding (list s p) 'graph '?gr123 bindings))))))
	      
    (,quads-block? .,rew/copy)))

(rules
 `(((@Unit) . ,rew/continue)
   ((@Query) . ,rew/continue) ;; *rewrite-select-queries?*
   ((@Prologue) . ,(lambda (block bindings rules)
		     (values `((@Prologue
			       (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
			       ,@(cdr block)))
			     '()
			     bindings)))
   (,select? . ,rew/copy)
   ((@Dataset) . ,rew/copy)
   (,quads-block? . ,(lambda (block bindings rules)
		       (let-values (((first-pass _ first-bindings)
				     (rewrite (cdr block) bindings expand-triples-rules)))
			 (let-values (((second-pass triples-constraints second-bindings)
				     (rewrite first-pass first-bindings triples-rules)))
			   (let-values (((third-pass quads-constraints third-bindings)
					 (rewrite second-pass second-bindings rules)))
			     (values `((WHERE ,@third-pass))
				     (append triples-constraints quads-constraints)
				     third-bindings))))))
   ((*REWRITTEN*) . ,(lambda (block bindings rules)
		       (values (cdr block) '() bindings)))
   (,pair? . ,(lambda (block bindings rules)
		(with-rewrite-values ((rw (rewrite block bindings rules)))
				     (list rw))))))
	       
