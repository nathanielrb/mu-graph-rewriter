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
;; Framework

(define default-rules (make-parameter (lambda () '())))

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

;; ** !!
(define merge-bindings append)

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

;; (define-syntax rw/lambda
;;   (syntax-rules ()
;;     ((_ ...

(define (rw/continue block bindings rules)
  (with-rewrite-values ((new-statements (rewrite (cdr block) bindings rules)))
    `((,(car block) ,new-statements))))

(define (rw/copy block bindings rules)
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
	    MINUS OPTIONAL UNION GRAPH)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Graph Rewriter

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace rewriter "http://mu.semte.ch/graphs/")

(define *realm* (make-parameter #f))

(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #f))

(define *realm-id-graph*
  (config-param "REALM_ID_GRAPH" '<http://mu.semte.ch/uuid> read-uri))

(define *subscribers-file*
  (config-param "SUBSCRIBERSFILE" "subscribers.json"))

(define *subscribers*
  (handle-exceptions exn '()
    (vector->list
     (alist-ref 'potentials
                (with-input-from-file (*subscribers-file*)
                  (lambda () (read-json)))))))
  
(define *cache* (make-hash-table))

(define *session-realm-ids* (make-hash-table))

(define (get-graph-query stype p)
  `((GRAPH ,(*default-graph*)
          (?graph a rewriter:Graph)
          (?rule rewriter:predicate ,p)
          (?rule rewriter:subjectType ,stype)
          ,(if (*realm*)
               `(UNION ((?rule rewriter:graphType ?type)
                        (?graph rewriter:type ?type)
                        (?graph rewriter:realm ,(*realm*)))
                       ((?rule rewriter:graph ?graph)))
                `(?rule rewriter:graph ?graph)))))

(define (get-graph stype p)
  (parameterize ((*namespaces* (append (*namespaces*) (query-namespaces))))
    (car-when
     (hit-hashed-cache
      *cache* (list stype p (*realm*))
      (query-with-vars 
       (graph)
       (s-select 
        '?graph
        (s-triples (get-graph-query stype p))
        from-graph: #f)
       graph)))))

(define (get-graph stype p) '<graph>)

(define (graph-match-statements graph s stype p new-stype?)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
    `(,@(splice-when
	 (and (not (iri? stype))
	      new-stype?
	      `((GRAPH ?AllGraphs (,s a ,stype))
		(GRAPH ,(*default-graph*) (?AllGraps a rewriter:Graph)))))
      (GRAPH ,(*default-graph*) 
	     (,rule a rewriter:GraphRule)
	     (,graph a rewriter:Graph)
	     ,(if (*realm*)
		  `(UNION ((,rule rewriter:graph ,graph))
			  ((,rule rewriter:graphType ,gtype)
			   (,graph rewriter:type ,gtype)
			   (,graph rewriter:realm ,(*realm*))))
		  `(,rule rewriter:graph ,graph))
	     ,(if (equal? p 'a)
		  `(,rule rewriter:predicate rdf:type)
		  `(,rule rewriter:predicate ,p))
	     (,rule rewriter:subjectType ,stype)))))

(define (get-type subject)
  (hit-hashed-cache
   *cache* (list 'Type subject)
   (query-unique-with-vars
    (type)
    (s-select '?type
              (s-triples
               `((GRAPH ,(*default-graph*) (?graph a rewriter:Graph))
                 (GRAPH ?graph (,subject a ?type))))
              from-graph: #f)
    type)))

(define (get-type s)
  '<TT>)

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
				 (let ((stype (or
					       ;; (expand-namespace s (query-namespaces))))
					       (and (iri? s) (get-type s))
					       (new-sparql-variable "stype"))))
				   ;;(or (and (iri? s) (get-type (expand-namespace s (query-namespaces))))
				   (loop (cdr ts)
					 (update-binding
					  (list s) 'stype stype bindings)))))))))))
    ((GRAPH) . ,(lambda (block bindings rules)
		  (if (*rewrite-graph-statements?*)
		      (let-values (((rw c b) (rewrite (cddr block) bindings expand-triples-rules)))
			(values rw '() bindings))
		      (values (list block) '() bindings))))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)))

(define rewrite-triples-rules
  `((,triple? . ,(lambda (triple bindings rules)
		   (match triple
		     ;; actually maybe not a good idea...
		     ;; what if we want to restrict this statement as well?
		     ((s 'a t)
		      (values `((*REWRITTEN* (GRAPH ?allGraphs ,triple))) '() bindings))
		     ((s p o)
		      (let* ((stype (get-binding (list s) 'stype bindings))
			     (bound-graph (get-binding (list s p) 'graph bindings))
			     (solved-graph (and (iri? stype) (iri? p) (get-graph stype p)))
			     (graph (or bound-graph solved-graph
					(new-sparql-variable "graph"))))
		
			(if solved-graph
			    (values `((*REWRITTEN* (GRAPH ,graph ,triple)))
				    '() bindings)
			    (values `((*REWRITTEN* (GRAPH ,graph ,triple)))
				    (if bound-graph '()
					(graph-match-statements graph s stype p bound-graph)) ;; **
				    (update-binding (list s p) 'graph graph bindings))))))))
    ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)))

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

(define (graph-rewriter-rules)
 `(((@Unit) . ,rw/continue)
   ((@Prologue) . ,(lambda (block bindings rules)
		     (values `((@Prologue
			       (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
			       ,@(cdr block)))
			     '()
			     bindings)))
   ((@Query) . ,rw/continue) ;; *rewrite-select-queries?*
   ((@Update) . ,(lambda (block bindings rules)
		   (let-values (((rw graph-statements _)
				 (rewrite (reverse (cdr block)) bindings rules)))
		     (let ((where-block (alist-ref 'WHERE rw)))
		       (values `((@Update . ,(alist-update
					      'WHERE (append
						      `((GRAPH ,(*default-graph*)
							       (?allGraphs a rewriter:Graph)))
						      graph-statements where-block)
					      (reverse rw))))
			       '() '())))))
   (,select? . ,rw/copy) ;;  '* => (extract-all-variables)
   ((@Dataset) . ,rw/copy)
   ((@Using) . ,rw/copy)
   ((GRAPH) . ,rw/copy)
   (,subselect? . ,(lambda (block bindings rules)
		     (match block
		       ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) quads)
			(let* ((subselect-vars (extract-subselect-vars vars))
			       (bindings (if (or (equal? subselect-vars '(*))
						 (equal? subselect-vars '*))
					     bindings
					     (project-bindings subselect-vars bindings))))
			  (let-values (((rw c b) (rewrite quads bindings rules)))
			    (values rw c (merge-bindings b bindings))))))))
					     
   (,quads-block? . ,(lambda (block bindings rules)
		       (let-values (((qds1 _ b1)
				     (rewrite (cdr block) bindings expand-triples-rules)))
			 (let-values (((qds2 c2 b2)
				     (rewrite qds1 b1 rewrite-triples-rules)))
			   (let-values (((qds3 c3 b3)
					 (rewrite qds2 b2 rules)))
			     (values `((,(car block) ,@qds3))
				     (append c2 c3)
				     b3))))))
   ((*REWRITTEN*) . ,(lambda (block bindings rules)
		       (values (cdr block) '() bindings)))
   (,pair? . ,(lambda (block bindings rules)
		(with-rewrite-values ((rw (rewrite block bindings rules)))
				     (list rw))))))

(default-rules graph-rewriter-rules)
	       
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; deltas

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; endpoints
