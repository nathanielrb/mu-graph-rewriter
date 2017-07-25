(use s-sparql s-sparql-parser mu-chicken-support
     matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client)

(require-extension sort-combinators)

(define *subscribers-file*
  (config-param "SUBSCRIBERSFILE" "subscribers.json"))

(define *subscribers*
  (handle-exceptions exn '()
    (vector->list
     (alist-ref 'potentials
                (with-input-from-file (*subscribers-file*)
                  (lambda () (read-json)))))))
  
(define *plugin*
  (config-param "PLUGIN_PATH" #f))

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

(define (expand-expanded-triple s p o)
  (cond  ((blank-node-path? o)
          (expand-blank-node-path (list s p o)))
         ((sequence-path? p)
          (expand-sequence-path-triple (list s p o)))
         (else
          (list (list s p o)))))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Nested association lists
(define (nested-alist-ref* keys alist)
  (if (null? keys)
      alist
      (let ((nested (alist-ref (car keys) alist)))
	(and nested
	     (nested-alist-ref* (cdr keys) nested)))))

(define-syntax nested-alist-ref
  (syntax-rules ()
    ((_ key ... alist) 
     (nested-alist-ref* (list key ...) alist))))

(define (nested-alist-update* keys val alist)
  (let ((key (car keys)))
    (if (null? (cdr keys))
        (alist-update key val alist)
        (alist-update 
         key
         (nested-alist-update*
          (cdr keys) val (or (alist-ref key alist) '()))
          alist))))

(define-syntax nested-alist-update
  (syntax-rules ()
    ((_ key ... val alist) 
     (nested-alist-update* (list key ...) val alist))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Context
(define (context-here context)
  (first context))

(define (context-previous context)
  (secound context))

(define (context-next context)
  (third context))

(define (context-parent context)
  (fourth context))

(define (parent-axis proc)
  (lambda (context)
    (call/cc
     (lambda (out)
       (let loop ((context context))
         (if (null? context) #f
             (let ((try (proc context)))
               (if try (out #t)
                   (loop (context-parent context))))))))))

(define (has-ancestor? node)
  (parent-axis
   (lambda (context)
     (equal? node (car (context-here context))))))
     
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Sparql transformations
(define default-rules (make-parameter (lambda () '())))

(define (nested-alist-replace* keys proc alist)
  (nested-alist-update* keys (proc (nested-alist-ref* keys alist)) alist))

(define (get-binding* vars key bindings)
  (nested-alist-ref* (append vars (list '@bindings key)) bindings))

(define (get-binding/default* vars key bindings default)
  (or (nested-alist-ref* (append vars (list '@bindings key)) bindings)) default)

(define-syntax get-binding
  (syntax-rules ()
    ((_ var ... key bindings)
     (get-binding* (list var ...) key bindings))))

(define-syntax get-binding/default
  (syntax-rules ()
    ((_ var ... key bindings default)
     (get-binding/default* (list var ...) key bindings default))))

(define (update-binding* vars key val bindings)
  (nested-alist-update* (append vars (list '@bindings key)) val bindings))

(define-syntax update-binding
  (syntax-rules ()
    ((_ var ... key val bindings)
     (update-binding* (list var ...) key val bindings))))

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

(define (merge-bindings new-bindings bindings)
  (if (and (pair? new-bindings) (pair? bindings))
      (let loop ((new-bindings new-bindings) (merged-bindings bindings))
        (if (null? new-bindings) merged-bindings
            (let* ((new-binding (car new-bindings)))
              (loop (cdr new-bindings)
                    (alist-update (car new-binding)
                                  (merge-bindings (cdr new-binding)
                                                  (or (alist-ref (car new-binding) bindings) '()))
                                  merged-bindings)))))
      new-bindings))

(define query-namespaces (make-parameter (*namespaces*)))

(define (remove-trailing-char sym #!optional (len 1))
  (let ((s (symbol->string sym)))
    (string->symbol
     (substring s 0 (- (string-length s) len)))))

(define (PrefixDecl? decl) (equal? (car decl) 'PREFIX))

(define (BaseDecl? decl) (equal? (car decl) 'BASE))

(define (graph? quad)
  (and (list? quad)
       (equal? (car quad) 'GRAPH)))

(define (query-prefixes QueryUnit)
  (map (lambda (decl)
         (list (remove-trailing-char (cadr decl)) (write-uri (caddr decl))))
       (filter PrefixDecl? (unit-prologue QueryUnit))))

(define (rewrite-query Query #!optional (rules ((default-rules))))
  (parameterize ((query-namespaces (query-prefixes Query)))
    (rewrite Query rules)))

(define (rewrite blocks #!optional (rules ((default-rules))) (bindings '()) (context '()))
  (let loop ((blocks blocks) (statements '()) (bindings bindings) (left-blocks '()))
    (if (null? blocks)
	(values statements bindings)
	(let-values (((new-statements updated-bindings)
		      (apply-rules (car blocks) rules bindings 
                                   (list (car blocks) left-blocks blocks context))))
	  (loop (cdr blocks)
		(append statements new-statements)
		updated-bindings
                (cons (car blocks) left-blocks))))))

(define (apply-rules block rules bindings context)
  (let ((rule-match? (lambda (rule)
		      (or (and (symbol? rule) (equal? rule block))
			  (and (pair? rule) (member (car block) rule))
			  (and (procedure? rule) (rule block))))))
    (let loop ((remaining-rules rules))
      (if (null? remaining-rules) (abort (format #f "No matching rule for ~A" block))
	  (match (car remaining-rules)
	    ((rule . proc) 
	     (if (rule-match? rule)
		 (proc block rules bindings context)
		 (loop (cdr remaining-rules)))))))))

(define-syntax with-rewrite
  (syntax-rules ()
    ((_ ((var expr)) body)
     (let-values (((var updated-bindings) expr))
       (values body updated-bindings)))))

;; macro for when neither the bindings nor the context matter
;; limited support for second-passes using the same rules and context,
;; but it'd be nice if this playing-nice with with-rewrite could be more general.
;; (rw/lambda (block)
;;  (with-rewrite ((rw (rewrite block)))
;;    body)) 
;; =>
;; (lambda (block rules bindings context)
;;  (let-values (((rw new-bindings) (rewrite exp rules bindings context)))
;;    (values body new-bindings))))
(define-syntax rw/lambda
  (syntax-rules (with-rewrite rewrite)
    ((_ (var) (with-rewrite ((rw (rewrite exp))) body))
     (lambda (var rules bindings context)
       (let-values (((rw new-bindings) (rewrite exp rules bindings context)))
         (values body new-bindings))))
    ((_ (var) body)
     (lambda (var rules bindings context)
       (values body bindings)))))

(define (rw/continue block rules bindings context)
  (with-rewrite ((new-statements (rewrite (cdr block) rules bindings context)))
    `((,(car block) ,@new-statements))))

(define (rw/copy block rules bindings context)
  (values (list block) bindings))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deltas
(define (update-query? query)
  ((list-of? update-unit?)
   (alist-ref '@Unit query)))

(define (update-unit? unit)
  (alist-ref '@Update unit))

;; This only matches single quad statements like GRAPH ?g { ?a ?b ?c } 
;; It should be extended to triples, and quads with 2+ triples and paths
;; GRAPH ?g { ?a ?b ?c . ?s ?p ?o, ?v } etc.
(define (construct-statements statements)
  (and statements
       `(CONSTRUCT
         ,@(join
            (map (lambda (statement)
                   (match statement
                     ((`GRAPH graph (s p o))
                      `((,s ,p ,o)
                        (,s ,p ,graph)
                        (rewriter:Graphs rewriter:include ,graph)))))
                 statements)))))

;; better to do this with rewrite rules?
(define (query-constructs query)
  (and (update-query? query)
       (let loop ((queryunits (alist-ref '@Unit query))
                  (prologues '(@Prologue)) (constructs '()))
         (if (null? queryunits) constructs
             (let* ((queryunit (car queryunits))
                    (unit (alist-ref '@Update queryunit))
                    (prologue (append prologues (or (alist-ref '@Prologue queryunit) '())))
                    (where (assoc 'WHERE unit))
                    (delete-construct (construct-statements (alist-ref 'DELETE unit)))
                    (insert-construct (construct-statements (alist-ref 'INSERT unit)))
                    (S (lambda (c) (and c `(,prologue ,c ,(or where '(NOWHERE)))))))
               (loop (cdr queryunits) prologue
                     (cons (list (S delete-construct) (S insert-construct))
                           constructs)))))))

(define (merge-triples-by-graph label quads)
  (let ((car< (lambda (a b) (string< (car a) (car b)))))
    (map (lambda (group)
           `(,(string->symbol (caar group))
             . ((,label 
                 . ,(list->vector
                     (map cdr group))))))
         (group/key car (sort quads car<)))))
                   
(define (run-delta-expand-properties s propertyset graphs)
  (let* ((p (car propertyset))
         (objects (vector->list (cdr propertyset)))
         (G? (lambda (object) (member (alist-ref 'value object) graphs)))
         (graph (alist-ref 'value (car (filter G? objects)))))
    (filter values
            (map (lambda (object)
                   (let ((o (alist-ref 'value object)))
                     (and (not (member o graphs))
                          `(,graph
                            . ((s . ,(symbol->string s))
                               (p . ,(symbol->string p))
                               (o . ,o))))))
                 objects))))

(define (run-delta query label)
  (let* ((gkeys '(http://mu.semte.ch/graphs/Graphs http://mu.semte.ch/graphs/include))
         (results (sparql/select (write-sparql query) raw?: #t))
         (graphs (map (cut alist-ref 'value <>)
                      (vector->list
                       (or (nested-alist-ref* gkeys results) (vector)))))
         (D (lambda (tripleset)
              (let ((s (car tripleset))
                    (properties (cdr tripleset)))
                (and (not (equal? s (car gkeys)))
                     (join (map (cut run-delta-expand-properties s <> graphs) properties)))))))
    (merge-triples-by-graph label (join (filter values (map D results))))))

(define (merge-deltas-by-graph delete-deltas insert-deltas)
  (list->vector
   (map (match-lambda
          ((graph . deltas)
          `((graph . ,(symbol->string graph))
            (deltas . ,deltas))))
       (let loop ((ds delete-deltas) (diffs insert-deltas))
         (if (null? ds) diffs
             (let ((graph (caar ds)))
               (loop (cdr ds)
                     (alist-update 
                      graph (append (or (alist-ref graph diffs) '()) (cdar ds))
                      diffs))))))))

(define (run-deltas query)
  (let ((constructs (query-constructs query))
        (R (lambda (c l) (or (and c (run-delta c l)) '()))))
    (map (match-lambda
           ((d i) 
            (merge-deltas-by-graph (R d 'deletes) (R i 'inserts))))
         constructs)))

(define (notify-subscriber subscriber deltastr)
  (let-values (((result uri response)
                (with-input-from-request 
                 (make-request method: 'POST
                               uri: (uri-reference subscriber)
                               headers: (headers '((content-type application/json))))
                 deltastr
                 read-string)))
    (close-connection! uri)
    result))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implentation
(define (virtuoso-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (when body
      (log-message "~%==Virtuoso Error==~% ~A ~%" body))
    (when response
      (log-message "~%==Reason==:~%~A~%" (response-reason response)))
    (abort exn)))

;; better : parameterize body and req-headers, and define functions...
(define $query (make-parameter (lambda (q) #f)))
(define $body (make-parameter (lambda (q) #f)))
(define $headers (make-parameter (lambda (h) #f)))
(define $mu-session-id (make-parameter #f))
(define $mu-call-id (make-parameter #f))

(define (rewrite-call _)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (query (parse-query query-string))
         (req-headers (request-headers (current-request)))
         (mu-session-id (header-value 'mu-session-id req-headers)))
    
    (log-message "~%==Received Headers==~%~A~%" req-headers)
    (log-message "~%==Rewriting Query==~%~A~%" query-string)

    (let ((rewritten-query (parameterize (($query $$query)
                                          ($body $$body)
                                          ($headers (lambda (h)
                                                      (header-value h req-headers)))
                                          ($mu-session-id (header-value 'mu-session-id req-headers))
                                          ($mu-call-id (header-value 'mu-call-id req-headers)))
                             (rewrite-query query))))

      (log-message "~%==Parsed As==~%~A~%" (write-sparql query))
      (log-message "~%==Rewritten Query==~%~A~%" (write-sparql rewritten-query))

      (handle-exceptions exn 
          (virtuoso-error exn)
        
        (when (update-query? rewritten-query)
           (let ((queries-deltas (run-deltas rewritten-query)))
             (for-each (lambda (query-deltas)
                         (let ((deltastr (json->string query-deltas)))
                           (format (current-error-port) "~%==Deltas==~%~A" deltastr)
                           (for-each (lambda (subscriber)
                                       (notify-subscriber subscriber deltastr))
                                     *subscribers*)))
                       queries-deltas)))

        ;; (parameterize ((tcp-read-timeout #f) (tcp-write-timeout #f) (tcp-connect-timeout #f))
        (let-values (((result uri response)
                      (with-input-from-request 
                       (make-request method: 'POST
                                     uri: (uri-reference (*sparql-endpoint*))
                                     headers: (headers
                                               '((Content-Type application/x-www-form-urlencoded)
                                                 (Accept application/sparql-results+json)))) 
                       ;;(append (headers->list (request-headers (current-request)))
                       `((query . , (format #f "~A" (write-sparql rewritten-query))))
                       read-string)))
          (close-connection! uri)
          (let ((headers (headers->list (response-headers response))))
            
            (log-message "~%==Results==~%~A~%" 
                         (substring result 0 (min 1000 (string-length result))))
            (mu-headers headers)
            (format #f "~A~" result)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification

(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

(when (*plugin*) (load (*plugin*)))

(*port* 8890)





















