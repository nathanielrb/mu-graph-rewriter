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

(define *cache* (make-hash-table))

(define *session-realm-ids* (make-hash-table))

(define (get-realm realm-id)
  (and realm-id
       (query-unique-with-vars
        (realm)
        (s-select '?realm (s-triples `((?realm mu:uuid ,realm-id)))
                  from-graph: (*realm-id-graph*))
        realm)))

(define (add-realm realm graph graph-type)
  (sparql/update
   (s-insert
    (s-triples
     `((,graph rewriter:realm ,realm)
       (,graph a rewriter:Graph)
       (,graph rewriter:type ,graph-type))))))

(define (delete-realm realm graph)
  (sparql/update
   (if graph
       (s-delete
        (s-triples `((,graph ?p ?o)))
        where: (s-triples `((,graph ?p ?o))))
       (s-delete
        (s-triples `((?graph ?p ?o)))
        where: (s-triples `((?graph rewriter:realm ,realm)
                            (?graph ?p ?o)))))))
         
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

;; (define (get-graph stype p) '<graph>)

(define (graph-match-statements graph s stype p new-stype?)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
    `(,@(splice-when
	 (and (not (iri? stype))
              new-stype?
	      `((GRAPH ?AllGraphs (,s a ,stype)) ;; only once!!
                )))
		;;(GRAPH ,(*default-graph*) (?AllGraphs a rewriter:Graph)))))
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

;;(define (get-type s)  '<TT>)

(define (all-graphs)
  (hit-hashed-cache
   *cache* (list 'graphs (*realm*))
   (query-with-vars 
    (graph)
    (s-select 
     '?graph
     (s-triples
      `((GRAPH
         ,(*default-graph*)
         (?graph a rewriter:Graph)
         ,@(splice-when
            (and (*realm*)
                 `((UNION ((?rule rewriter:graphType ?type)
                           (?graph rewriter:type ?type)
                           (?graph rewriter:realm ,(*realm*)))
                          ((?rule rewriter:graph ?graph)))))))))
     from-graph: #f)
     graph)))

(define (replace-dataset where-clause label)
  (let ((graphs ;; (append (if (*rewrite-graph-statements?*)
                ;;        '()
                ;;        (extract-graphs where-clause))
         (all-graphs))
        (type  (case label ((FROM) '@Dataset) ((USING) '@Using)))
        (label-named (case label
                       ((FROM) '|FROM NAMED|)
                       ((USING) '|USING NAMED|))))
    `((,type
       (,label ,(*default-graph*))
       ,@(map (lambda (graph) 
                `(,label ,graph))
              graphs)
       (,label-named ,(*default-graph*))
       ,@(map (lambda (graph) 
                `(,label-named ,graph))
              graphs)))))

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

(define (rewrite-block-name part-name)
  (case part-name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else part-name)))

;; use letrec 
(define (expand-triples-rules rewrite-graph-statements?)
  `((,triple? . ,(lambda (block rules bindings context)
		  (let ((triples (expand-triple block)))
		    (let loop ((ts triples) (bindings bindings))
		      (if (null? ts)
			  (values triples bindings)
			  (match (car ts)
			    ((s `a o)
			     (loop (cdr ts)
				   (update-binding* (list s) 'stype o bindings)))
			    ((s p o)
			     (if (get-binding* (list s) 'stype bindings)
				 (loop (cdr ts) bindings)
				 (let ((stype (or
					       ;; (expand-namespace s (query-namespaces))))
					       (and (iri? s) (get-type s))
					       (new-sparql-variable "stype"))))
				   (loop (cdr ts)
					 (update-binding*
					  (list s) 'stype stype
                                          (update-binding* (list s) 'new-stype? #t bindings))))))))))))
    ((GRAPH) . ,(lambda (block rules bindings context)
                  (print "rewrite this graph???")
                  (print rewrite-graph-statements?)(print block)(newline)
		  (if rewrite-graph-statements?
		      (let-values (((rw b) (rewrite (cddr block) (expand-triples-rules rewrite-graph-statements?) bindings context)))
			(values rw b))
		      (values (list block) bindings))))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)))

(define rewrite-triples-rules
  `((,triple? . ,(lambda (triple rules bindings context)
		   (match triple
		     ;; is this a good idea?...
		     ;; what if we want to restrict this statement as well?
		     ((s 'a t)
		      (values `((*REWRITTEN* (GRAPH ?AllGraphs ,triple))) bindings))
		     ((s p o)
		      (let* ((stype (get-binding* (list s) 'stype bindings))
                             (new-stype? (get-binding* (list s) 'new-stype? bindings))
			     (bound-graph (get-binding* (list s p) 'graph bindings))
			     (solved-graph (and (iri? stype) (iri? p) (get-graph stype p)))
			     (graph (or bound-graph solved-graph
					(new-sparql-variable "graph")))
                             (gmatch (and (not bound-graph)
                                          ;; new-type? not accurate...
                                          (graph-match-statements graph s stype p new-stype?)))
                             (in-place? (alist-ref 'in-place? context)))
			(if solved-graph
			    (values `((*REWRITTEN* (GRAPH ,graph ,triple)))
				    bindings)
			    (values `((*REWRITTEN* 
                                       ,@(splice-when (and in-place?
                                                           gmatch)))
                                      (GRAPH ,graph ,triple))
                                    (update-binding* (list s) 'new-stype? #f
                                                     (update-binding* (list s p) 'graph graph 
                                                                      (update-binding* '() 'graph-statements
                                                                                       (append 
                                                                                        (or (get-binding* '() 'graph-statements bindings) '())
                                                                                        (if in-place? '()
                                                                                            (or gmatch '())))
                                                                                       bindings))))))))))
    ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)))

(define flatten-graph-rules
  `((,triple? . ,rw/copy)
;    ((WHERE OPTIONAL UNION MINUS) . ,(lambda (block rules bindings context)
 ;                                    (with-rewrite-values ((new-statements
  ;                                                          (rewrite (cdr block) rules bindings context)))
   ;                                    `((,(car block) ,@new-statements)))))

    ((GRAPH) . ,(lambda (block rules bindings context)
                  (values (cddr block) bindings)))
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
    ((@Dataset) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)
    (,pair? . ,rw/continue)))

(define (rewrite-subselect block rules bindings context)
  (match block
    ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . quads)
     (let* ((subselect-vars (extract-subselect-vars vars))
            (bindings (if (or (equal? subselect-vars '(*))
                              (equal? subselect-vars '*))
                          bindings
                          (project-bindings subselect-vars bindings))))
       (let-values (((rw b) (rewrite quads rules bindings context)))
         (values (list (cons (car block) rw)) (merge-bindings b bindings)))))))

(define (graph-rewriter-rules)
  (print "session id " ($mu-session-id))

  (let* (;;(mu-session-id (($headers) 'mu-session-id))
         (graph-realm (or (($headers) 'mu-graph-realm)
                          (get-realm (($headers) 'mu-graph-realm-id))
                          (get-realm (($query) 'graph-realm-id))
                          (($query) 'graph-realm)
                          (($body) 'graph-realm)
                          (get-realm (hash-table-ref/default *session-realm-ids* ($mu-session-id) #f)))))
    
    (log-message "~%==Graph Realm==~%~A~%" graph-realm)

(let ((rewrite-graph-statements?
                    (not (or (($headers) 'preserve-graph-statements)
                             (($query) 'preserve-graph-statements)))))
    (parameterize ((*realm* graph-realm)
                   
                   (*rewrite-select-queries?* 
                    (or (equal? "true" (($headers)'rewrite-select-queries))
                        (equal? "true" (($query) 'rewrite-select-queries))
                        (*rewrite-select-queries?*))))
      (print "REWRITE??? " rewrite-graph-statements?)
      `(((@Unit) . ,rw/continue)
        ((@Prologue) . ,(lambda (block rules bindings context)
                          (values `((@Prologue
                                     (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
                                     ,@(cdr block)))
                                  bindings)))
        ((@Query) . ,(lambda (block rules bindings context)
                       (format (current-error-port) "rewrite select??? ~A "  (*rewrite-select-queries?*))
                       (if (*rewrite-select-queries?*)
                           (with-rewrite-values ((new-statements (rewrite (cdr block) rules bindings context)))
                                                `((,(car block) ,new-statements)))
                           (values (list
                                    (cons '@Query
                                          (alist-update
                                           '@Dataset 
                                           (replace-dataset '() 'FROM)
                                           (let-values (((bl _) (rewrite (cdr block) flatten-graph-rules))) bl))))
                                   bindings))))

        ((@Update) . ,(lambda (block rules bindings context)
                        (let-values (((rw nbs)
                                      (rewrite (reverse (cdr block)) rules bindings context)))
                          (let ((where-block (alist-ref 'WHERE rw))
                                (graph-statements (or (get-binding* '() 'graph-statements nbs) '())))
                            (values `((@Update . ,(alist-update
                                                   'WHERE (append
                                                           `((GRAPH ,(*default-graph*)
                                                                    (?AllGraphs a rewriter:Graph)))
                                                           graph-statements where-block)
                                                   (reverse rw))))
                                    '())))))
        ((@Dataset) . ,rw/copy)
        ((@Using) . ,rw/copy)
        ((GRAPH) . ,rw/copy)
        (,select? . ,rw/copy) ;;  '* => (extract-all-variables)
        (,subselect? . ,rewrite-subselect)
        (,(lambda (block)
            (and (equal? (car block) 'WHERE)
                 (subselect? (cdr block)))) . ,(lambda (block rules bindings context)
                                                 (let-values (((rw b)
                                                               (rewrite-subselect (cdr block) rules bindings context)))
                                                   (values `((WHERE ,@rw))
                                                           (merge-bindings b bindings)))))


        (,quads-block? . ,(lambda (block rules bindings context)
                            (let ((new-context (if (equal? (car block) 'WHERE)
                                                   (alist-update 'in-place? #t context)
                                                   context)))
                              (let-values (((bl1 b1)
                                            (rewrite (cdr block) (expand-triples-rules rewrite-graph-statements?) bindings new-context)))
                                (let-values (((bl2 b2)
                                              (rewrite bl1 rewrite-triples-rules b1 new-context)))
                                  (let-values (((bl3 b3)
                                                (rewrite bl2 rules b2 new-context)))
                                    (values `((,(rewrite-block-name (car block)) ,@bl3))
                                            b3)))))))
        ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
        ((*REWRITTEN*) . ,(lambda (block rules bindings context)
                            (values (cdr block) bindings)))
        (,pair? . ,(lambda (block rules bindings context)
                     (with-rewrite-values ((rw (rewrite block rules bindings context)))
                                          (list rw)))))))))

(default-rules graph-rewriter-rules)



(define change-session-realm-call 
  (rest-call
   (realm-id)
   (let ((mu-session-id (header-value 'mu-session-id (request-headers (current-request)))))
     (log-message "~%Changing graph-realm-id for mu-session-id ~A to ~A~%"
             mu-session-id realm-id)
     (hash-table-set! *session-realm-ids* mu-session-id realm-id)
     `((mu-session-id . ,mu-session-id)
       (realm-id . ,realm-id)))))

(define (delete-session-realm-call _)
  (let ((mu-session-id (header-value 'mu-session-id (request-headers (current-request)))))
    (log-message "~%Removing graph-realm-id for mu-session-id ~A to ~A~%"
            mu-session-id realm-id)
    (hash-table-delete! *session-realm-ids* mu-session-id)
     `((mu-session-id . ,mu-session-id)
       (realm-id . #f))))
                     
(define (add-realm-call _)
  (let* ((req-headers (request-headers (current-request)))
         (body (read-request-json))
         (realm (or (read-uri (alist-ref 'graph-realm body))
                    (get-realm (alist-ref 'graph-realm-id body))))
         (graph-type (read-uri (alist-ref 'graph-type body)))
         (graph (read-uri (alist-ref 'graph body))))
    (log-message "~%Adding graph-realm ~A for ~A  ~%"
            realm graph)
    (add-realm realm graph graph-type)
    (hash-table-delete! *cache* '(graphs #f))
    `((status . "success")
      (realm . ,(write-uri realm)))))

(define (delete-realm-call _)
  (let* ((req-headers (request-headers (current-request)))
         (body (read-request-json))
         (realm (or (read-uri (alist-ref 'graph-realm body))
                    (get-realm (alist-ref 'graph-realm-id body))))
         (graph (read-uri (alist-ref 'graph body))))
    (log-message "~%Deleting graph-realm for ~A or ~A  ~%"
            realm graph)
    (hash-table-delete! *cache* '(graphs #f))
    (delete-realm realm graph)))

(define-rest-call 'PATCH '("session" "realm" realm-id) change-session-realm-call)

(define-rest-call 'DELETE '("session" "realm") delete-session-realm-call)

(define-rest-call 'POST '("realm") add-realm-call)
(define-rest-call 'POST '("realm" realm-id) add-realm-call)

(define-rest-call 'DELETE '("realm") delete-realm-call)
(define-rest-call 'DELETE '("realm" realm-id) delete-realm-call)

	       
