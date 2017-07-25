;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Graph Rewriter

(define-namespace mu "http://mu.semte.ch/vocabularies/core/") 
(define-namespace rewriter "http://mu.semte.ch/graphs/")

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
        (s-select '?realm (write-triples `((?realm mu:uuid ,realm-id)))
                  from-graph: (*realm-id-graph*))
        realm)))

(define (add-realm realm graph graph-type)
  (sparql/update
   (s-insert
    (write-triples
     `((,graph rewriter:realm ,realm)
       (,graph a rewriter:Graph)
       (,graph rewriter:type ,graph-type))))))

(define (delete-realm realm graph)
  (sparql/update
   (if graph
       (s-delete
        (write-triples `((,graph ?p ?o)))
        where: (write-triples `((,graph ?p ?o))))
       (s-delete
        (write-triples `((?graph ?p ?o)))
        where: (write-triples `((?graph rewriter:realm ,realm)
                            (?graph ?p ?o)))))))
         
(define (get-graph-query realm stype p)
  `((GRAPH ,(*default-graph*)
           (?graph a rewriter:Graph)
           (?rule rewriter:predicate ,p)
           (?rule rewriter:subjectType ,stype)
           ,(if realm
                `(UNION ((?rule rewriter:graphType ?type)
                         (?graph rewriter:type ?type)
                         (?graph rewriter:realm ,realm))
                        ((?rule rewriter:graph ?graph)))
                `(?rule rewriter:graph ?graph)))))

(define (get-graph realm stype p)
  (parameterize ((*namespaces* (append (*namespaces*) (query-namespaces))))
    (car-when
     (hit-hashed-cache
      *cache* (list stype p realm)
      (query-with-vars 
       (graph)
       (s-select '?graph (write-triples (get-graph-query realm stype p))
        from-graph: #f)
       graph)))))

;; (define (get-graph stype p) '<graph>)

(define (get-type subject)
  (hit-hashed-cache
   *cache* (list 'Type subject)
   (query-unique-with-vars
    (type)
    (s-select '?type
              (write-triples
               `((GRAPH ,(*default-graph*) (?graph a rewriter:Graph))
                 (GRAPH ?graph (,subject a ?type))))
              from-graph: #f)
    type)))

;;(define (get-type s)  '<TT>)

(define (all-graphs realm)
  (hit-hashed-cache
   *cache* (list 'graphs realm)
   (query-with-vars 
    (graph)
    (s-select 
     '?graph
     (write-triples
      `((GRAPH
         ,(*default-graph*)
         (?graph a rewriter:Graph)
         ,@(splice-when
            (and realm
                 `((UNION ((?rule rewriter:graphType ?type)
                           (?graph rewriter:type ?type)
                           (?graph rewriter:realm ,realm))
                          ((?rule rewriter:graph ?graph)))))))))
     from-graph: #f)
     graph)))

(define (graph-match-statements realm graph s stype p new-stype?)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
    `(,@(splice-when
	 (and (not (iri? stype))
              new-stype?
              ;; and what about types in un-rewritten named-graph? 
	      `((GRAPH ?AllGraphs (,s a ,stype)) ;; only once!!
                )))
		;;(GRAPH ,(*default-graph*) (?AllGraphs a rewriter:Graph)))))
      (GRAPH ,(*default-graph*) 
	     (,rule a rewriter:GraphRule)
	     (,graph a rewriter:Graph)
	     ,(if realm
		  `(UNION ((,rule rewriter:graph ,graph))
			  ((,rule rewriter:graphType ,gtype)
			   (,graph rewriter:type ,gtype)
			   (,graph rewriter:realm ,realm)))
		  `(,rule rewriter:graph ,graph))
	     ,(if (equal? p 'a)
		  `(,rule rewriter:predicate rdf:type)
		  `(,rule rewriter:predicate ,p))
	     (,rule rewriter:subjectType ,stype)))))

(define (replace-dataset realm label #!optional named? (extra-graphs '()))
  (let ((graphs (append (all-graphs realm) extra-graphs))
        (type  (case label ((FROM) '@Dataset) ((USING) '@Using)))
        (label-named (case label
                       ((FROM) '|FROM NAMED|)
                       ((USING) '|USING NAMED|))))
    `((,type
       (,label ,(*default-graph*))
       ,@(map (lambda (graph) 
                `(,label ,graph))
              graphs)
       ,@(splice-when
          (and named?
               `((,label-named ,(*default-graph*))
                 ,@(map (lambda (graph) 
                          `(,label-named ,graph))
                        graphs))))))))

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

(define (expand-triple-rule block rules bindings)
  (let ((triples (expand-triple block)))
    (let loop ((ts triples) (bindings bindings))
      (if (null? ts)
          (values triples bindings)
          (match (car ts)
            ((s `a o)
             (loop (cdr ts)
                   (update-binding*
                    (list s) 'stype o 
                    (update-binding* (list s) 'new-stype? #f bindings))))
            ((s p o)
             (if (get-binding* (list s) 'stype bindings)
                 (loop (cdr ts) bindings)
                 (let ((stype (or
                               (and (iri? s) (get-type (expand-namespace s (query-namespaces))))
                               (new-sparql-variable "stype"))))
                   (loop (cdr ts)
                         (update-binding*
                          (list s) 'stype stype
                          (update-binding* (list s) 'new-stype? #t bindings)))))))))))

(define (%expand-graph-rule rewrite-graph-statements?)
  (lambda (block rules bindings)
    (let-values (((rw b) (rewrite (cddr block)
                                  (expand-triples-rules rewrite-graph-statements?)
                                  bindings)))
      (if rewrite-graph-statements?
          (values rw b)
          (values (list block) 
                  (update-binding* '() 'named-graphs 
                                   (cons (second block)
                                         (get-binding/default* '() 'named-graphs b '()))
                                   b))))))

(define (expand-triples-rules rewrite-graph-statements?)
  `((,triple? . ,expand-triple-rule)
    ((GRAPH) . ,(%expand-graph-rule rewrite-graph-statements?))
    ((FILTER BIND MINUS OPTIONAL UNION) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)))

(define (%rewrite-triple-rule realm)
  (lambda (triple rules bindings)
    (let ((in-where? ((parent-axis (lambda (context) (equal? (context-here context) 'WHERE))) (*context*))))
      (match triple
        ((s p o)
         (if (and (equal? p 'a) in-where?)
           (values `((*REWRITTEN* (GRAPH ?AllGraphs ,triple))) bindings)
           (let* ((stype (get-binding* (list s) 'stype bindings))
                  (new-stype? (get-binding* (list s) 'new-stype? bindings))
                  (bound-graph (get-binding* (list s p) 'graph bindings))
                  (solved-graph (and (iri? stype) (iri? p) (get-graph realm stype p)))
                  (graph (or bound-graph solved-graph
                             (new-sparql-variable "graph")))
                  ;; (in-place? (alist-ref 'in-place?))
                  (gmatch (and (not bound-graph)
                               (graph-match-statements realm graph s stype p (and new-stype? in-where?)))))
             (if solved-graph
                 (values `((*REWRITTEN* (GRAPH ,graph ,triple)))
                         bindings)
                 (values `((*REWRITTEN* 
                            ,@(splice-when (and in-where?
                                                gmatch)))
                           (GRAPH ,graph ,triple))
                         (update-binding* (list s) 'new-stype? #f
                                          (update-binding*
                                           (list s p) 'graph graph 
                                           (update-binding*
                                            '() 'graph-statements
                                            (append 
                                             (or (get-binding* '() 'graph-statements bindings) '())
                                             (if in-where? '()
                                                 (or gmatch '())))
                                            bindings))))))))))))

(define (rewrite-triples-rules realm)
    `((,triple? . ,(%rewrite-triple-rule realm))
      ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)
      (,select? . ,rw/copy)
      (,subselect? . ,rw/copy)))

(define flatten-graph-rules
  `((,triple? . ,rw/copy)
    ((GRAPH) . ,(lambda (block rules bindings)
                  (values (cddr block) bindings)))
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
    ((@Dataset) . ,rw/copy)
    (,select? . ,rw/copy)
    (,subselect? . ,rw/copy)
    (,pair? . ,rw/continue)))

(define (rewrite-subselect block rules bindings)
  (match block
    ((((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . quads)
     (let* ((subselect-vars (extract-subselect-vars vars))
            (bindings (if (or (equal? subselect-vars '(*))
                              (equal? subselect-vars '*))
                          bindings
                          (project-bindings subselect-vars bindings))))
       (let-values (((rw b) (rewrite quads rules bindings)))
         (values (list (cons (car block) rw)) (merge-bindings b bindings)))))))

(define (where-subselect? block)
  (and (equal? (car block) 'WHERE)
       (subselect? (cdr block))))

(define (graph-rewriter-rules #!key preserve-graph-statements? rewrite-select-queries? realm)
  (let* ((graph-realm (or realm
                          (($headers) 'mu-graph-realm)
                          (get-realm (($headers) 'mu-graph-realm-id))
                          (get-realm (($query) 'graph-realm-id))
                          (($query) 'graph-realm)
                          (($body) 'graph-realm)
                          (get-realm (hash-table-ref/default *session-realm-ids* ($mu-session-id) #f))))
         (rewrite-graph-statements? (not (or preserve-graph-statements?
                                             (($headers) 'preserve-graph-statements)
                                             (($query) 'preserve-graph-statements))))
         (rewrite-select-queries? (or rewrite-select-queries?
                                      (equal? "true" (($headers) 'rewrite-select-queries))
                                      (equal? "true" (($query) 'rewrite-select-queries))
                                      (*rewrite-select-queries?*))))

    (log-message "~%==Graph Realm==~%~A~%" graph-realm)

    `(((@Unit) . ,rw/continue)
      ((@Prologue)
       . ,(lambda (block rules bindings)
            (values `((@Prologue
                       (PREFIX |rewriter:| <http://mu.semte.ch/graphs/>)
                       ,@(cdr block)))
                    bindings)))
      ((@Query)
       . ,(lambda (block rules bindings)
            (if rewrite-select-queries?
                (with-rewrite ((new-statements (rewrite (cdr block) rules bindings)))
                  `((,(car block) ,new-statements)))
                ;; what about rewrite-graph-rules = #f??
                ;; (extract-graphs (alist-ref 'WHERE rw)))
                (with-rewrite ((rw (rewrite (cdr block) flatten-graph-rules)))
                  `((@Query
                     ,(alist-update
                       '@Dataset (replace-dataset graph-realm 'FROM #f)
                       rw)))))))
      ((@Update)
       . ,(lambda (block rules bindings)
            (let-values (((rw nbs) (rewrite (reverse (cdr block)) rules bindings)))
              (let ((where-block (alist-ref 'WHERE rw))
                    (graph-statements (or (get-binding* '() 'graph-statements nbs) '())))
                (values `((@Update . ,(alist-update
                                       'WHERE
                                       `((GRAPH ,(*default-graph*) (?AllGraphs a rewriter:Graph))
                                         ,@graph-statements
                                         ,@where-block)
                                       (reverse rw))))
                        nbs)))))
      ((@Dataset) . ,rw/copy)
      ((@Using) . ,rw/copy)
      ((GRAPH) . ,rw/copy)
      (,select? . ,rw/copy) ;;  '* => (extract-all-variables)
      (,subselect? . ,rewrite-subselect)        
      (,where-subselect?
       . ,(lambda (block rules bindings)
            (let-values (((rw b) (rewrite-subselect (cdr block) rules bindings)))
              (values `((WHERE ,@rw))
                      (merge-bindings b bindings)))))
      (,quads-block?
       . ,(lambda (block rules bindings)
            ;; (let ((new-context (if (equal? (car block) 'WHERE) (alist-update 'in-place? #t))))
              (let-values (((bl1 b1) (rewrite (cdr block) (expand-triples-rules rewrite-graph-statements?)
                                              bindings)))
                (let-values (((bl2 b2) (rewrite bl1 (rewrite-triples-rules graph-realm) b1)))
                  (let-values (((bl3 b3) (rewrite bl2 rules b2)))
                    (values `((,(rewrite-block-name (car block)) ,@bl3))
                            b3)))))) ;)
      ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
      ((*REWRITTEN*)
       . ,(lambda (block rules bindings)
            (values (cdr block) bindings)))
      (,pair? . ,(lambda (block rules bindings)
                   (with-rewrite ((rw (rewrite block rules bindings)))
                                 (list rw)))))))

(default-rules graph-rewriter-rules)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Additional Endpoints

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

	       
