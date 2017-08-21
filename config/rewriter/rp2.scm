(define-namespace rewriter "http://mu.semte.ch/graphs/")

(define *realm-id-graph*
  (config-param "REALM_ID_GRAPH" '<http://mu.semte.ch/uuid> read-uri))

(define *session-realm-ids* (make-hash-table))

(define *realm* (make-parameter #f))

(define (replace-dataset realm label #!optional named? (extra-graphs '()))
  (dataset label (append (all-graphs realm) extra-graphs) named?))

;; for un-rewritten SELECT queries
(select-query-rules
 `((,triple? . ,rw/copy)
   ((GRAPH)
    . ,(lambda (block bindings)
         (values (cddr block) bindings)))
   ((FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
   ((@Dataset) 
    . ,(lambda (block bindings)
         (let ((realm (query-graph-realm)))
           (values (list (replace-dataset realm 'FROM #f)) bindings))))
   (,select? . ,rw/copy)
   (,subselect? . ,rw/copy)
   (,pair? . ,rw/continue)))

;; Extending the default rules
(rules
 `( ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite-quads-block block bindings)))
            (values `((WHERE
                       (GRAPH ,(*default-graph*) (?AllGraphs a rewriter:Graph))
                       ,@rw))
                    new-bindings))))
    ,@(rules)))

(triples-rules
 (lambda ()
   (let ((realm (query-graph-realm)))
     (log-message "~%==Graph Realm==~%~A~%" realm)
     `((,triple? . ,(%rewrite-triple-rule realm))
       ((FILTER BIND MINUS OPTIONAL UNION GRAPH) . ,rw/copy)
       (,select? . ,rw/copy)
       (,subselect? . ,rw/copy)))))

(define (query-graph-realm)
  (or (*realm*) ;; for testing
      (header 'mu-graph-realm)
      (get-realm (header 'mu-graph-realm-id))
      (get-realm (($query) 'graph-realm-id))
      (($query) 'graph-realm)
      (($body) 'graph-realm)
      (get-realm (hash-table-ref/default *session-realm-ids* (header 'mu-session-id) #f))))

(define (get-realm realm-id)
  (and realm-id
       (query-unique-with-vars
        (realm)
        (s-select '?realm (write-triples `((?realm mu:uuid ,realm-id)))
                  from-graph: (*realm-id-graph*))
        realm)))

(define (%rewrite-triple-rule realm)
  (lambda (triple bindings)
    (let ((in-where? ((parent-axis (lambda (context) 
                                     (let ((head (context-head context)))
                                       (and head (equal? (car head) 'WHERE))))) (*context*))))
      (match triple
        ((s p o)
         (if (and (equal? p 'a) in-where?)
             (values `((*REWRITTEN* (GRAPH ?AllGraphs ,triple))) bindings)
             (let* ((stype (get-binding* (list s) 'stype bindings))
                    (new-stype? (get-binding* (list s) 'new-stype? bindings))
                    (bound-graph (get-binding* (list s p) 'graph bindings))
                    (solved-graph (and (iri? stype) (iri? p) (get-graph realm stype p)))
                    (graph (or bound-graph solved-graph (new-sparql-variable "graph")))
                    (named-graphs (get-binding 'named-graphs bindings))
                    (gmatch (and (not bound-graph)
                                 (graph-match-statements realm graph s stype p
                                                         named-graphs new-stype?))))
                                                         ;;(and new-stype? in-where?)))))
               (if solved-graph
                   (values `((*REWRITTEN* (GRAPH ,graph ,triple))) bindings)
                   (values `((*REWRITTEN* 
                              ,@(splice-when (and in-where? gmatch)))
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

(define (graph-match-statements realm graph s stype p named-graphs new-stype?)
  (let ((rule (new-sparql-variable "rule"))
	(gtype (new-sparql-variable "gtype")))
    `(,@(splice-when
	 (and (not (iri? stype))
              new-stype?
              ;; better to add this only once!!
              ;; and what about types in un-rewritten named-graph? 
	      ;; `((GRAPH ?AllGraphs (,s a ,stype)))))
              (if named-graphs
                  `((UNION
                    ((GRAPH ?AllGraphs (,s a ,stype)))
                    ,@(map (lambda (graph)
                             `((GRAPH ,graph (,s a ,stype))))
                           named-graphs)))
                  `((GRAPH ?AllGraphs (,s a ,stype))))))
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

;; for testing
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

;; for testing
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

