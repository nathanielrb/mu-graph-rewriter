(define-namespace rewriter "http://mu.semte.ch/graphs/")

(define *realm-id-graph*
  (config-param "REALM_ID_GRAPH" '<http://mu.semte.ch/uuid> read-uri))

(define *session-realm-ids* (make-hash-table))

(define *realm* (make-parameter #f))

(define (replace-dataset realm label #!optional named? (extra-graphs '()))
  (dataset label (append (all-graphs realm) extra-graphs) named?))

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

(define (query-graph-realm)
  (or (*realm*) ;; for testing
      (header 'mu-graph-realm)
      (get-realm (header 'mu-graph-realm-id))
      (get-realm (($query) 'graph-realm-id))
      (($query) 'graph-realm)
      (($body) 'graph-realm)
      (get-realm (hash-table-ref/default *session-realm-ids* (header 'mu-session-id) #f))))

(define (get-type x) #f)

(define (get-realm realm-id) 
  (and realm-id
       (query-unique-with-vars
        (realm)
        (s-select '?realm (write-triples `((?realm mu:uuid ,realm-id)))
                  from-graph: (*realm-id-graph*))
        realm)))

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

(define (add-realm realm graph graph-type)
  (sparql-update
   (s-insert
    (write-triples
     `((,graph rewriter:realm ,realm)
       (,graph a rewriter:Graph)
       (,graph rewriter:type ,graph-type))))))

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

(define (delete-realm realm graph)
  (sparql-update
   (if graph
       (s-delete
        (write-triples `((,graph ?p ?o)))
        where: (write-triples `((,graph ?p ?o))))
       (s-delete
        (write-triples `((?graph ?p ?o)))
        where: (write-triples `((?graph rewriter:realm ,realm)
                                (?graph ?p ?o)))))))

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
