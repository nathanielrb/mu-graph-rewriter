(use files matchable intarweb spiffy spiffy-request-vars
     uri-common intarweb medea irregex srfi-13 http-client cjson
     memoize)

(import s-sparql mu-chicken-support)
(use s-sparql mu-chicken-support)

(require-extension sort-combinators)

(define *subscribers-file*
  (config-param "SUBSCRIBERSFILE" 
                (if (feature? 'docker)
                    "/config/subscribers.json"
                    "./config/rewriter/subscribers.json")))

(define *subscribers*
  (handle-exceptions exn '()
    (vector->list
     (alist-ref 'potentials
                (with-input-from-file (*subscribers-file*)
                  (lambda () (read-json)))))))

(define *functional-properties* (make-parameter '()))

;; Can be a string, an s-sparql expression, 
;; or a thunk returning a string or an s-sparql expression.
;; Used by (constrain-triple) below.
(define *read-constraint*
  (make-parameter
   `((@QueryUnit
      ((@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE (GRAPH ,(*default-graph*) (?s ?p ?o)))))))))

(define *write-constraint*
  (make-parameter
   `((@QueryUnit
      ((@Query
       (CONSTRUCT (?s ?p ?o))
       (WHERE (GRAPH ,(*default-graph*) (?s ?p ?o)))))))))
 
(define *plugin*
  (config-param "PLUGIN_PATH" ))
                ;; (if (feature? 'docker)
                ;;     "/config/plugin.scm"
                ;;     "./config/rewriter/plugin.scm")))

(define *cache* (make-hash-table))

(define *rewrite-graph-statements?*
  (config-param "REWRITE_GRAPH_STATEMENTS" #t))

(define (preserve-graphs?)
  (or (header 'preserve-graph-statements)
      (($query) 'preserve-graph-statements)
      (not (*rewrite-graph-statements?*))))

(define *rewrite-select-queries?* 
  (config-param "REWRITE_SELECT_QUERIES" #t))

(define (rewrite-select?)
  (or (equal? "true" (header 'rewrite-select-queries))
      (equal? "true" (($query) 'rewrite-select-queries))
      (*rewrite-select-queries?*)))

(define *send-deltas?* 
  (config-param "SEND_DELTAS" #f))

(define *calculate-annotations?* 
  (config-param "CALCULATE_ANNOTATIANS" #t))

(define *calculate-potentials?* 
  (config-param "CALCULATE_POTENTIAL_GRAPHS" #f))

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
                                (project-bindings subselect-vars bindings))))
       (let-values (((rw new-bindings) (rewrite rest inner-bindings)))
         (values `((@SubSelect (,label ,@(filter sparql-variable? vars)) ,@rw)) 
                 (merge-bindings 
                  (project-bindings subselect-vars new-bindings)
                  bindings)))))))

(define (rw/list block bindings)
  (let-values (((rw new-bindings) (rewrite block bindings)))
    (cond ((nulll? rw) (values '() new-bindings))
          ((fail? rw) (values (list #f) new-bindings))
          (else (values (list (filter pair? rw)) new-bindings)))))

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

(define (rdf-equal? a b)
  (let ((nss (append (query-namespaces)
		     ;; (*constraint-namespaces*)
		     (*namespaces*)))) ; memoize this
    (if (and (symbol? a) (symbol? b))
        (equal? (expand-namespace (a->rdf:type a) nss)
                (expand-namespace (a->rdf:type b) nss))
        (equal? a b))))

(define (rdf-member a bs)
  (let ((r (filter (cut rdf-equal? a <>) bs)))
    (if (null? r) #f r)))

(define (literal-triple-equal? a b)
  (null? (filter not (map rdf-equal? a b))))

(define replace-a
  (match-lambda ((s p o) `(,s ,(a->rdf:type p) ,o))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Solving and Simplifying
(define (replace-variables exp substitutions)
  (let rec ((exp exp))
    (if (null? exp) '()
	(let ((e (car exp))
	      (rest (rec (cdr exp))))
	  (if (pair? e)
	      (cons (rec e) rest)
	      (cons (or (alist-ref e substitutions) e) rest))))))
                             
(define (simplify-values vars vals bindings)
  (let ((vars (if (pair? vars) vars (list vars)))
	(vals (if (pair? (car vals)) vals (map list vals))))
    (call/cc
     (lambda (out)
       (let loop ((vars vars) (new-vars '()) 
		  (vals vals) (new-vals (make-list (length vals) '())))
	 (cond ((null? vars) 
		(if (null? new-vars) '()
		    `(VALUES ,(reverse new-vars) ,@(map reverse new-vals))))
	       ((pair? vars)
		(cond ((sparql-variable? (car vars))
		       (loop (cdr vars) 
			     (cons (car vars) new-vars)
			     (map cdr vals) 
			     (map cons (map car vals) new-vals)))
		      (else
		       (let ((matches (map (cut rdf-equal? <> (car vars)) (map car vals))))
			 (if (null? (filter values matches))
			     (out #f)
			     (loop (cdr vars) new-vars
				   (filter values (map (lambda (match? val) (and match? val)) 
						       matches vals))
				   (filter values (map (lambda (match? val) (and match? val)) 
						       matches new-vals))))))))
	       (else (cond ((sparql-variable? vars) `(VALUES ,vars ,@vals))
			   (else
			    (if (null? (filter not (map (cut rdf-equal? <> vars) vals)))
				'()
				(out #f)))))))))))

(define (validate-filter statement bindings)
  (let ((check (validate-constraint (second statement) bindings)))
    (case check
      ((#t) (values '() bindings))
      ((?) (values (list statement) bindings))
      ((#f)
       (values (list #f) bindings)))))

(define (validate-constraint constraint bindings)
  (match constraint
    ((`= a b) (unify a b bindings))
    ((`!= a b) (disunify a b bindings))
    ((`IN a bs) (constraint-member a bs bindings))
    ((`|NOT IN| a b)  (constraint-not-member a b bindings))
    (else '?)))

;; These are way too simple.
(define (unify a b bindings)
  (if (or (sparql-variable? a) (sparql-variable? b))
      '?
      (rdf-equal? a b)))

(define (disunify a b bindings)
  (if (or (sparql-variable? a) (sparql-variable? b))
      '?
      (not (rdf-equal? a b))))

(define (constraint-member a bs bindings)
  (let ((possible? (lambda (x) (or (sparql-variable? x) (rdf-equal? a x)))))
    (cond ((sparql-variable? a) '?)
	  ;; ((member a b) #t)
          ((not (null? (filter possible? bs))) #t)
	  ((null? (filter sparql-variable? bs)) #f)
	  (else '?))))

(define (constraint-not-member a b bindings)
  (cond ((sparql-variable? a) '?)
	((member a b) #f)
	((null? (filter sparql-variable? b)) #t)
	(else '?)))
  
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
;; Annotations
;; bug: annotation in top-level WHERE { @access x . .... }
;; breaks optimizations
(define (annotation? exp)
  (and (pair? exp)
       (equal? (car exp) '@Annotation)))

(define (nulll? block)
  (null? (remove annotation? block)))

(define (get-annotations query)
  (delete-duplicates (rewrite-query query (get-annotations-rules))))

;; subselects/projection?
(define (get-annotations-rules)
  `(((@QueryUnit @UpdateUnit)
     . ,(lambda (block bindings)
          (let-values (((rw b) (rewrite (cdr block) bindings)))
            (values (list rw) b))))
    ((@Query @Update WHERE OPTIONAL)
     . ,(lambda (block bindings)
          (rewrite (cdr block) bindings)))
    (,annotation? 
     . ,(lambda (block bindings)
          (values
           (match block
             ((`@Annotation `access key)  (list key))
             ((`@Annotation `access key var) (list (list key var)))
             (else (error (format "Invalid annotation: ~A" block))))
           bindings)))
    (,select? . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings)
          (match block
            ((@SubSelect (label . vars) . rest)
             (rewrite rest bindings)))))
    ((GRAPH)
     . ,(lambda (block bindings)
          (rewrite (cddr block) bindings)))
    ((UNION) 
     . ,(lambda (block bindings)
          (let-values (((rw b) (rewrite (cdr block))))
            (let ((vals (apply merge-alists (map second (filter values? rw))))
                  (quads (filter (compose not values?) rw)))
            (values (append quads `((*values* ,vals)))
                    b)))))
    (,triple? . ,rw/remove)
    ((VALUES)
     . ,(lambda (block bindings)
          (match block
            ((`VALUES vars . vals)
             (values
              (if (pair? vars)
                  `((*values*
                    ,(apply
                      merge-alists
                      (map (lambda (vallst)
                             (map (lambda (var val)
                                    (list var
                                          (expand-namespace val (append (*namespaces*) (query-namespaces)))))
                                  vars vallst)) 
                           vals))))
                  `((*values*
                     ,(map (lambda (val)
                             (,vars ,(expand-namespace val))))
                     vals)))
              bindings)))))
    ((@Prologue @Dataset @Using CONSTRUCT SELECT FILTER BIND |GROUP BY| OFFSET LIMIT INSERT DELETE) 
     . ,rw/remove)
    (,list?
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite block bindings)))
            (let-values (((vals annotations) (partition values? rw))) ; generalize this
              (let ((merged-vals (apply merge-alists (map second vals))))
              (values (append (map (lambda (a)
                                     (update-annotation-values a merged-vals))
                                   annotations)
                              `((*values* ,merged-vals)))
                      new-bindings))))))))

(define (update-annotation-values annotation valss)
  (match annotation
    ((key var)
     `(,key ,var ,(alist-ref var valss)))
    (else annotation)))

;; merges alists whose values must be lists
;; e.g., '((a . (1 2 3))) '((a . (4)))
(define (merge-alists #!rest alists)
  (let loop ((alists alists) (accum '()))
    (if (null? alists) accum
        (let inner ((alist (car alists)) (accum accum))
          (if (null? alist) (loop (cdr alists) accum)
              (let ((kv (car alist)))
                (inner (cdr alist)
                       (alist-update-proc (car kv) 
                                          (lambda (current)
                                            (append-unique (or current '()) (cdr kv)))
                                          accum))))))))

(define (values? exp)
  (and (pair? exp) (equal? (car exp) '*values*)))


(define (query-time-annotations annotations)
  (map (lambda (a)
         (match a
                ((key var vals)
                 `(,key ,(string->symbol (string-join (map symbol->string vals)))))
                (else a)))
       (remove values? annotations)))

;; filter through values with rdf-equal?
(define (query-annotations annotations query)
  (let-values (((pairs singles) (partition pair? annotations)))
    (let ((pairs (remove values? pairs)))
      (if (null? pairs)
          singles
          (let ((annotations-query
                 (rewrite-query 
                  query 
                  (query-annotations-rules (delete-duplicates (map second pairs))))))
            (join
             (map (lambda (row)
                    (filter pair?
                         (map (lambda (annotation binding)
                                (match binding
                                  ((var . val)
                                   (if (and (equal? (->string var)
                                                    (substring (->string (second annotation)) 1))
                                            (or (null? (cddr annotation))
                                                (member val (third annotation))))
                                       (list (first annotation) val)
                                       '()))))
                              pairs row)))
                       (sparql-select (write-sparql annotations-query))))))))))

(define (query-annotations-rules vars)
  `(((@UpdateUnit @QueryUnit) 
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings)))
            (values `((@QueryUnit ,@rw)) bindings))))
    ((@Update @Query) 
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings)))
            (values `((@Query
                       ,@(insert-child-before 'WHERE
                                              `(SELECT DISTINCT ,@vars)
                                              rw)))
                    bindings))))
    ((@Using) 
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings)))
            (values `((@Dataset ,@rw)) bindings))))
    ((@Dataset @Prologue WHERE) . ,rw/copy)
    ((INSERT DELETE) . ,rw/remove)
    (,select? . ,rw/remove)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Expanding triples and quads
;; refactor this a bit, to use #!key for mapp and bindingsp
;; and mostly to NOT use a flatten-graphs? parameter, but separate functions & arg passing
(define flatten-graphs? (make-parameter #f))

(define (expand-triples block #!optional (bindings '()) mapp bindingsp)
  (rewrite block bindings (expand-triples-rules mapp bindingsp)))

;; (define (expand-triples-flatten-graphs

(define (recursive-expand-triples block #!optional (bindings '()) mapp bindingsp)
  (rewrite block bindings (recursive-expand-triples-rules mapp bindingsp)))

;; (define (recursive-expand-triples-flatten-graphs 

(define (expand-triple-rule #!optional mapp bindingsp)
  (let ((mapp (or mapp values))
        (bindingsp (or bindingsp (lambda (triples bindings) bindings))))
    (lambda (triple bindings)
      (let ((triples (map mapp (expand-triple triple))))
        (values triples (bindingsp triples bindings))))))

(define (expand-triples-rules #!optional mapp bindingsp)
  `(((@QueryUnit @UpdateUnit @Query @Update CONSTRUCT WHERE
      DELETE INSERT |DELETE WHERE| |INSERT DATA|) . ,rw/quads)
    (( @Prologue @Dataset @Using) . ,rw/copy)
    ((FILTER BIND MINUS OPTIONAL UNION VALUES
      @SubSelect |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,select? . ,rw/copy)
    ((UNION) . ,rw/union)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
	  (if (flatten-graphs?)
              (values rw new-bindings)
	      (values `((GRAPH ,(second block) ,@rw))
		      (cons-binding (second block) 'named-graphs new-bindings))))))
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))

(define (recursive-expand-triples-rules #!optional mapp bindingsp)
  `(((@QueryUnit @UpdateUnit @Query @Update CONSTRUCT WHERE
      DELETE INSERT |DELETE WHERE| |INSERT DATA|) . ,rw/quads)
    ((@Prologue @Dataset @Using) . ,rw/copy)
    (,symbol? . ,rw/copy)
    (,annotation? . ,rw/copy)
    ((MINUS OPTIONAL) . ,rw/quads)
    ((UNION) . ,rw/union)
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
            (if (flatten-graphs?)
                (values rw new-bindings)
                (values `((GRAPH ,(second block) ,@rw)) new-bindings)))))
    (,triple? . ,(expand-triple-rule mapp bindingsp))
    ((VALUES FILTER BIND |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

;; be careful with optionals etc:
;; (expand-graphs '((GRAPH <G>  (OPTIONAL (?s ?p ?o)) (?s a <Truck>)))))
;;             => '((OPTIONAL (GRAPH <G> (?s ?p ?o))) (GRAPH <G> (?s a <Truck>)))
(define (expand-graphs statements #!optional (bindings '()))
  (rewrite statements bindings expand-graphs-rules))

(define eg-graph (make-parameter #f))

(define expand-graphs-rules
  `(((@SubSelect) . ,rw/subselect)
    (,select? . ,rw/copy)
    ((OPTIONAL WHERE) . ,rw/quads)
    ((UNION) . ,rw/union)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . quads)
             (parameterize ((eg-graph graph))
               (rewrite quads bindings))))))
    (,triple? 
     . ,(lambda (triple bindings)
          (values `((GRAPH ,(eg-graph) ,triple))
                  bindings)))
    (,annotation? 
     . ,(lambda (annotation bindings)
          (values `((GRAPH ,(eg-graph) ,annotation))
                  bindings)))
    ((VALUES FILTER BIND MINUS |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)
    (,symbol? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Graphs
(define (get-all-graphs query)
  (let ((query (if (procedure? query) (query) query)))
    (let-values (((rewritten-query bindings) (rewrite-query query extract-graphs-rules)))
      (let ((query-string (write-sparql rewritten-query)))
        (let-values (((vars iris) (partition sparql-variable? (get-binding/default 'all-graphs bindings '()))))
          (if (null? vars) iris
              (delete-duplicates
               (append iris
                       (map cdr (join (sparql-select query-string from-graph: #f)))))))))))

(define extract-graphs-rules
  `(((@QueryUnit @UpdateUnit) . ,rw/quads)
    (,annotation? . ,rw/copy)
    ((@Query @Update) 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let ((graphs (delete-duplicates
                           (filter sparql-variable?
                                   (get-binding/default 'all-graphs new-bindings '())))))
            (values `((@Query
                       ,@(insert-child-before 'WHERE `(|SELECT DISTINCT| ,@graphs) rw)))
                    new-bindings)))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rw/quads block bindings)))
            (values rw new-bindings))))
    ((INSERT DELETE)
     . ,(lambda (block bindings)
          (let-values (((_ new-bindings) (rw/quads block bindings)))
            (values '() new-bindings))))
    ((@Prologue) . ,rw/copy)
    ((@Dataset @Using) 
     . ,(lambda (block bindings)
          (let ((graphs (map second (cdr block))))
            (values (list block) (fold-binding graphs 'all-graphs append '() bindings)))))
    (,select? . ,rw/remove)
    ((@SubSelect)
     . ,(lambda (block bindings)
          (match block
            ((@SubSelect (label . vars) . rest)
	     (let* ((dataset (get-child-body '@Dataset rest))
		    (graphs (and dataset (map second dataset))))
               (let-values (((rw new-bindings) (rewrite (get-child-body 'WHERE rest) bindings)))
		 (values
		  `((@SubSelect (,label ,@vars) ,@(replace-child-body 'WHERE rw rest)))
		  (if graphs
		      (fold-binding graphs 'all-graphs append '() new-bindings)
		      new-bindings))))))))
    ;; ((CONSTRUCT SELECT INSERT DELETE |INSERT DATA|) . ,rw/remove)
    ((CONSTRUCT SELECT) . ,rw/remove)
    ((GRAPH) . ,(lambda (block bindings)
                  (match block
                    ((`GRAPH graph . quads)
                     (let-values (((rw new-bindings) 
                                   (rewrite quads (cons-binding graph 'all-graphs bindings))))
                       (values `((GRAPH ,graph ,@rw)) new-bindings))))))

    ((UNION) 
     . ,(rw/lambda (block)
          (with-rewrite ((rw (rewrite (cdr block))))
	    `((UNION ,@rw)))))
    ((VALUES) ;; order-dependent
     . ,(lambda (block bindings)
          (let ((graphs (get-binding/default 'all-graphs bindings '())))
            (match block
              ((`VALUES vars . vals)
               (if (pair? vars)
                   (values (list block) bindings)
                   (if (member vars graphs)
                       (values (list block)
                               (update-binding 'all-graphs 
                                               (append vals (delete vars graphs))
                                               bindings))
                       (values (list block) bindings))))))))
    (,quads-block? . ,rw/quads)
    (,triple? . ,rw/copy)
    ((VALUES FILTER BIND) . ,rw/copy)
    ((|GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Find Graphs for Triples
(define (find-triple-graphs triple block)
  (delete-duplicates (rewrite block '() (find-triple-graphs-rules triple))))

;; doesn't work for nested graphs
(define (find-triple-graphs-rules triple)
  `(((GRAPH)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cddr block))))
            (values
             (if (nulll? rw) '()
                 (append (list (second block))
                         (filter symbol? rw)))
             bindings))))
    ((@SubSelect)
     . ,(lambda (block bindings)
          (rewrite (cddr block))))
    ((OPTIONAL WHERE UNION)
     . ,(lambda (block bindings)
          (rewrite (cdr block))))
    ((OPTIONAL) . ,rw/quads)
    (,triple? 
     . ,(lambda (block bindings)
          (values
           (if (fail? (map rdf-equal? block triple)) '()
               (list #t))
           bindings)))
    ((FILTER VALUES BIND) . ,rw/remove)
    (,annotation? . ,rw/remove)
    (,list?
     . ,(lambda (block bindings)
          (rewrite block)))))

(define (get-triple-graphs triple bindings)
  (let ((vars (filter sparql-variable? triple))
        (triple (map a->rdf:type triple)))
    (cdr-or 
     (assoc triple (get-binding/default* vars 'graphs bindings '()))
     '())))

(define (update-triple-graphs new-graphs triple bindings)
  (let ((vars (filter sparql-variable? triple))
        (triple (map a->rdf:type triple)))
    (fold-binding* triple
                   vars
                   'graphs
                   (lambda (triple graphs-list)
                     (alist-update-proc triple 
					(lambda (graphs)
					  (if graphs (append new-graphs graphs) new-graphs))
                                    graphs-list))
                   '()
                   bindings)))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Bindings, Substitutions and Renamings
(define (current-substitution var bindings)
  (let ((substitutions (get-binding/default 'constraint-substitutions bindings '())))
    (a->rdf:type (alist-ref var substitutions))))

(define (current-substitution-recursive block bindings)
  (let rec ((exp block))
    (cond ((null? exp) '())
	  ((pair? exp) 
           (cons (rec (car exp))
                 (rec (cdr exp))))
	  ((sparql-variable? exp)
           (current-substitution exp bindings))
	  (else exp))))

(define (dependency-substitution var bindings)
  (let ((substitutions (dependency-substitutions))) ;; (get-binding/default 'dependency-substitutions bindings '())))
    (a->rdf:type (alist-ref var substitutions))))

(define (substitution-or-value var bindings)
  (if (sparql-variable? var)
      (current-substitution var bindings)
      var))

(define (deps var bindings)
  (let ((dependencies (renaming-dependencies))) ;; (get-binding/default 'dependencies bindings '())))
    (filter (lambda (v) (member v (or (alist-ref var dependencies) '())))
	    ;;(get-binding/default 'matched-triple bindings '()))))
	    (matched-triple))))

;; (define deps (memoize deps*))

(define (keys* var bindings)
  (map (cut dependency-substitution <> bindings) (deps var bindings)))

(define keys (memoize keys*))

(define (renaming* var bindings)
  (let ((renamings
         (get-binding/default* (keys var bindings) (deps var bindings) bindings '())))
    (alist-ref var renamings)))

(define renaming (memoize renaming*))

(define (update-renaming var substitution bindings)
  (fold-binding* substitution
                 (keys var bindings)
                 (deps var bindings)
                 (cut alist-update var <> <>)
                '()
                bindings))

(define (project-substitutions vars substitutions)
  (filter (lambda (substitution)
            (member (car substitution) vars))
          substitutions))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Temp constraints
(define *temp-graph* (config-param "TEMP_GRAPH" '<http://mu.semte.ch/rewriter/temp> read-uri))

(define *use-temp?* (config-param "USE_TEMP" #f))

(define *insert-into-temp?* (make-parameter #f))

;; $query is broken; use header for now, and fix $query
(define (use-temp?)
  (cond ((or (header 'use-temp-graph) (($query) 'use-temp-graph))
         => (lambda (h) (equal? h "true")))
        (else (*use-temp?*))))

(define temp-rules
  `(((@QueryUnit @Query) . ,rw/continue)
    ((@Prologue @Dataset) . ,rw/copy)
    (,annotation? . ,rw/copy)
    ((CONSTRUCT) 
     . ,(lambda (block bindings)
          (values (list block) (update-binding 'triple (second block) bindings))))
    ((WHERE)
     . ,(lambda (block bindings)
          (let ((triple (get-binding 'triple bindings)))
            (values `((WHERE
                       (UNION ,(cdr block)
                              ((GRAPH ?temp ,triple)
                               (VALUES (?temp) (,(*temp-graph*)))))))
                    bindings))))))

(define (sync-temp-query)
  `(@UpdateUnit
    (@Update
     (@Prologue)
     (DELETE
      (GRAPH ,(*temp-graph*) (?s ?p ?o)))
     (INSERT
      (?s ?p ?o))
     (WHERE
      (GRAPH ,(*temp-graph*) (?s ?p ?o))))))

(define (sync-temp)
  (parameterize ((*rewrite-graph-statements?* #f)
                 (*use-temp?* #t))
    (sparql-update
     (write-sparql
      (rewrite-query (sync-temp-query) top-rules)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core Transformation
(define (rewrite-block-name name)
  (case name
    ((|INSERT DATA|) 'INSERT)
    ((|DELETE DATA| |DELETE WHERE|) 'DELETE)
    (else name)))

(define level-quads (make-parameter '()))

(define (rewritten? block) (equal? (car block) '*REWRITTEN*))

(define (new-level-quads new-quads)
  (append 
   (level-quads)
   (join (map cdr (filter rewritten? new-quads)))))

;; Virtuoso bug (?) when VALUES statement is not after all statements
;; whose variables it binds. Maybe not a bug at all. To think about.
(define (reorder block)
  (let loop ((statements block) (new-statements '())
             (values-statements '()) (annotations '()))
    (cond ((null? statements) (delete-duplicates
                               (append 
                                (reverse annotations)
                                (reverse new-statements)
                                (reverse values-statements))))
          ((equal? (caar statements) 'VALUES)
           (loop (cdr statements) new-statements 
                 (cons (car statements) values-statements) annotations))
          ((annotation? (car statements))
           (loop (cdr statements) new-statements
                 values-statements (cons (car statements) annotations)))
          (else
           (loop (cdr statements) (cons (car statements) new-statements) 
                 values-statements annotations)))))

(define (optimize-duplicates block)
   (lset-difference equal?
     (delete-duplicates 
      (group-graph-statements
       (reorder
        (filter pair? block))))
     (level-quads)))

(define (rewrite-quads-block block bindings)
  (parameterize ((flatten-graphs? (not (preserve-graphs?))))
    ;; expand triples
    (let-values (((q1 b1) (expand-triples block bindings replace-a)))
      ;; rewrite-triples
      (let-values (((q2 b2) (rewrite q1 b1 triples-rules)))
        ;; continue in nested quad blocks
        (let-values (((q3 b3)
                      (parameterize ((level-quads (new-level-quads q2)))
                        (rewrite q2 b2))))
          (cond ((nulll? q3) (values '() b3))
                ((fail? q3) (values (list #f) b3))
                (else (values (filter pair? q3) b3))))))))

(define (quads-block-rule block bindings)
  (let-values (((rw new-bindings) (rewrite-quads-block (cdr block) bindings)))
    (cond ((nulll? rw) (values '() new-bindings))
          ((fail? rw) (values (list #f) new-bindings))
          (else (values `((,(rewrite-block-name (car block)) 
                           ,@(optimize-duplicates rw)))
                        new-bindings)))))

(define top-rules
  `(((@QueryUnit @UpdateUnit) . ,rw/quads)
    ((@Prologue) 
     . ,(lambda (block bindings)
          (values `((@Prologue
                     ,@(append-unique 
			(*constraint-prologues*)
			(cdr block))))
                  bindings)))
	  
    ((@Dataset) . ,rw/remove)
    ((@Using) . ,rw/remove)
    ((@Query)
     . ,(lambda (block bindings)
	  (if (rewrite-select?)
	      (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
		  (values `((@Query 
                             ,@(replace-child-body 
                                'WHERE
                                (clean
                                (apply-optimizations
				 (group-graph-statements
				  (reorder
                                   (get-child-body 'WHERE rw)))))
                                rw)))
			  new-bindings))
	      (with-rewrite ((rw (rewrite (cdr block) bindings select-query-rules)))
	        `((@Query ,@rw))))))
    ((@Update)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (reverse (cdr block)) '())))
	    (if (insert-query? block) 
		(values
		 `((@Update
		    ,@(instantiated-insert-query rw new-bindings)))
		 new-bindings)
		(let* ((delete-constraints (get-binding/default 'delete-constraints new-bindings '()))
                       (new-where
                        (clean
                         (apply-optimizations 
                          (group-graph-statements
                           (reorder
                            (append (or (get-child-body 'WHERE rw) '())
                                    delete-constraints))))))
		       (delete-block (triples-to-quads (get-child-body 'DELETE rw) new-where)) 
                       ;; (rewrite (get-child-body 'DELETE rw) new-bindings 
                       ;;          (triples-to-quads-rules new-where)))
		       (rw (replace-child-body 'DELETE delete-block rw)))
		  (if (nulll? new-where)
		      (values `((@Update ,@(reverse rw))) new-bindings)
		      (values `((@Update ,@(replace-child-body 
					    'WHERE 
					    `((@SubSelect (SELECT *) (WHERE ,@new-where)))
					    (reverse rw))))
			      new-bindings)))))))
    (,select? . ,rw/copy)
    ((@SubSelect) . ,rw/subselect)
    ((GRAPH) . ,rw/copy)
    ((*REWRITTEN*)
     . ,(lambda (block bindings)
          (values (cdr block) bindings)))
    ((DELETE |DELETE WHERE| |DELETE DATA|)
     . ,(lambda (block bindings)
          (parameterize ((delete? #t)) (quads-block-rule block bindings))))
    ((INSERT |INSERT WHERE| |INSERT DATA|)
     . ,(lambda (block bindings)
          (parameterize ((insert? #t)) (quads-block-rule block bindings))))
    ((MINUS OPTIONAL GRAPH) . ,quads-block-rule)
    ((WHERE)
     . ,(lambda (block bindings)
	  (parameterize ((where? #t))
            (let-values (((rw new-bindings) (rewrite-quads-block (cdr block) bindings)))
              (cond ((nulll? rw) (values '() new-bindings))
                    ((fail? rw) (values (list #f) new-bindings))
                    (else (values `((,(rewrite-block-name (car block)) 
                                     ,@(optimize-duplicates rw)))
                                  new-bindings)))))))
    ;; ((WHERE)
    ;;  . ,(lambda (block bindings)
    ;;       (let-values (((rw new-bindings) (rewrite-quads-block block bindings)
    ;; (,quads-block? . ,rewrite-quads-block)
    ((UNION) . ,rw/union)
    ((FILTER BIND |ORDER| |ORDER BY| |LIMIT| |GROUP BY| OFFSET LIMIT) . ,rw/copy)
    (,annotation? . ,rw/copy)
    (,list? 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite-quads-block block bindings)))
            (cond ((nulll? rw) (values '() new-bindings))
                  ((fail? rw) (values (list #f) new-bindings))
                  (else (values (list (filter pair? rw)) new-bindings))))))

    ;;(,list?  . ,rewrite-quads-block)
     ;; . ,(lambda (block bindings)
     ;;      (log-message "~A => ~A" block (expand-triples block bindings replace-a))
     ;;      (log-message "==> "           (rw/list (expand-triples block bindings replace-a) bindings))
     ;;      (rw/list (expand-triples block bindings replace-a) bindings)))

    (,symbol? . ,rw/copy)))   

(define (triples-to-quads triples constraints)
  (rewrite triples '() (triples-to-quads-rules constraints)))

(define (triples-to-quads-rules constraints)
  `((,triple?
     . ,(lambda (triple bindings)
          (let* ((graphs (find-triple-graphs triple constraints))
                 (graphs* (if (*insert-into-temp?*) graphs
                              (remove (cut equal? (*temp-graph*) <>) graphs))))
            (values (map (lambda (graph) `(GRAPH ,graph ,triple)) graphs*) bindings))))))

;; this is a bit circuitous and verbose, but works for now.
;; and a big mess. which one to use, when?? values/not values/mixing with quads/contstraint
;; (insert-block (rewrite insert-block new-bindings (triples-to-quads-rules constraints)))
;; problem: literal graph has to be in VALUES or it's not applied on instantiation
;; e.g., GRAPH ?g { s p o } VALUES (?g){ (<G>) }
(define (instantiated-insert-query rw new-bindings)
  (parameterize ((flatten-graphs? #t))
    (let* ((opt (compose delete-duplicates apply-optimizations group-graph-statements reorder))
           (delete (expand-triples (or (get-child-body 'DELETE rw) '()) '() replace-a))
           (insert (expand-triples (or (get-child-body 'INSERT rw) '()) '() replace-a))
           (where (opt (or (get-child-body 'WHERE rw) '())))
           (insert-constraints (opt (get-binding/default 'insert-constraints new-bindings '())))
           (delete-constraints (opt (get-binding/default 'delete-constraints new-bindings '())))
           (instantiated-constraints (instantiate insert-constraints insert))
           (new-delete (triples-to-quads delete (append delete-constraints where)))
           (new-insert (triples-to-quads insert (append insert-constraints where)))
           (new-where (opt (append instantiated-constraints delete-constraints where))))
      (replace-child-body-if 
       'DELETE (and (not (null? new-delete)) new-delete)
       (replace-child-body-if
        'INSERT (and (not (null? new-insert)) new-insert)
        (if (null? new-where)
            (replace-child-body 'WHERE '() (reverse rw)) ; is this correct?
            (replace-child-body 
             'WHERE `((@SubSelect (SELECT *) (WHERE ,@new-where)))
             (reverse rw))))))))

(define (make-dataset label graphs #!optional named?)
  (let ((label-named (symbol-append label '| NAMED|)))
    `((,label ,(*default-graph*))
      ,@(map (lambda (graph) `(,label ,graph))
             graphs)
      ,@(splice-when
         (and named?
              `((,label (NAMED ,(*default-graph*))) 
                ,@(map (lambda (graph) `(,label (NAMED ,graph)))
                       graphs)))))))

(define select-query-rules
 `(((GRAPH)
    . ,(rw/lambda (block) 
         (cddr block)))
   ((@Prologue @SubSelect WHERE FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
    (,annotation? . ,rw/copy)
   ((@Dataset) 
    . ,(lambda (block bindings)
         (let ((graphs (get-all-graphs (*read-constraint*))))
	   (values `((@Dataset ,@(make-dataset 'FROM graphs #f))) 
		   (update-binding 'all-graphs graphs bindings)))))
   (,select? . ,rw/copy)
   (,triple? . ,rw/copy)
   (,list? . ,rw/list)))

(define triples-rules
  `((,triple? 
     . ,(lambda (triple bindings)
          (let ((graphs (get-triple-graphs triple bindings)))
            (if (or #t (null? graphs)) ; this seems wrong, once we have different read/write constraints,
                (if (where?)           ; so I turned it off... but is optimize-duplicates enough?
                    (apply-read-constraint triple bindings)
                    (apply-write-constraint triple bindings))
                (values ; not used!
                 (if (where?) '() 
                     `((*REWRITTEN* ,@(list triple))))
                 bindings)))))
    (,annotation? . ,rw/copy)
    (,list? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constraint Renaming
(define (parse-constraint constraint)
  (let ((constraint
         (if (pair? constraint) constraint (parse-query constraint))))
    (car (recursive-expand-triples (list constraint) '() replace-a))))

;; (define parse-constraint (memoize parse-constraint*))

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
           ;; (C** (if (and (use-temp?) (update?))
           ;;          (rewrite-query C* temp-rules)
           ;;          C*)))
      (rewrite (list C*) bindings (apply-constraint-rules triple)))))

(define (apply-read-constraint triple bindings)
  (apply-constraint triple bindings (*read-constraint*)))

(define (apply-write-constraint triple bindings)
  (apply-constraint triple bindings (*write-constraint*)))

;; (define (remove-renaming var bindings)
;;  (delete-binding* (keys var bindings) (deps var bindings) bindings))

;; (define (project-renamings ..) 

;; (define (merge-renamings ...)

;;(define (apply-constraint-rule 

(define constraint-where (make-parameter '()))

(define matched-triple (make-parameter '()))
(define constraint-substitutions (make-parameter '()))
(define dependency-substitutions (make-parameter '()))
(define dependencies  (make-parameter '()))

(define (apply-constraint-rules triple)
  (match triple
    ((a (`^ b) c) (apply-constraint-rules `(,c ,b ,a)))
    ((a b c)
     `((,symbol? . ,rw/copy)
       ((@QueryUnit) 
        . ,(lambda (block bindings)
             (rewrite (cdr block) bindings)))
       (,annotation? . ,rw/copy)
       ((@Query)
        . ,(lambda (block bindings)
       	     (parameterize ((constraint-where (list (get-child 'WHERE (cdr block)))))
       	       (rewrite (cdr block) bindings))))
       ((CONSTRUCT) 
        . ,(lambda (block bindings)
             (let-values (((triples _) 
                           (parameterize ((flatten-graphs? #t))
                             (expand-triples (cdr block)))))
               (let-values (((rw new-bindings) (rewrite triples bindings)))
                 (let* ((constraints (filter (compose not fail?) rw))
                        (constraints (if (equal? (length constraints) 1) ; what's this?
                                         (car constraints)
                                         `((UNION ,@constraints))))) ; and this?
                   (let ((graphs (find-triple-graphs (list a b c) constraints))) ; is this still used?
                     (if (where?)
                         (values `((*REWRITTEN* ,@constraints)) 
                                 (update-triple-graphs graphs (list a b c) 
                                                       new-bindings))
                         (values
                          `((*REWRITTEN* (,a ,b ,c)))
                          (fold-binding `((OPTIONAL ,@constraints ))
                                        (if (insert?) 'insert-constraints 'delete-constraints)
                                        append-unique '() 
                                        (update-triple-graphs graphs (list a b c) 
                                                              new-bindings))))))))))
       ((@Dataset) . ,rw/copy)
       (,triple? 
        . ,(lambda (triple bindings)
             (match triple
               ((s p o)
                (let ((initial-substitutions (make-initial-substitutions a b c s p o bindings)))
                  (if initial-substitutions
		      (parameterize ((matched-triple (list s p o))
				     (dependency-substitutions initial-substitutions))
			(let-values (((rw new-bindings)
				      (rewrite (constraint-where)
					       (update-binding 'constraint-substitutions initial-substitutions bindings)
					       rename-constraint-rules)))
			  (let ((cleaned-bindings #f)) ; (clean-constraint-bindings bindings new-bindings)))
			    (values (list (get-child-body 'WHERE rw)) ; (get-child-body 'WHERE rw)
                                    new-bindings))))
                      (values '() bindings)))))))
       (,list? . ,rw/remove)))))

(define (make-initial-substitutions a b c s p o bindings)
  (let ((check (lambda (u v) (or (sparql-variable? u) (sparql-variable? v) (rdf-equal? u v)))))
    (and (check a s) (check b p) (check c o)
	 `((,s . ,a) (,p . ,b) (,o . ,c)))))

(define (update-renamings new-bindings)
  (let ((substitutions (get-binding 'constraint-substitutions new-bindings)))
    (fold (lambda (substitution bindings)
	    (update-renaming (car substitution) (cdr substitution) bindings))
	  (delete-binding 'constraint-substitutions new-bindings)
	  substitutions)))

;; rewrite in CPS
(define (new-substitutions exp bindings)
  (let-values (((renamed-exp substitutions)
		(let loop ((exp exp) (renamed-exp '()) (substitutions '()))
		  (if (null? exp) 
		      (values renamed-exp substitutions)
		      (let ((var (car exp)))
			(cond ((pair? var)
			       (let-values (((sub subs) (loop var '() substitutions)))
				 (loop (cdr exp) 
				       (append renamed-exp (list sub)) ;; CPS
				       subs)))
			      ((not (sparql-variable? var))
			       (loop (cdr exp) 
				     (append renamed-exp (list var)) ;; CPS
				     substitutions))
			      ((current-substitution var bindings)
			       => (lambda (v) 
				    (loop (cdr exp) 
					  (append renamed-exp (list v)) ;; CPS
					  substitutions)))
			      ((renaming var bindings)
			       => (lambda (v)
				    (loop (cdr exp)
					  (append renamed-exp (list v))
					  (cons `(,var . ,v) substitutions))))
			      (else (let ((new-var (gensym var)))
				      (loop (cdr exp)
					    (append renamed-exp (list new-var))
					    (cons `(,var . ,new-var) substitutions))))))))))
    (values renamed-exp
	    (update-binding 'constraint-substitutions
			    (append substitutions
				    (get-binding 'constraint-substitutions bindings))
			    bindings))))

(define rename-constraint-triple
  (lambda (triple bindings)
    (let ((graph (get-context-graph)))
      (let-values (((renamed-quad new-bindings) (new-substitutions (cons graph triple) bindings)))
        (values             
         (if (and (use-temp?) (update?) (where?))
             `((UNION (,(cdr renamed-quad)) ((GRAPH ,(*temp-graph*) ,(cdr renamed-quad)))))
             (list (cdr renamed-quad)))
         new-bindings)))))

(define (project-substitution-bindings vars bindings)
  (update-binding 
   'constraint-substitutions 
   (project-substitutions vars (get-binding 'constraint-substitutions bindings))
   bindings))

(define (merge-substitution-bindings vars old-bindings new-bindings)
  (update-binding 'constraint-substitutions
		  (append
		   (project-substitutions
		    vars (get-binding 'constraint-substitutions new-bindings))
		   (get-binding 'constraint-substitutions old-bindings))
		  new-bindings))

;; what about Expressions??
(define (substituted-subselect-vars vars bindings)
  (filter sparql-variable?
	  (map (lambda (var)
		 (if (equal? var '*) var
		     (current-substitution var bindings)))
	       vars)))

(define renaming-dependencies (make-parameter '()))

(define rename-constraint-rules
  `((,symbol? . ,rw/copy)
    ((CONSTRUCT) . ,rw/remove)
    (,annotation? 
     . ,(lambda (block bindings)
          (match block
            ((`@Annotation label key) (values (list block) bindings))
            ((`@Annotation label key var)
             (let-values (((rw new-bindings) (new-substitutions (list var) bindings)))
               (values `((@Annotation ,label ,key ,@rw)) new-bindings))))))
    ((@SubSelect)
     . ,(lambda (block bindings) 
          (match block
	    ((`@SubSelect (label . vars) . rest)
             (let ((subselect-vars (extract-subselect-vars vars)))
             (let-values (((rw new-bindings)
			   (rewrite (get-child-body 'WHERE rest) (project-substitution-bindings subselect-vars bindings))))
               (let ((merged-bindings (merge-substitution-bindings subselect-vars bindings new-bindings)))
               (if (nulll? rw)
                   (values '() merged-bindings)
                   (values `((@SubSelect 
			      (,label ,@(substituted-subselect-vars vars merged-bindings))
			      ,@(replace-child-body 'WHERE rw rest)))
                           merged-bindings)))))))))
    ((WHERE)
     . ,(lambda (block bindings)
	  (parameterize ((renaming-dependencies (constraint-dependencies block bindings)))
	    (let-values (((rw new-bindings)
			  (rewrite (cdr block)
				   (update-binding
				    'dependencies (constraint-dependencies block bindings) 
				    bindings))))
	      (let ((new-bindings (update-renamings new-bindings)))
	      (if (fail? rw)
		  (values `((WHERE #f)) new-bindings)
                  (values `((WHERE ,@rw)) new-bindings)))))))

    ((UNION) . ,rw/union)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph . rest)
             (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
	       (values
		(if (nulll? (cdr rw)) '()
                    `((GRAPH ,(substitution-or-value graph new-bindings)
                             ,@(cdr rw))))
                new-bindings))))))
    (,triple? . ,rename-constraint-triple)
    ;; ((VALUES) ;; what about new-bindings (new subs)?
    ;;  . ,(lambda (block bindings)
    ;;       (match block
    ;;         ((`VALUES vars . vals)
    ;;          (let ((renamed-vars 
    ;;                 (if (pair? vars)
    ;;                     (map (cut current-substitution <> bindings) vars)
    ;;                     (current-substitution vars bindings))))
    ;; 	       (values
    ;; 		(list (simplify-values (current-substitution-recursive vars bindings) vals bindings))
    ;; 		bindings))))))
    ((VALUES) ;; what about new-bindings (new subs)?
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (new-substitutions block bindings)))
	    (match rw
	      ((`VALUES vars . vals)
	       (values
		(list (simplify-values vars vals bindings))
		new-bindings))))))

    ((FILTER) 
     . ,(lambda (block bindings) ;; what about new-bindings (new subs)?
	  (let-values (((rw new-bindings) (new-substitutions block bindings)))
	    (validate-filter rw new-bindings))))
    ((BIND) 
     . ,(lambda (block bindings) ;; what about new-bindings (new subs)?
	  (let-values (((rw new-bindings) (new-substitutions block bindings)))
	    (match rw
	      ((`BIND (`AS exp var))
	       (if (sparql-variable? var)
		   (values (list rw) new-bindings)
		   (abort
		    (make-property-condition
		     'exn
		     'message (format "Invalid constrained BIND form: '~A'" 
				      (write-sparql rw))))))))))
    (,quads-block? . ,rw/quads)
    (,list? . ,rw/list)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Renaming dependencies
(define (constraint-dependencies constraint-where-block bindings)
  (get-dependencies 
   (list constraint-where-block) 
   (map car (dependency-substitutions)))) ;; (get-binding 'dependency-substitutions bindings))))

(define (get-dependencies query bound-vars)
  (rewrite query '() (dependency-rules bound-vars)))
    
;; (define get-dependencies (memoize get-dependencies*))

(define (minimal-dependency-paths source? sink? dependencies)
  (let-values (((sources nodes) (partition (compose source? car) dependencies)))
    (let loop ((paths (join (map (match-lambda ((head . rest) (map (lambda (r) (list r head)) rest)))
                                 sources)))
               (finished-paths '()))
      (if (null? paths) finished-paths
          (let* ((path (car paths))
                 (head (car path))
                 (neighbors (remove (lambda (n) (member n path)) ;; remove loops
                                     (delete head (or (alist-ref head nodes) '())))))
            (if (or (sink? head)
                    (null? neighbors))
                (loop (cdr paths) (cons path finished-paths))
                (loop (append (cdr paths) 
                              (filter values
                                      (map (lambda (n) 
                                             (and (not (source? n)) (cons n path)))
                                           neighbors)))
                      finished-paths)))))))
                                  
(define (concat-dependency-paths paths)
  (let loop ((paths paths) (dependencies '()))
    (if (null? paths)
        (map (lambda (pair) 
               (cons (car pair) (delete-duplicates (cdr pair))))
             dependencies)
        (match (reverse (car paths))
          ((head . rest)
           (loop (cdr paths)
                 (fold (lambda (var deps)
                         (alist-update var (cons head (or (alist-ref var deps) '())) deps))
                       dependencies
                       rest)))))))

(define constraint-graph (make-parameter #f))

;; To do: better use of return values & bindings
(define (dependency-rules bound-vars)
  `((,triple? 
     . ,(lambda (triple bindings)
          
         (let* ((dependencies (or (get-binding 'dependency-paths bindings) '()))
                (bound? (lambda (var) (member var bound-vars)))
                (vars* (filter sparql-variable? triple))
                (vars (if (equal? vars* bound-vars)
                          vars*
                          (cons (constraint-graph) vars*)))
                (update-dependency-var
                 (lambda (var deps) 
                   (alist-update 
                    var (delete-duplicates
                         (append (delete var vars)
                                 (or (alist-ref var deps) '())))
                    deps))))
           (let-values (((bound unbound) (partition bound? vars)))
             (values
              (list triple)
              (update-binding
               'dependency-paths
               (fold update-dependency-var dependencies vars)
               bindings))))))
    (,annotation? . ,rw/copy)
    ((GRAPH) . ,(lambda (block bindings)
                   (match block
                     ((`GRAPH graph . rest)
                      (parameterize ((constraint-graph graph))
                        (rewrite rest 
                                 (cons-binding graph 'constraint-graphs bindings)))))))
     ((UNION) . ,rw/union)
     ((OPTIONAL) . ,rw/quads)
     ((VALUES)
      . ,(lambda (block bindings)
           (match block
             ((`VALUES vars . vals)
              (if (symbol? vars)
                  ;; actually depends on other VALUES singletons
                  (values '() bindings)
                  ;; abstract this!
                  (let* ((dependencies (or (get-binding 'dependency-paths bindings) '()))
                         (bound? (lambda (var) (member var bound-vars)))
                         (vars (filter sparql-variable? vars)))
                    (let-values (((bound unbound) (partition bound? vars)))
                      (values
                       '()
                       (update-binding
                        'dependency-paths
                        (fold (lambda (var deps) ; same function as above?
                                (alist-update 
                                 var (delete-duplicates
                                      (append (delete var vars)
                                              (or (alist-ref var deps) '())))
                                 deps))
                              dependencies
                              vars)
                        bindings)))))))))
     ((BIND)
      . ,(lambda (block bindings)
	   (match block
	     ((`BIND (`AS exp var))
	      (values '()
		      (fold-binding var 'dependency-paths
				    (lambda (var paths-list)
				      (fold (lambda (other-var paths)
					      (alist-update-proc other-var
					      		 (lambda (path)
					      		   (cons var (or path '())))
					      		 paths))
					    paths-list
					    (filter sparql-variable? (flatten exp))))
				    '()
				    bindings))))))

		      
     ((@SubSelect)
      . ,(lambda (block bindings)
           (match block
             ((`@SubSelect ((or `SELECT `|SELECT DISTINCT| `|SELECT REDUCED|) . vars) . rest)
              (rewrite (get-child-body 'WHERE rest) bindings)))))
     ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let* ((paths (get-binding 'dependency-paths new-bindings))
                   (source? (lambda (var) (member var bound-vars)))
                   (sink? (lambda (x) (member x (get-binding/default 'constraint-graphs new-bindings '())))))
              (values
               (concat-dependency-paths (minimal-dependency-paths source? sink? paths))
               bindings)))))
     ((FILTER) . ,rw/copy) ;; ??
     (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Functional Properties (and other optimizations)
(define (apply-optimizations block)
  (if (nulll? block) '()
      (let-values (((rw new-bindings) (rewrite (list block) '() optimization-rules)))
        (if (equal? rw '(#f))
            (error (format "Invalid query block (optimizations):~%~A" (write-sparql block)))
            (car (filter quads? rw))))))

(define fprops (make-parameter '((props) (subs))))

(define (replace-fprop elt)
  (if (sparql-variable? elt)
      (let ((subs (alist-ref 'subs (fprops))))
        (or (alist-ref elt subs) elt))
        elt))

(define (subs? exp)
  (and (pair? exp)
       (equal? (car exp) '*subs*)))

(define quads? (compose not subs?))

(define (append-subs rw)
  (let-values (((subs quads) (partition subs? rw)))
    (let* ((top-subs (alist-ref 'subs (fprops)))
          (new-subs (delete-duplicates (filter pair? (append top-subs (join (map second subs)))))))
      (if (null? new-subs) quads
          (cons `(*subs* ,new-subs)
                (group-graph-statements (reorder quads))))))) ; abstract this!

(define (collect-fprops block #!optional rec?)
  (let-values (((subs quads) (partition subs? block)))
    (let loop ((quads quads)
               (props (alist-ref 'props (fprops)))
               (subs (append (join (map second subs)) (alist-ref 'subs (fprops)))))
    (cond ((null? quads) `((props . ,props) (subs . ,subs)))
          ((triple? (car quads))
	   (match (car quads)
	     ((s p o) 
	      (if (rdf-member p (*functional-properties*))
		  (let ((current-value (cdr-when (assoc `(,s ,(a->rdf:type p)) props))))
		    (cond ((not current-value)
			   (loop (cdr quads) (cons `((,s ,(a->rdf:type p)) . ,o) props) subs))
			  ((and (sparql-variable? current-value)
				(not (sparql-variable? o)))
			   (loop (cdr quads) (cons `((,s ,(a->rdf:type p)) . ,o) props) 
				 (assoc-update current-value o subs)))
			  ((rdf-equal? o current-value) (loop (cdr quads) props subs))
			  ((sparql-variable? o)
			   (loop (cdr quads) props 
				 (assoc-update o current-value subs)))
			  (else #f)))
		  (loop (cdr quads) props subs)))))
          ((and (not rec?) (graph? (car quads)))
           (let ((inner (collect-fprops (cddr (car quads)) #t)))
             (if inner (loop (cdr quads) (append (alist-ref 'props inner) props) (append (alist-ref 'subs inner) subs)) #f)))
          (else (loop (cdr quads) props subs))))))

(define collect-level-quads-rules
  `(((GRAPH)
     . ,(lambda (block bindings)
          (match block
           ((`GRAPH graph . rest)
            (values
             (map (lambda (triple)
                    (cons graph triple))
                  (filter triple? rest)) ; not quite right; what about GRAPH ?g1 { GRAPH ?g2 { s p o } } ?
             bindings)))))
    ((VALUES BIND FILTER) . ,rw/copy)
    (,list? . ,rw/remove)))

(define (collect-level-quads block)
  (rewrite block '() collect-level-quads-rules))

(define this-level-quads (make-parameter '()))

(define last-level-quads (make-parameter '()))

(define (optimize-list block bindings)
  (let ((saved-llq (last-level-quads)) ; a bit... awkward
        (saved-tlq (this-level-quads))) 
  (parameterize ((fprops (collect-fprops block))
                 (last-level-quads (this-level-quads))
                 (this-level-quads (append (last-level-quads)  (collect-level-quads block))))
     (if (fprops)
        (let-values (((rw new-bindings) (rw/list (filter quads? block) bindings)))
          (cond ((fail? rw) (values (list #f) new-bindings)) ; duplicate #f check... which one is correct?
                ((or (equal? (map (cut filter quads? <>) rw)
                             (list (filter quads? block)))
                     (equal? rw '(#f)))
                 (values rw new-bindings))
                (else (let ((fp (fprops))) ; ?
                        (parameterize ((fprops (collect-fprops rw)) 
                                       (this-level-quads saved-tlq)
                                       (last-level-quads saved-llq))
                          (let-values (((rw2 nb2) (rewrite rw new-bindings)))
                            (values (append-subs rw2) nb2)))))))
        (values (list #f) bindings))) ) )

(define optimizations-graph (make-parameter #f))

(define optimization-rules
  `(((@UpdateUnit @QueryUnit @Update @Query) . ,rw/continue)
    ((@Prologue @Dataset GROUP |GROUP BY| LIMIT ORDER |ORDER BY| OFFSET) . ,rw/copy)
    (,select? . ,rw/copy)
    (,annotation? 
     . ,(lambda (block bindings)
          (match block
            ((`@Annotation `access key var) 
             (values `((@Annotation access ,key ,(replace-fprop var))) bindings))
            (else (values (list block) bindings)))))

    ((@SubSelect)     ; when can we make a direct call to the db?
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rw/subselect block bindings)))
            (values rw new-bindings))))
    ((*subs*) . ,rw/copy)
    ((VALUES) 
     . ,(lambda (block bindings)
          (if (member block (last-level-quads))
              (values '() bindings)
              (match block
                     ((`VALUES vars . vals)
                      (values
                       (list (simplify-values (map replace-fprop vars) vals bindings))
                       bindings))))))
    ((FILTER) 
     . ,(lambda (block bindings)
          (if (member block (last-level-quads))
              (values '() bindings)
              (let ((subs (alist-ref 'subs (fprops))))
                (validate-filter (replace-variables block subs) bindings)))))
    ((BIND) 
     . ,(lambda (block bindings)
          (if (member block (last-level-quads))
              (values '() bindings)
              (values (list block) bindings)))) ; enough?
    ((GRAPH)
     . ,(lambda (block bindings)
          (parameterize ((optimizations-graph (second block)))
            (let-values (((rw new-bindings) (rewrite (cddr block) bindings)))
              (let ((rw (delete-duplicates rw)))
                (if (nulll? rw) (values '() new-bindings)
                    (values `((GRAPH ,(second block) ,@rw)) new-bindings)))))))
    ((UNION) ;; . ,rw/union)
     . ,(lambda (block bindings)
          ;; (let ((rw (filter values (map (cut rewrite <> bindings) (map list (cdr block)))) ))
          (let ((rw (filter values (map (cut optimize-list <> bindings) (cdr block)))))
            (let* ((nss (map second (map car (filter pair? (map (cut filter subs? <>) rw)))))
                   (new-subs (if (null? nss) '() (apply lset-intersection equal? nss)))
                   (new-blocks (filter (compose not fail?)  (map (cut filter quads? <>) rw))) ; * !!
                   (new-subs-block (if (null? new-subs) '() `((*subs* ,new-subs)))))
            (case (length new-blocks)
              ((0)  (values (list #f) bindings)) ;; ?? (filter (compose not fail?)  ...
              ((1)  (values (append new-subs-block (caar new-blocks)) bindings)) ; * caar?
              (else (values (append new-subs-block `((UNION ,@(join new-blocks)))) bindings))))))  ) ; ? join
    ((WHERE)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (optimize-list (cdr block) bindings)))
	    (if (fail? rw)
		(abort
		 (make-property-condition
		  'exn
		  'message (format "Invalid query: functional property conflict.")))
		(values `((WHERE ,@(join (filter quads? rw)))) new-bindings)))))
    (,quads-block? 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (optimize-list (cdr block) bindings)))
            (cond ((fail? rw) (values (list #f) new-bindings))
                  ((nulll? rw) (values '() new-bindings))
                  (else
                   (values `((,(car block) ,@(join (filter quads? rw))))
                           new-bindings))))))
    (,triple? 
     . ,(lambda (triple bindings)
          (if (member (cons (optimizations-graph) triple) (last-level-quads))
              (values '() bindings)
              (values (list (map replace-fprop triple)) bindings))))
    (,list? . ,optimize-list)
    (,symbol? . ,rw/copy)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instantiation
;; for INSERT and INSERT DATA
;; To do: expand namespaces before checking
(define (instantiate where-block triples)
  (if (nulll? where-block) where-block
      (let ((where-block (expand-graphs where-block)))
        (let-values (((rw new-bindings)
                      (rewrite (list where-block) '() (get-instantiation-matches-rules triples))))
          (if (fail? rw)
              (error "Invalid query")
              (let-values (((rw2 _) (rewrite rw new-bindings instantiation-union-rules) ))
                (car rw2)))))))

;; matches a triple against a list of triples, and if successful returns a binding list
;; ex: (match-triple '(?s a dct:Agent) '((<a> <b> <c>) (<a> a dct:Agent)) => '((?s . <a>))
;; but should differentiate between empty success '() and failure #f
(define (match-triple triple triples #!optional success?)
  (if (nulll? triples) 
      (and success? '())
      (let loop ((triple1 triple) (triple2 (car triples)) (match-binding '()))
        (cond ((null? triple1) 
               (if (null? match-binding)
                   (match-triple triple (cdr triples) #t) ; success
                   (cons match-binding (match-triple triple (cdr triples) #t))))
              ((and (sparql-variable? (car triple1))
                    (not (equal? (car triple1) (car triple2)))) ; no substitutions '(?s . ?s)
               (loop (cdr triple1) (cdr triple2) (cons (cons (car triple1) (car triple2)) match-binding)))
              ((rdf-equal? (car triple1) (car triple2))
               (loop (cdr triple1) (cdr triple2) match-binding))
              (else (match-triple triple (cdr triples) success?))))))

(define (instantiate-triple triple binding)
  (let loop ((triple triple) (new-triple '()))
    (cond ((null? triple) (reverse new-triple))
          ((sparql-variable? (car triple))
           (loop (cdr triple) (cons (or (alist-ref (car triple) binding) (car triple)) new-triple)))
          (else (loop (cdr triple) (cons (car triple) new-triple))))))

(define (expand-instantiation graph triple match-binding)
  (let ((instantiation (instantiate-triple triple match-binding)))
    (list `((GRAPH ,graph ,triple))
	  (if (null? (filter sparql-variable? instantiation))
	      '()
	      `((GRAPH ,graph ,instantiation)))
	  match-binding)))

;; Given a list of triples from the INSERT or INSERT DATA block,
;; accumulates matched instantiations in the binding 'instantiated-quads
;; in the form '(matched-quad instantiated-quad match-binding)
;; or '(matched-quad () match-binding) if the instantiated quad is instantiated completely.
;; To do: group matches by shared variables (i.e., (map car match-binding) )
;; To do: abstract out matching for more complex quads patterns
(define (get-instantiation-matches-rules triples)
  `(((@SubSelect) . ,rw/subselect)
    (,annotation? . ,rw/copy)
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             ;; ;;(if (member triple triples)
             ;;     (values `((*graph* . ,graph)) bindings)
             (let ((match-bindings (match-triple triple triples)))
               (cond ((not match-bindings) (values (list block) bindings))
                     ((null? match-bindings)  (values `((*graph* . ,graph)) bindings))
                     (else (values `((*graph* . ,graph))
                                   (fold-binding (map (cut expand-instantiation graph triple <>) match-bindings)
                                                 'instantiated-quads 
                                                 append '() ; (compose delete-duplicates 
                                                 bindings)))))))))
    ;; ((OPTIONAL) . ,rw/quads)
    ((UNION)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
            (let ((new-blocks (filter values rw))) ; (compose not fail?) 
              (case (length new-blocks)
                ((0)  (values (list new-blocks) new-bindings))
                ((1)  (values `((OPTIONAL ,@(car new-blocks))) new-bindings))
                (else (values `((UNION ,@new-blocks)) new-bindings)))))))
    (,quads-block? 
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (rewrite (list (cdr block)) bindings)))
            (if (fail? rw) (values (list #f) new-bindings)
                (values `((,(car block) ,@(join rw))) new-bindings)))))
    ((FILTER)
     . ,(lambda (block bindings)
    	  (match block
    	    ((`FILTER (`|NOT EXISTS| . quads)) ; way too specific
    	     (let ((not-triples (expand-triples quads))) ; generalize this for real quads!
	       (let ((trs
		      (remove 
		       null?
		       (map (lambda (ntriple)
			      (let ((match-bindings (match-triple ntriple triples)))
				(cond ((null? match-bindings) '())
				      ((not match-bindings) ntriple)
				      (else '()))))  ; constraint?
			    not-triples))))
		 (values (if (null? trs) (list #f)
			     `((FILTER (|NOT EXISTS| ,@trs))))
			 bindings))))
    	    (else 
	     (values (list block) bindings)))))
    ((VALUES FILTER BIND) . ,rw/copy) ;; ???
    (,symbol? . ,rw/copy)
    (,nulll? . ,rw/remove)
    (,list? 
     . ,(lambda (block bindings)
	  (let-values (((rw new-bindings) (rw/list block bindings)))
            ;; Remove blocks that only contain VALUES and FILTER statements.
	    ;; Assumes length = 1. Is this warranted?
            ;; Only handles 1 VALUES statement.
            ;; 'instantiated-values SHOULD be used as following:
            ;; - separate alists for each VALUES tuple
            ;; - graphs kept separately, and instantiated only if the corresponding VALUES is used
            ;; and what about the filters?
            ;; and can/should this be abstracted & used elsewhere?
            (cond ((nulll? rw)
                   (values '() new-bindings))
                  ((fail? rw) (values (list #f) new-bindings))
                  (else
                   (let loop ((statements (car rw)) 
                              (filter-statements '()) (values-statements '()) 
                              (graphs '()) (quads '()))
                     (if (null? statements)
			 ;; ! really approximate...
                         ;; (if (and #f (null? quads) (null? filter-statements) (pair? values-statements))
                         ;; ;; (if (and (null? quads) (pair? values-statements))
                         ;;     (match (car values-statements) ;; ** only 1!!
                         ;;       ((`VALUES vars . vals)
                         ;;        (let ((new-instantiated-values
                         ;;               (if (pair? vars)
                         ;;                   (map reverse
                         ;;                        (fold (lambda (n r) (map cons n r))
                         ;;                              (map list vars)
                         ;;                              vals))
                         ;;                   `((,vars . ,vals)))))
                         ;;          (values '()
                         ;;                  (update-instantiated-values new-instantiated-values new-bindings)))))
                             (let ((rw-non-graphs (filter (not-node? '*graph*) (car rw))))
                               (values (if (null? rw-non-graphs) '() (list rw-non-graphs))
                                       (if (null? graphs)
                                           new-bindings
                                           (fold-binding graphs 'instantiated-graphs append-unique '() new-bindings))))  ;)
                         (case (caar statements)
                           ((*graph*) (loop (cdr statements) filter-statements
                                            values-statements (cons (cdar statements) graphs) quads))
                           ((FILTER) (loop (cdr statements) (cons (car statements) filter-statements) 
                                           values-statements graphs quads))
                           ((VALUES) (loop (cdr statements) filter-statements 
                                           (cons (car statements) values-statements) graphs quads))
                           ((BIND) (loop (cdr statements) filter-statements values-statements graphs quads)) ;; ??
                           (else (loop (cdr statements) filter-statements
                                       values-statements graphs (cons (car statements) quads)))))))))))))

  (define (match-instantiated-quads quad bindings)
    (filter values
            (map (match-lambda
		 ((a b binding)
		  (let ((vars (map car binding)))
		    (and (any values (map (cut member <> vars) quad))
			 (list a b binding)))))
                 (get-binding/default 'instantiated-quads bindings '()))))

(define instantiation-union-rules
  `(((@SubSelect) 
     . ,(lambda  (block bindings)
          (match block
            ((`@SubSelect (label . vars) . rest)
             (let-values (((rw new-bindings)
			   (rewrite (get-child-body 'WHERE rest) 
				    (subselect-bindings vars bindings))))
               (values
                `((@SubSelect (,label ,@vars)
                              ,@(replace-child-body 'WHERE rw rest)))
                (merge-subselect-bindings vars new-bindings bindings)))))))
    ((WHERE OPTIONAL MINUS) . ,rw/quads)
    (,annotation? . ,rw/copy)
    ((UNION)
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rewrite (cdr block) bindings)))
	    (let ((new-blocks (filter values rw)))
	      (case (length new-blocks)
		((0)  (values '() new-bindings))
                ((1)  (values new-blocks new-bindings))
		(else (values `((UNION ,@new-blocks)) new-bindings)))))))
    ((GRAPH) 
     . ,(lambda (block bindings)
          (match block
            ((`GRAPH graph triple)
             (let ((matching-quads (match-instantiated-quads (cons graph triple) bindings)))
               (if (null? matching-quads)
                   (values (list block) bindings)
		   (union-over-instantiation block matching-quads bindings)))))))
    (,symbol? . ,rw/copy)
    (,nulll? . ,rw/remove)
    ((VALUES). ,rw/copy) ;; ??
    ((*graph*) . ,rw/remove)    ;; can we remove this?
    ((FILTER) ;; ??
     . ,(lambda (block bindings)
          (values (list block) bindings)))
    (,list? 
     . ,(lambda (block bindings)
          (let-values (((rw new-bindings) (rw/list block bindings)))
            (values (list (group-graph-statements (car rw))) new-bindings))))
    ))

;; didn't I already do this somewhere else?
;; and at least do it efficiently...
;; (define (update-instantiated-values kvs bindings)
;;   (if (null? kvs) bindings
;;       (let ((key (caar kvs)) (vals (cdar kvs)))
;;         (fold-binding key 'instantiated-values
;;                       (lambda (key ivals)
;;                         (alist-update key (delete-duplicates (append vals (or (alist-ref key ivals) '()))) ivals))
;;                       '()
;;                       (update-instantiated-values (cdr kvs) bindings)))))

(define (union-over-instantiation block matching-quads bindings)
  (match block
    ((`GRAPH graph triple)
     (match (car matching-quads)
       ((matched-quad-block instantiated-quad-block binding)
        (rewrite
         `((UNION (,block ,@matched-quad-block)
                  ((GRAPH ,graph ,(instantiate-triple triple binding))
                   ,@instantiated-quad-block)))
         (update-binding 'instantiated-quads (cdr matching-quads) bindings)
         instantiation-union-rules))))))

(define (group-graph-statements statements)
  (let loop ((statements statements) (graph #f) (statements-same-graph '()))
    (cond ((nulll? statements) 
           (if graph `((GRAPH ,graph ,@statements-same-graph)) '()))
          (graph
           (match (car statements)
             ((`GRAPH graph2 . rest) 
              (if (equal? graph2 graph)
                  (loop (cdr statements) graph (append-unique statements-same-graph rest))
                  (cons `(GRAPH ,graph ,@statements-same-graph)
                        (loop (cdr statements) graph2 rest))))
             (else (cons `(GRAPH ,graph ,@statements-same-graph)
                         (cons (car statements)
                               (loop (cdr statements) #f '()))))))
          (else
           (match (car statements)
             ((`GRAPH graph . rest) 
              (loop (cdr statements) graph rest))
             (else (cons (car statements) (loop (cdr statements) #f '()))))))))

(define clean (compose delete-duplicates group-graph-statements reorder))

;; ;; This isn't quite correct. Should be done per-tuple, right?
;; (define (instantiate-values block substitutions graphs)
;;   (let ((substitutions (if (or #t (not graphs)) ; b/c graphs shouldn't be variables...??
;;                            substitutions
;;                            (fold (lambda (graph substitutions)
;;                                    (alist-update-proc graph (lambda (graphs) (cons graph (or graphs '()))) substitutions))
;;                                  substitutions
;;                                  graphs))))
;;     (rewrite block '() (instantiate-values-rules substitutions))))

;; (define (instantiate-values-rules substitutions)
;;   `(((GRAPH) 
;;      . ,(lambda (block bindings)
;;           (match block
;;             ((`GRAPH graph . triples)
;;              (let ((new-graphs (alist-ref graph substitutions)))
;;                (if new-graphs
;;                    (let-values (((rw _) (rewrite triples)))
;;                      (values (map (lambda (new-graph)
;;                                     `(GRAPH ,new-graph ,@triples))
;;                                   new-graphs)
;;                              bindings))
;;                    (values (list block) bindings)))))))
;;     (,triple? 
;;      . ,(lambda (triple bindings)
;;           (values
;;            (let loop ((triple triple) (k list))
;;              (if (null? triple) (k '())
;;                  (let ((subs (alist-ref (car triple) substitutions)))
;;                    (if subs
;;                        (loop (cdr triple)
;;                              (lambda (rest)
;;                                (join
;;                                 (map
;;                                  (lambda (s) (k (cons s rest)))
;;                                  subs))))
;;                        (loop (cdr triple)
;;                              (lambda (rest)
;;                                (k (cons (car triple) rest))))))))
;;            bindings)))

;;     (,list? . ,rw/list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Top
(define (rew query)
  (handle-exceptions exn
      (case exn
        ((optimizations) (error (format "Invalid query (opt):~%~A" (write-sparql query))))
        (else (error (format "Invalid query:~%~A" (write-sparql query)))))
    (rewrite-query query top-rules)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deltas
;; To do: refactor this using rewrite rules, and clean up.
(define (construct-statement triple #!optional graph)
  (match triple
    ((s p o)
     `((,s ,p ,o)
       ,@(splice-when
          (and graph
               `((,s ,p ,graph)
                 (rewriter:Graphs rewriter:include ,graph))))))))

(define (construct-statements statements)
  (and statements
       `(CONSTRUCT
         ,@(join
            (map (lambda (statement)
                   (match statement
                     ((`GRAPH graph . triples)
                      (join (map (cut construct-statement <> graph)
                                 (join (map expand-triple triples)))))
                     (else (join (map (cut construct-statement <>) (expand-triple statement))))))
                 statements)))))

(define (query-constructs query)
  (and (update-query? query)
       (let loop ((queryunits (alist-ref '@UpdateUnit query))
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

(define (merge-delta-triples-by-graph label quads)
  (let ((car< (lambda (a b) (string< (car a) (car b)))))
    (map (lambda (group)
           `(,(string->symbol (caar group))
             . ((,label 
                 . ,(list->vector
                     (map cdr group))))))
         (group/key car (sort quads car<)))))

(define (expand-delta-properties s propertyset graphs)
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

(define (merge-deltas-by-graph delete-deltas insert-deltas)
  (list->vector
   (map (match-lambda
          ((graph . deltas)
           `((graph . ,(symbol->string graph))
             (delta . ,deltas))))
        (let loop ((ds delete-deltas) (diffs insert-deltas))
          (if (null? ds) diffs
              (let ((graph (caar ds)))
                (loop (cdr ds)
                      (alist-update 
                       graph (append (or (alist-ref graph diffs) '()) (cdar ds))
                       diffs))))))))

(define (run-delta query label)
  (parameterize ((*query-unpacker* string->json))
    (let* ((gkeys '(http://mu.semte.ch/graphs/Graphs http://mu.semte.ch/graphs/include))
           (results (sparql-select (write-sparql query)))
           (graphs (map (cut alist-ref 'value <>)
                        (vector->list
                         (or (nested-alist-ref* gkeys results) (vector)))))
           (D (lambda (tripleset)
                (let ((s (car tripleset))
                      (properties (cdr tripleset)))
                  (and (not (equal? s (car gkeys)))
                       (join (map (cut expand-delta-properties s <> graphs) properties)))))))
      (merge-delta-triples-by-graph
       label (join (filter values (map D results)))))))

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

(define (notify-deltas query)
  (let ((queries-deltas (run-deltas query)))
    (thread-start!
     (make-thread
      (lambda ()
        (for-each (lambda (query-deltas)
                    (let ((deltastr (json->string query-deltas)))
                      (for-each (lambda (subscriber)
                                  (log-message "~%Deltas: notifying ~A~% " subscriber)
                                  (notify-subscriber subscriber deltastr))
                                *subscribers*)))
                  queries-deltas))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implentation
(define (virtuoso-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (log-message "Virtuoso error: ~A" exn)
    (when body
      (log-message "~%==Virtuoso Error==~% ~A ~%" body))
    (when response
      (log-message "~%==Reason==:~%~A~%" (response-reason response)))
    (abort 'virtuoso-error)))

(define (rewriter-error exn)
  (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                      ((condition-property-accessor 'server-error 'response) exn)))
        (body (or ((condition-property-accessor 'client-error 'body) exn)
                  ((condition-property-accessor 'server-error 'body) exn))))
    (log-message "~%" exn)
    (when body
      (log-message "~%==Rewriter Error==~% ~A ~%" body))
    (when response
      (log-message "~%==Reason==:~%~A~%" (response-reason response)))
    (abort exn)))

;; better : parameterize body and req-headers, and define functions...
(define $query (make-parameter (lambda (q) #f)))
(define $body (make-parameter (lambda (q) #f)))
(define $mu-session-id (make-parameter #f))
(define $mu-call-id (make-parameter #f))

(define (proxy-query rewritten-query-string endpoint)
  (with-input-from-request 
   (make-request method: 'POST
                 uri: (uri-reference endpoint)
                 headers: (headers
                           '((Content-Type application/x-www-form-urlencoded)
                             (Accept application/sparql-results+json)))) 
   `((query . , (format #f "~A" rewritten-query-string)))
   read-string))

(define (parse q) (parse-query q))

(define (log-headers) (log-message "~%==Received Headers==~%~A~%" (*request-headers*)))

(define (log-received-query query-string query)
  (log-headers)
  (log-message "~%==Rewriting Query==~%~A~%" query-string)
  ;; (log-message "~%==With Constraint==~%~A~%" (write-sparql (if (procedure? (*constraint*)) ((*constraint*)) (*constraint*))))
  (log-message "~%==Parsed As==~%~A~%" (write-sparql query)))

(define (log-rewritten-query rewritten-query-string)
  (log-message "~%==Rewritten Query==~%~A~%" rewritten-query-string))

(define (log-results result)
  (log-message "~%==Results==~%~A~%" (substring result 0 (min 1500 (string-length result)))))

(define (rewrite-call _)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (query (parse query-string)))
         
    (log-received-query query-string query)

    (let-values (((rewritten-query bindings)
                  (parameterize (($query $$query) ($body $$body))
                    (handle-exceptions exn 
                        (begin (log-message "~%==Rewriting Error==~%") 
                               (log-message "~%~A~%" ((condition-property-accessor 'exn 'message) exn))
                               (print-error-message exn (current-error-port))
                               (print-call-chain (current-error-port))
                               (abort exn))
                      (rewrite-query query top-rules)))))
      (let ((rewritten-query-string (write-sparql rewritten-query)))
        
        (log-rewritten-query rewritten-query-string)

        (handle-exceptions exn 
            (virtuoso-error exn)

          (when (and (update-query? rewritten-query) (*send-deltas?*))
            (notify-deltas rewritten-query))

          (plet-if (not (update-query? query))
                   ((potential-graphs (handle-exceptions exn
                                          (begin (log-message "~%Error getting potential graphs or annotations: ~A~%" exn) 
                                                 #f)
                                        (cond ((*calculate-potential-graphs?*)
                                               (get-all-graphs rewritten-query))
                                              ((*calculate-annotations?*)
                                               (get-annotations rewritten-query))
                                              (else #f))))
                    ((result response)
                     (let-values (((result uri response)
                                   (proxy-query (add-prefixes rewritten-query-string)
                                                (if (update-query? query)
                                                    (*sparql-update-endpoint*)
                                                    (*sparql-endpoint*)))))
                       (close-connection! uri)
                       (list result response))))
                   
                   (when (or (*calculate-potential-graphs?*) (*calculate-annotations?*))
                     (log-message "~%==Potentials==~%(Will be sent in headers)~%~A~%"  potential-graphs))
                   
                   (let ((headers (headers->list (response-headers response))))
                     (log-results result)
                     (mu-headers headers)
                     result)))))))

         
          
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification
(define-rest-call 'GET '("sparql") rewrite-call)
(define-rest-call 'POST '("sparql") rewrite-call)

(define-namespace rewriter "http://mu.semte.ch/graphs/")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Test suite
(define (construct-intermediate-graph-rules graph)
  `(((@QueryUnit @Query)  . ,rw/quads)
    ((CONSTRUCT)
     . ,(lambda (block bindings)
          (values `((INSERT
                     (GRAPH ,graph ,@(cdr block))))
                  bindings)))
    (,values . ,rw/copy)))
    
;; could be abstracted & combined with select-query-rules
(define (replace-dataset-rules graph)
  `(((@QueryUnit @Query) . ,rw/quads)
    ((GRAPH)
     . ,(rw/lambda (block) (cddr block)))
    ((@Prologue @SubSelect WHERE FILTER BIND |ORDER| |ORDER BY| |LIMIT|) . ,rw/copy)
    ((@Dataset) 
     . ,(rw/lambda (block)
          (parameterize ((*default-graph* graph))
            `((@Dataset ,@(make-dataset 'FROM '() #f))))))
    (,select? . ,rw/copy)
    (,triple? . ,rw/copy)
    (,list? . ,rw/list)))

(define (test query #!optional (cleanup? #t))
  (let ((rewritten-query (rewrite-query query top-rules))
        (intermediate-graph 
         (expand-namespace
          (symbol-append '|rewriter:| (gensym 'graph)))))
    (log-message "Creating intermediate graph: ~A " intermediate-graph)
    (print
     (sparql-update
      (write-sparql 
       (rewrite-query 
        (if (procedure? (*read-constraint*)) ((*read-constraint*)) (*read-constraint*))
        (construct-intermediate-graph-rules intermediate-graph)))))

    (parameterize ((*query-unpacker* sparql-bindings))
      (let ((r1 (sparql-select (write-sparql rewritten-query)))
            (r2 (sparql-select (write-sparql (rewrite-query query (replace-dataset-rules intermediate-graph))))))
        (log-message "~%==Expected Results==~%~A" r2)
        (log-message "~%==Actual Results==~%~A" r1)
        (when cleanup?
          (sparql-update (format "DELETE WHERE { GRAPH ~A { ?s ?p ?o } }" intermediate-graph)))
        (values r1 r2)))))

(define (test-call _)
  (let* (($$query (request-vars source: 'query-string))
         (body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string (or ($$query 'query) ($$body 'query) body))
         (cleanup? (not (string-ci=? "false" (or ($$query 'cleanup) "true"))))
         (query (parse query-string)))
    (let-values (((actual expected) (test query cleanup?)))
      `((expected . ,(list->vector expected))
        (actual . ,(list->vector actual))
        (equal . ,(equal? expected actual))))))

(define-rest-call 'POST '("test") test-call)

(define (sandbox-call _)
  (let* ((body (read-request-body))
         ($$body (let ((parsed-body (form-urldecode body)))
                   (lambda (key)
                     (and parsed-body (alist-ref key parsed-body)))))
         (query-string ($$body 'query))
         (session-id (conc "\"" ($$body 'session-id) "\""))
         (replace-sid (lambda (str) 
                        (irregex-replace/all "<SESSION_ID>" str session-id)))
	 (constraint-string (replace-sid (or ($$body 'constraint) "")))
	 (read-constraint-string (replace-sid (or ($$body 'readconstraint) constraint-string)))
	 (write-constraint-string (replace-sid (or ($$body 'writeconstraint) constraint-string)))
	 (read-constraint (parse-constraint read-constraint-string))
	 (write-constraint (parse-constraint write-constraint-string))
         (query (parse query-string))
	 (fprops (map string->symbol
		      (string-split (or ($$body 'fprops) "") ", ")))
         (authorization-insert ($$body 'authorization-insert)))
    (parameterize ((*write-constraint*  write-constraint)
    		   (*read-constraint* read-constraint)
		   (*functional-properties* fprops)
		   (*constraint-prologues*
		    (append-unique
		     (all-prologues (cdr read-constraint))
		     (all-prologues (cdr write-constraint)))))
      (sparql-update "DELETE WHERE { GRAPH <http://mu.semte.ch/authorization> { ?s ?p ?o } }")
      (sparql-update "INSERT DATA { GRAPH <http://mu.semte.ch/authorization> { ~A } }" authorization-insert)
      (let* ((rewritten-query (rewrite-query query top-rules))
             (annotations (get-annotations rewritten-query))
             (qt-annotations (query-time-annotations annotations))
             (queried-annotations (query-annotations annotations rewritten-query)))
        (log-message "~%===Annotations===~%~A~%" annotations)
        (log-message "~%===Queried Annotations===~%~A~%"
                     (format-queried-annotations queried-annotations))
        `((rewrittenQuery . ,(format (write-sparql rewritten-query)))
          (annotations . ,(format-annotations qt-annotations))
          (queriedAnnotations . ,(format-queried-annotations queried-annotations)))))))

(define (format-queried-annotations queried-annotations)
  (list->vector
   (map (lambda (annotation)
          (match annotation
            ((key val)
             `((key . ,(symbol->string key))
               (var . ,(write-uri val))))
            (key `((key . ,(symbol->string key))))))
        queried-annotations)))

(define (format-annotations annotations)
  (list->vector
   (map (lambda (annotation)
          (match annotation
            ((`*values* rest) `((key . ,(format "~A" rest)))) ; fudging
            ((key var) `((key . ,(symbol->string key))
                         (var . ,(symbol->string var))))
            (key `((key . ,(symbol->string key))))))
        annotations)))

(define-rest-call 'POST '("sandbox") sandbox-call)

(define-rest-call 'POST '("proxy")
  (lambda (_)
    (let ((query (read-request-body)))
      (proxy-query (add-prefixes query)
		   (if (update-query? (parse query))
		       (*sparql-update-endpoint*)
		       (*sparql-endpoint*))))))


(define (serve-file path)
  (call-with-input-file path
    (lambda (port)
      (read-string #f port))))

(define (sandbox filename)
  (if (feature? 'docker) 
      (make-pathname "/app/sandbox/" filename)
      (make-pathname "./sandbox/" filename)))

(define-rest-call 'GET '("sandbox") (lambda (_) (serve-file (sandbox "index.html"))))

(define-rest-call 'GET '("sandbox" file)
  (rest-call
   (file)
   (serve-file (sandbox file))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load
(log-message "~%==Query Rewriter Service==")

(when (*plugin*) 
  (log-message "~%Loading plugin: ~A " (*plugin*))
  (load (*plugin*)))

(log-message "~%Proxying to SPARQL endpoint: ~A " (*sparql-endpoint*))
(log-message "~%and SPARQL update endpoint: ~A " (*sparql-update-endpoint*))

;; (if (procedure? (*constraint*))
;;     (log-message "~%with constraint:~%~A" (write-sparql ((*constraint*))))
;;     (log-message "~%with constraint:~%~A" (write-sparql (*constraint*))))

(*port* 8890)

;; (use slime)

;; (when (not (feature? 'docker))
;;   (define *swank*
;;     (thread-start!
;;      (make-thread
;;       (lambda ()
;;         (swank-server-start 4005))))))
