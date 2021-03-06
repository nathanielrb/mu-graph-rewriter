;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implentation
(define logkey (make-parameter #f))

(define (virtuoso-error exn)
  (if (or ((condition-predicate 'client-error) exn)
          ((condition-predicate 'server-error) exn))
      (let ((response (or ((condition-property-accessor 'client-error 'response) exn)
                          ((condition-property-accessor 'server-error 'response) exn)))
            (body (or ((condition-property-accessor 'client-error 'body) exn)
                      ((condition-property-accessor 'server-error 'body) exn))))
        (log-message "Virtuoso error: ~A" exn)
        (log-message "~A~%" (condition->list exn))
        (when body
              (log-message "~%==Virtuoso Error==~% ~A ~%" body))
        (when response
              (log-message "~%==Reason==:~%~A~%" (response-reason response)))
        (abort 'virtuoso-error))
      (abort exn)))

(define (rewriting-error exn)
  (log-message "~%==Rewriting Error (~A)==~%" (logkey)) 
  (log-message "~%~A~%" ((condition-property-accessor 'exn 'message) exn))
  (print-error-message exn (current-error-port))
  (print-call-chain (current-error-port))
  (abort exn))

(define (proxy-query key rewritten-query-string endpoint)
  (handle-exceptions exn
                     (virtuoso-error exn)
    (let ((t1 (current-milliseconds)))
      (let-values (((result uri response)
                    (with-input-from-request 
                     (make-request method: 'POST
                                   uri: (uri-reference endpoint)
                                   headers: (headers
                                             '((Content-Type application/x-www-form-urlencoded)
                                               (Accept application/sparql-results+json)
                                               (Connection: Keep-Alive))))
                     `((query . , (format #f "~A" rewritten-query-string)))
                     read-string)))
        (let ((t2 (current-milliseconds)))
          (debug-message "~%[~A]  Query Time: ~Ams~%" key (- t2 t1))
          (values result uri response (- t2 t1)))))))

(define (parse key q)
  (let-values (((ut1 st1) (cpu-time)))
    (let ((result (parse-query q)))
      (let-values (((ut2 st2) (cpu-time)))
        (debug-message "~%[~A]  Parse Time : ~Ams / ~Ams~%" 
                     key (- ut2 ut1) (- st2 st1))
        result))))

(define (log-headers)
  (debug-message "~%[~A]  ==Received Headers==~%~A~%" (logkey) (*request-headers*)))

(define (call-if C) (if (procedure? C) (C) C))

(define (rewrite-call)
  (lambda (_)
    (timed "Total"
      (parameterize ((logkey (gensym 'query)))
        (let ((query-string (or ((request-vars source: 'query-string) 'query)
                                (let ((body (read-request-body)))
                                  (or (let ((url-decoded-body (form-urldecode body)))
                                        (and url-decoded-body (alist-ref 'query url-decoded-body)))
                                      body)))))
          (log-headers)
          (log-message "~%[~A]  ==Rewriting Query==~%~A~%" (logkey) query-string)

          (let-values (((rewritten-query-string annotations annotations-query-strings annotations-pairs
                                                deltas-query-string bindings update?)
                        (handle-exceptions exn (rewriting-error exn)
                                           (apply-constraints-with-form-cache query-string))))

            (handle-exceptions exn (virtuoso-error exn)

              ;; (when (and update? (*send-deltas?*)) ...

              (let-values (((result uri response query-time)
                            (proxy-query (logkey)
                                         rewritten-query-string
                                         (if update?
                                             (*sparql-update-endpoint*)
                                             (*sparql-endpoint*)))))
                
                (when (*calculate-annotations?*) 
                      (log-message "~%[~A]  ==Annotations==~%~A~% " 
                                   (logkey) annotations)

                      (log-message "~%[~A]  ==Queried Annotations==~%~A~% " 
                                   (logkey) (try-safely "Getting Queried Annotations" annotations-query-strings
                                                        (and annotations-query-strings
                                                             (map (lambda (q)
                                                                    (query-annotations q annotations-pairs))
                                                                  annotations-query-strings)))))

                (let ((headers (headers->list (response-headers response))))
                  (log-message "~%[~A]  ==Results==~%~A~%" 
                               (logkey) (substring result 0 (min 1500 (string-length result))))
                  (mu-headers headers)
                  result)))))))        ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Specification
(define-rest-call 'GET '("sparql") (rewrite-call))
(define-rest-call 'POST '("sparql") (rewrite-call))

(define-namespace rewriter "http://mu.semte.ch/graphs/")












