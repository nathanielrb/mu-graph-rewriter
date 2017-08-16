(use mini-kanren srfi-1)

(define (deto sources sinks triples fpath)
  (fresh (head)
    (membero head sources)
    (deto* sources sinks triples (list head) fpath)))

(define (deto* sources sinks triples path fpath)
  (fresh (head neighbor extended-path)
   (caro path head)
   (conso neighbor path extended-path)
   (codeto head neighbor triples)
   (rembero neighbor path path) ; no loops
   (rembero neighbor sources sources)
   (conde 
    ((membero neighbor sinks) (== extended-path fpath))
    ((deto* sources sinks triples extended-path fpath)))
   ))
   
(define (codeto a b triples)
  (conde
    ((== triples '()) (== #t #f))
    ((fresh (triple rest)
      (=/= a b)
      (conso triple rest triples)
      (conde 
        ((membero a triple)
         (membero b triple))
	((codeto a b rest )))))))

;; filtered, sparql variables only
(define triples
  '((?s ?type)
    (?rule ?type)
    (?rule ?p)
    (?name ?o)
    (?rule ?graph)))
