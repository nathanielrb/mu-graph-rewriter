(use s-sparql-parser)
(load "app-refactored.scm")

(define u (parse-query
"PREFIX dc: <http://schema.org/dc/> 
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

SELECT *
 WHERE {
   ?a mu:uuid ?b . ?c mu:uuid ?e . <http://data.europa.eu/eurostat/graphs/rules/ECOICOPs> mu:title ?h
  } 

"))

(define v (parse-query
"PREFIX dc: <http://schema.org/dc/> 
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

DELETE { ?s ?p ?o }
INSERT { ?s ?p <ABC> }
 WHERE {
  ?s ?p ?o
  } 

"))
