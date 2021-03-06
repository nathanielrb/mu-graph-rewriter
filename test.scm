;; (use s-sparql s-sparql-parser mu-chicken-support matchable)

(include "app.scm")
;; (load "constraints.scm")
;; (*plugin* "plugins/realms-plugin.scm")

;; (access-log "access.log")
;; (debug-log "debug.log")

;; (*default-graph* '<http://data.europa.eu/eurostat/graphs>)

;; (*realm-id-graph* '<http://data.europa.eu/eurostat/uuid>)

;; (vhost-map `((".*" . ,handle-app)))

;; (*sparql-endpoint* "http://localhost:8890/sparql")

;; (*subscribers-file* "../config/rewriter/subscribers.json")

(define psp (compose print write-sparql))
(*rewrite-select-queries?* #t)

(define-syntax time
  (syntax-rules ()
    ((_ body ...)
     (-
      (- (cpu-time)
         (begin body ...
                (cpu-time)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; testing
(use spiffy)

;; (define-namespace skos "http://www.w3.org/2004/02/skos/core#")
;; (define-namespace eurostat "http://data.europa.eu/eurostat/")

;(*sparql-endpoint* "http://172.31.63.185:8890/sparql")


(define v (parse-query "PREFIX pre: <http://www.home.com/> 
                  PREFIX rdf: <http://www.gooogle.com/> 

WITH <http://www.google.com/>
DELETE { ?a mu:uuid ?b . ?l ?m ?o }
WHERE {
  ?s ?p ?o . ?x ?y ?z
}"))

(define u (parse-query ;; (car (lex QueryUnit err "
"PREFIX dc: <http://schema.org/dc/> 
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

DELETE WHERE {
   ?a mu:uuid ?b . ?c mu:uuid ?e . <http://data.europa.eu/eurostat/graphs/rules/ECOICOPs> mu:title ?h
  } 

"))

(define s (parse-query "PREFIX pre: <http://www.home.com/> 
                  PREFIX rdf: <http://www.gooogle.com/> 
SELECT ?a ?a
FROM <http://www.google.com/>
FROM NAMED <http://www.google.com/>  
  WHERE {
     GRAPH ?g { ?s ?p ?o }
  } 
"))

(define t (parse-query "PREFIX pre: <http://www.home.com/> 
                  PREFIX rdf: <http://www.gooogle.com/> 
SELECT ?a ?a
FROM <http://www.google.com/>
FROM NAMED <http://www.google.com/>  
  WHERE {
     OPTIONAL { ?s ?p ?o }

    {?a ?b ?c} UNION { ?d ?e ?f }
  } 
"))

(define t2 (parse-query "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

SELECT ?s
  WHERE {
    ?s a skos:Concept.
    ?s ?p ?o
  } 
"))

(define t2b (parse-query "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

INSERT DATA {
    eurostat:ECOICOP4 a skos:Concept .
    eurostat:product1 mu:category eurostat:ECOICOP4 .
  } 
"))
(print "AA")
(define t2c (parse-query "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

INSERT {
   ?product mu:category ?ecoicop .
   ?product mu:uuid \"12345\" .
  } 
WHERE {
  ?product mu:category eurostat:ECOICOP2.
  ?ecoicop mu:uuid ?id.
  FILTER( ?id = \"ecoicop1\")
}
"))

;;    eurostat:ECOICOP4 mu:uuid \"ecoicop4\" .

(define t3 (parse-query "
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

SELECT DISTINCT *
  WHERE {
    GRAPH <http://google> {
    OPTIONAL { ?s mu:category ?category }
    OPTIONAL { ?t mu:uuid ?id }
  }
  FILTER( ?s < 45)
  } 
"))
(print "BB")
(define t4 (parse-query "
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX graphs: <http://mu.semte.ch/graphs/>
PREFIX eurostat: <http://data.europa.eu/eurostat/>

INSERT {
  ?s skos:notation \"12345\"
}
  WHERE {
    ?s a skos:Concept .
  } 
"))
(print "D")
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Calls

;; (define (descendance-call relation inverse?)
;;   (rest-call (scheme-id id)

;;  (*handlers* `((GET ("test") ,(lambda (b) `((status . "success"))))
;;                (GET ("schemes") ,concept-schemes-call)

 
(define q1 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ((COUNT (DISTINCT ?uuid)) AS ?count) WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a qb:Observation. 
}
}
"))
(print "E")
(define q2 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?uuid WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a qb:Observation. 
}
} GROUP BY ?uuid OFFSET 0 LIMIT 20
"))
(print "CC")
(define q3 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"3bea4b42e69a0360905c837ac12b6515\". 
}
}
"))

(define q4 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT * WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    OPTIONAL {<http://data.europa.eu/eurostat/id/observation/2b8174ad39a111ba85c3544aa4188c5e> eurostat:amount ?amount.}
}
}

"))

(define q5 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
INSERT DATA 
{
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> a schema:Offer.
    <http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> mu:uuid \"594BA222EC3E6C0009000002\".
    <http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:description \"a bag of coke\".
}
}"))

(define q6 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT * WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    OPTIONAL {<http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:description ?description.}
OPTIONAL {<http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:gtin13 ?gtin13.}
OPTIONAL {<http://data.europa.eu/eurostat/id/offer/594BA222EC3E6C0009000002> schema:identifier ?identifier.}
}
}"))


(define q7 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
DELETE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/594BBE69EC3E6C0009000001> schema:description ?gensym0.
}
} WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        OPTIONAL {     <http://data.europa.eu/eurostat/id/offer/594BBE69EC3E6C0009000001> schema:description ?gensym0.
 }
}
};
INSERT DATA 
{
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/594BBE69EC3E6C0009000001> schema:description \"two bags of coke\".
}
}


"))

(define q8 (parse-query " PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
DELETE {

        ?s mu:uuid \"594BBE69EC3E6C0009000001\".
    ?s a schema:Offer.
    ?s schema:description ?description.
    ?s schema:gtin13 ?gtin13.
    ?s schema:identifier ?identifier.
    ?s schema:category ?category.

} WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        ?s mu:uuid \"594BBE69EC3E6C0009000001\".
    ?s a schema:Offer.
    OPTIONAL {?s schema:description ?description.}
    OPTIONAL {?s schema:gtin13 ?gtin13.}
    OPTIONAL {?s schema:identifier ?identifier.}
    OPTIONAL {?s schema:category ?category.}
}
}

"))

(define q9 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
INSERT 
{
  ?s mu:uuid ?id.
  ?s schema:description ?desc
}
WHERE {
   GRAPH <http://data.europa.eu/eurostat/temp> {
                                                ?s a schema:Offer.
                                                   ?s mu:uuid ?id.
                                                   ?s schema:description ?desc
}
}"))

(define class (parse-query "            SELECT DISTINCT ?GTINdesc ?GTIN ?ISBA ?ISBAUUID ?ESBA ?ESBAdesc ?UUID ?quantity ?unit ?training
	        FROM <http://data.europa.eu/eurostat/temp>
            FROM <http://data.europa.eu/eurostat/ECOICOP>
	        WHERE {
                ?obs eurostat:product ?offer.
                ?offer a schema:Offer;
                    semtech:uuid ?UUID;
                    schema:description ?GTINdesc;
                    schema:gtin13 ?GTIN.
                OPTIONAL {
                    ?offer schema:includesObject [
                        a schema:TypeAndQuantityNode;
                        schema:amountOfThisGood ?quantity;
                        schema:unitCode ?unit
                    ].}
               OPTIONAL {
                    ?offer schema:category ?ISBA.
                    ?ISBA semtech:uuid ?ISBAUUID.
                    }
                ?obs eurostat:classification ?ESBA.
                ?ESBA skos:prefLabel ?ESBAdesc.
                ?obs qb:dataSet ?dataset.
                ?dataset dct:publisher <http://data.europa.eu/eurostat/id/organization/publishers>.
                ?dataset dct:issued \"issueds\".
                ?obs eurostat:training ?training.
            }"))

(define sync (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

DELETE {
  GRAPH <http://data.europa.eu/eurostat/backup> { ?s ?p ?o }
  ?s ?p ?o
}
INSERT {
  ?s ?p ?o
}
WHERE {
  GRAPH <http://data.europa.eu/eurostat/backup> {
    ?s ?p ?o
  }
}
"))


(define vincents-query "   
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX schema: <http://schema.org/>
    PREFIX interval: <http://reference.data.gov.uk/def/intervals/>
    PREFIX offer: <http://data.europa.eu/eurostat/id/offer/>
    PREFIX sdmx-subject: <http://purl.org/linked-data/sdmx/2009/subject#>
    PREFIX sdmx-concept: <http://purl.org/linked-data/sdmx/2009/concept#>
    PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
    PREFIX semtech: <http://mu.semte.ch/vocabularies/core/>

    SELECT DISTINCT ?GTINdesc ?GTIN ?ISBA ?ISBAUUID ?ESBA ?ESBAdesc ?UUID ?quantity ?unit ?training
        FROM <http://data.europa.eu/eurostat/temp>
         FROM <http://data.europa.eu/eurostat/ECOICOP>
	        WHERE {
          GRAPH <http://google/temp> {                          ?obs eurostat:product ?offer.}
                ?offer a schema:Offer;
                    semtech:uuid ?UUID;
                    schema:description ?GTINdesc; 
                    schema:gtin13 ?GTIN.
                OPTIONAL {
                    ?offer schema:includesObject [
                        a schema:TypeAndQuantityNode;
                        schema:amountOfThisGood ?quantity;
                        schema:unitCode ?unit
                    ].}
                OPTIONAL {
                    ?offer schema:category ?ISBA.
                    ?ISBA semtech:uuid ?ISBAUUID.
                    }
                ?obs eurostat:classification ?ESBA.
                ?ESBA skos:prefLabel ?ESBAdesc.
                ?obs qb:dataSet ?dataset.
                ?dataset dct:publisher <http://data.europa.eu/eurostat/id/organization/59562F0BDF757B0009000002>.
                ?dataset dct:issued \"2017-06-30\"^^xsd:dateTime.
                ?obs eurostat:training ?training.
            } ORDER BY ?a ?b ?c
")

(define v (parse-query vincents-query))

(define aads-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
DELETE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/829898b55516ea087eabeac67dd8b6b1> schema:category ?s.
}
} WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        OPTIONAL {     <http://data.europa.eu/eurostat/id/offer/829898b55516ea087eabeac67dd8b6b1> schema:category ?s.
 }
}
}; 
INSERT DATA 
{
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/829898b55516ea087eabeac67dd8b6b1> schema:category <http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/021310701>.
}
}  ")

(define aads (parse-query aads-query))

(define sync (parse-query " DELETE {
  GRAPH <http://data.europa.eu/eurostat/backup> {
    ?s ?p ?o.
   }  
 }
INSERT {
  ?s ?p ?newo.
 }

WHERE {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s ?p ?o.
    BIND (IF(DATATYPE(?o) = <http://www.w3.org/2001/XMLSchema#string>, STR(?o), ?o) AS ?newo)
   }  
 }
"))

(define muclr-query (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

DELETE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        ?s mu:uuid \"595CE6AE975D3E0009000002\".
    ?s a dct:Agent.
    ?s dct:title ?name.
    ?datasets dct:publisher ?s.
}
} WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        ?s mu:uuid \"595CE6AE975D3E0009000002\".
    ?s a dct:Agent.
    OPTIONAL {?s dct:title ?name.}
    OPTIONAL {?datasets dct:publisher ?s.}
}
}"))


(define aad2 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ((COUNT (DISTINCT ?uuid)) AS ?count) WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    <http://data.europa.eu/eurostat/id/organization/595E00F71006431B15000001> ^dct:publisher ?resource. ?resource mu:uuid ?uuid.  
}
}"))


(define aad3 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
DELETE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category ?s.
}
} WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        OPTIONAL {     <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category ?s.
 }
}
}; 
INSERT DATA 
{
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category <http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/012220701>.
}
}
"))

(define aad4 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?target WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"595CC438E1D94E0009000003\". ?s ^dct:publisher/mu:uuid ?target.  
}
}"))

(define aad5 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?target WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"596779991006436CDA000001\". ?s ^dct:publisher/mu:uuid ?target.  
}
}"))


(define deltas (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
DELETE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category ?s.
}
}
INSERT {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category ?s.
} } WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
        OPTIONAL {     <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category ?s.
 }
}
}; 
INSERT DATA 
{
    GRAPH <http://data.europa.eu/eurostat/temp> {
        <http://data.europa.eu/eurostat/id/offer/7fc0214e3427c3eefeb2a26493bcb375> schema:category <http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/012220701>.
}
}
"))


(define rw (parse-query "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

DELETE {
  ?s ?pp ?oo.
 } WHERE {
{
   SELECT DISTINCT ?s
   WHERE {
     GRAPH <http://data.europa.eu/eurostat/temp> {
       ?s ?p ?o.
      }  
    }
  }
  ?s ?pp ?oo.
 }"))

(define subs (parse-query "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

DELETE {
  ?s ?pp ?oo.
 } WHERE {
   SELECT DISTINCT ?s
   WHERE {
     GRAPH <http://data.europa.eu/eurostat/temp> {
       ?s ?p ?o.
      }  
    }
 }"))

(define sync2 (parse-query " PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
 DELETE {
  ?s ?pp ?oo.
 }

WHERE {
{
   SELECT DISTINCT ?s
   WHERE {
     GRAPH <http://data.europa.eu/eurostat/temp> {
       ?s2 ?p ?o.
       ?s3 ?p ?o.
      }  
    }
  }
  ?s ?pp ?oo.
 }
"))

(define class (parse-query "
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX schema: <http://schema.org/>
    PREFIX sdmx-subject: <http://purl.org/linked-data/sdmx/2009/subject#>
    PREFIX sdmx-concept: <http://purl.org/linked-data/sdmx/2009/concept#>
    PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
    PREFIX interval: <http://reference.data.gov.uk/def/intervals/>
    PREFIX offer: <http://data.europa.eu/eurostat/id/offer/>
    PREFIX semtech: <http://mu.semte.ch/vocabularies/core/>
            SELECT DISTINCT ?GTINdesc ?GTIN ?ISBA ?ISBAUUID ?ESBA ?ESBAdesc ?UUID ?quantity ?unit ?training
	        FROM <http://data.europa.eu/eurostat/temp>
            FROM <http://data.europa.eu/eurostat/ECOICOP>
	        WHERE {
                ?obs eurostat:product ?offer.
                ?offer a schema:Offer;
                    semtech:uuid ?UUID;
                    schema:description ?GTINdesc;
                    schema:gtin13 ?GTIN.
                OPTIONAL {
                    ?offer schema:includesObject [
                        a schema:TypeAndQuantityNode;
                        schema:amountOfThisGood ?quantity;
                        schema:unitCode ?unit
                    ].}
                OPTIONAL {
                    ?offer schema:category ?ISBA.
                    ?ISBA semtech:uuid ?ISBAUUID.
                    }
                ?obs eurostat:classification ?ESBA.
                ?ESBA skos:prefLabel ?ESBAdesc.
                ?obs qb:dataSet ?dataset.
                ?dataset dct:publisher <http://data.europa.eu/eurostat/id/organization/5975E7011006433913000001>.
                ?dataset dct:issued \"2017-07-24\"^^xsd:dateTime.
                ?obs eurostat:training ?training.
            }"))

(define hier (parse-query "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?child1 ?uuid1 ?description1 ?notation1 ?child2 ?uuid2 ?description2 ?notation2 ?child3 ?uuid3 ?description3 ?notation3 ?child4 ?uuid4 ?description4 ?notation4 ?child5 ?uuid5 ?description5 ?notation5
FROM <http://data.europa.eu/eurostat/ECOICOP3>
WHERE {
 ?parent mu:uuid \"379436c4-08c3-459a-9b75-b094bdfdbaf4\".
?child1 skos:broader ?parent.
?child1 mu:uuid ?uuid1.
?child1 skos:prefLabel ?description1. FILTER (LANG(?description1) = 'en' || LANG(?description1) = '')
?child1 skos:notation ?notation1. FILTER (LANG(?notation1) = 'en' || LANG(?notation1) = '')
OPTIONAL { ?child2 skos:broader ?child1.
?child2 mu:uuid ?uuid2.
?child2 skos:prefLabel ?description2. FILTER (LANG(?description2) = 'en' || LANG(?description2) = '')
?child2 skos:notation ?notation2. FILTER (LANG(?notation2) = 'en' || LANG(?notation2) = '')
OPTIONAL { ?child3 skos:broader ?child2.
?child3 mu:uuid ?uuid3.
?child3 skos:prefLabel ?description3. FILTER (LANG(?description3) = 'en' || LANG(?description3) = '')
?child3 skos:notation ?notation3. FILTER (LANG(?notation3) = 'en' || LANG(?notation3) = '')
OPTIONAL { ?child4 skos:broader ?child3.
?child4 mu:uuid ?uuid4.
?child4 skos:prefLabel ?description4. FILTER (LANG(?description4) = 'en' || LANG(?description4) = '')
?child4 skos:notation ?notation4. FILTER (LANG(?notation4) = 'en' || LANG(?notation4) = '')
OPTIONAL { ?child5 skos:broader ?child4.
?child5 mu:uuid ?uuid5.
?child5 skos:prefLabel ?description5. FILTER (LANG(?description5) = 'en' || LANG(?description5) = '')
?child5 skos:notation ?notation5. FILTER (LANG(?notation5) = 'en' || LANG(?notation5) = '') } } } } 
} 
ORDER BY ?child1 ?child2 ?child3 ?child4 ?child5
"))

(define martin (parse-query "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   PREFIX qb: <http://purl.org/linked-data/cube#>
   PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX schema: <http://schema.org/>
   PREFIX sdmx-subject: <http://purl.org/linked-data/sdmx/2009/subject#>
   PREFIX sdmx-concept: <http://purl.org/linked-data/sdmx/2009/concept#>
   PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
   PREFIX interval: <http://reference.data.gov.uk/def/intervals/>
   PREFIX offer: <http://data.europa.eu/eurostat/id/offer/>
   PREFIX semtech: <http://mu.semte.ch/vocabularies/core/>
   
           SELECT (COUNT (?offer) AS ?C)
           FROM <http://data.europa.eu/eurostat/temp>
           FROM <http://data.europa.eu/eurostat/ECOICOP>
           WHERE {
               ?obs eurostat:product ?offer.
               OPTIONAL {
                   ?offer schema:includesObject [ a schema:TypeAndQuantityNode;  schema:amountOfThisGood ?quantity;  schema:unitCode ?unit ].
               }
               OPTIONAL {
                   ?offer schema:category ?ISBA.
                   ?ISBA semtech:uuid ?ISBAUUID.
                   }
               ?obs eurostat:classification ?ESBA.
               ?ESBA skos:prefLabel ?ESBAdesc.
               ?obs qb:dataSet ?dataset.
              ?obs eurostat:training false.
             
           }"))

(define vincent  (parse-query "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   PREFIX qb: <http://purl.org/linked-data/cube#>
   PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX schema: <http://schema.org/>
   PREFIX sdmx-subject: <http://purl.org/linked-data/sdmx/2009/subject#>
   PREFIX sdmx-concept: <http://purl.org/linked-data/sdmx/2009/concept#>
   PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
   PREFIX interval: <http://reference.data.gov.uk/def/intervals/>
   PREFIX offer: <http://data.europa.eu/eurostat/id/offer/>
   PREFIX semtech: <http://mu.semte.ch/vocabularies/core/>
           SELECT DISTINCT ?GTINdesc ?GTIN ?ISBA ?ISBAUUID ?ESBA ?ESBAdesc ?UUID ?quantity ?unit ?training
            FROM <http://data.europa.eu/eurostat/temp>
           FROM <http://data.europa.eu/eurostat/ECOICOP>
            WHERE {
               ?obs eurostat:product ?offer.
               ?offer a schema:Offer;
                   semtech:uuid ?UUID;
                   schema:description ?GTINdesc;
                   schema:gtin13 ?GTIN.
               OPTIONAL {
                   ?offer schema:includesObject [
                       a schema:TypeAndQuantityNode;
                       schema:amountOfThisGood ?quantity;
                       schema:unitCode ?unit
                   ].}                ?offer schema:category ?ISBA.
               ?ISBA semtech:uuid ?ISBAUUID.                ?obs eurostat:classification ?ESBA.
               ?ESBA skos:prefLabel ?ESBAdesc.
               ?obs qb:dataSet ?dataset.
               ?dataset dct:publisher <http://data.europa.eu/eurostat/id/organization/596E26C85E3E9B01E2000003>.
               ?dataset dct:issued ?date.
               FILTER ( ?date >= \"2016-07-18\"^^xsd:dateTime)
               ?obs eurostat:training ?training.
           }"))


(define c1 (parse-query "
SELECT ?a WHERE { ?a ?b ?c. ?d ?e ?f } "))

(define c2 (parse-query "
SELECT (COUNT(?a) AS ?count) WHERE { ?a ?b ?c } "))

(define c3 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?uuid WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a dct:Agent. 
}
} GROUP BY ?uuid OFFSET 0 LIMIT 20"))

(define c4 (parse-query  "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

INSERT {
 ?s mu:uuid ?uuid 
}
 WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a dct:Agent. 
}
}"))

(define c6 (parse-query "
SELECT ?a WHERE { ?a ?b ?c. ?a ?b ?d } "))

(define c7 (parse-query "
SELECT ?a WHERE { ?a ?b ?c. ?a ?e ?d } "))

(define c5 (parse-query "
SELECT ?a WHERE { GRAPH <temp> { ?a ?b ?c. ?a ?b ?d } }"))

(define c0 (parse-query "
SELECT ?s WHERE { ?s ?p ?o }"))

(define c01 (parse-query "
INSERT { ?s ?p ?o } WHERE { ?s ?p ?o }"))

(define c8 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
 DELETE {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"5994542D1006434909000002\".
    ?s a dct:Agent.
    ?s dct:title ?name.
    ?datasets dct:publisher ?s.
   }  
 }

WHERE {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"5994542D1006434909000002\".
    ?s a dct:Agent.
    OPTIONAL {
      ?s dct:title ?name.
     }
    OPTIONAL {
      ?datasets dct:publisher ?s.
     }
   }  
 }
"))

(define c9 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
 INSERT DATA {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    <http://data.europa.eu/eurostat/id/organization/5994542D1006434909000002> rdf:type dct:Agent.
    <http://data.europa.eu/eurostat/id/organization/5994542D1006434909000002> mu:uuid \"5994542D1006434909000002\".
    <http://data.europa.eu/eurostat/id/organization/5994542D1006434909000002> dct:title \"your\".
   }  
 }"))

(define c9prim (parse-query "
INSERT { ?s <exists> <now> }
WHERE { ?s ?p ?o}"))

(define c9bis (parse-query "
INSERT { ?s <exists> <now> }
WHERE { ?s a ?T }"))

(define c9tert (parse-query "
INSERT { ?s <exists> <now> }
WHERE { ?s <thinks> <good> }"))

(define c9quart (parse-query "
INSERT { ?s <wantsToBe> ?T . ?s a qb:Dataset }
WHERE { ?s a ?T }"))


(define q (parse-query "PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
INSERT DATA {
 GRAPH <http://mu.semte.ch/application> {
  <http://webcat.tmp.tenforce.com/themes/59AEC217BAED420009000001> rdf:type mu:Book.
  <http://webcat.tmp.tenforce.com/themes/59AEC217BAED420009000001> mu:uuid \"59AEC217BAED420009000001\".
  <http://webcat.tmp.tenforce.com/themes/59AEC217BAED420009000001> dct:title \"Adventure\".
 }
}"))

(define c10 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
 INSERT {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s rdf:type dct:Agent.
    ?s dct:title \"you\".
   }  
 }
WHERE { ?s rdf:type qb:DataSet }"))

(define c11 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?uuid WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s <http://mu.semte.ch/vocabularies/core/uuid> ?uuid; a dct:Agent. 
}
} GROUP BY ?uuid OFFSET 0 LIMIT 20"))

(define c12 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?target
WHERE
{
 GRAPH <http://data.europa.eu/eurostat/temp> 
 {
  ?s mu:uuid \"599711A5100643503200000C\".
  ?s ^dct:publisher/mu:uuid ?target.
 }
}"))

(define c13 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ((COUNT (DISTINCT ?uuid)) AS ?count) WHERE {
    GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?uuid; a dct:Agent. 
}
}
"))

(define c14 (parse-query "PREFIX rewriter: <http://mu.semte.ch/graphs/>
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?uuid
WHERE
{
 {
  SELECT DISTINCT ?graph10905 ?type10906
  WHERE
  {
   GRAPH <http://data.europa.eu/eurostat/graphs> 
   {
    ?rule10904 a rewriter:GraphRule.
    ?graph10905 a rewriter:Graph.
    ?rule10904 rewriter:graph ?graph10905.
    ?rule10904 rewriter:predicate mu:uuid.
    ?rule10904 rewriter:subjectType ?type10906.
   }
  }
 }
 GRAPH <http://data.europa.eu/eurostat/graphs> 
 {
  ?allGraphs10907 a rewriter:Graph.
 }
 GRAPH ?allGraphs10907 
 {
  ?s rdf:type ?type10906.
 }
 GRAPH ?graph10905 
 {
  ?s mu:uuid ?uuid.
 }
 {
  SELECT DISTINCT ?graph10909 ?type10906
  WHERE
  {
   GRAPH <http://data.europa.eu/eurostat/graphs> 
   {
    ?rule10908 a rewriter:GraphRule.
    ?graph10909 a rewriter:Graph.
    ?rule10908 rewriter:graph ?graph10909.
    ?rule10908 rewriter:predicate rdf:type.
    ?rule10908 rewriter:subjectType ?type10906.
   }
  }
 }
 GRAPH ?graph10909 
 {
  ?s rdf:type dct:Agent.
 }
}
GROUP BY ?uuid
OFFSET 0
LIMIT 20"))

(define c15 (parse-query "
PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
DELETE
{
 GRAPH <http://data.europa.eu/eurostat/temp> 
 {
  ?s mu:uuid \"5995A74A1006435032000005\".
  ?s a dct:Agent.
  ?s dct:title ?name.
  ?datasets dct:publisher ?s.
 }
}
WHERE
{
 GRAPH <http://data.europa.eu/eurostat/temp> 
 {
  ?s mu:uuid \"5995A74A1006435032000005\".
  ?s a dct:Agent.
  OPTIONAL
{
 ?s dct:title ?name.
}
  OPTIONAL
{
 ?datasets dct:publisher ?s.
}
 }
}"))

(define cc8 "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
 DELETE {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"5994542D1006434909000002\".
    ?s a dct:Agent.
    ?s dct:title ?name.
    ?datasets dct:publisher ?s.
   }  
 }

WHERE {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid \"5994542D1006434909000002\".
    ?s a dct:Agent.
    OPTIONAL {
      ?s dct:title ?name.
     }
    OPTIONAL {
      ?datasets dct:publisher ?s.
     }
   }  
 }
")


(define c21 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT *
WHERE {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    ?s mu:uuid ?id.
    ?s a dct:Agent.
    OPTIONAL {
      ?s dct:title ?name.
     }
    OPTIONAL {
      ?datasets dct:publisher ?s.
     }
   }
 }
"))

(define c22 (parse-query "
SELECT *
WHERE {
 ?a ?b ?c 
  OPTIONAL { ?a ?b ?d }    
}
"))

(define c16 (parse-query "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
 INSERT DATA {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    <02> rdf:type dct:Agent.
    <02> rdf:type qb:Dataset.
    <02> dct:title \"your\".
   }  
 }"))

(define c17 (parse-query "SELECT * WHERE { SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o } }"))

(define c18 (parse-query "SELECT * WHERE { ?a ?b ?c . { SELECT (COUNT(?a) AS ?count) WHERE { ?a ?b ?c } }}"))

(define c19 (parse-query "SELECT * WHERE { ?a ?b ?c . { SELECT ?a (COUNT(?a) AS ?count) WHERE { ?a ?b ?c } }}"))

(define (go query)
  (rewrite-constraints query))

(define (pgo query) (print (write-sparql (go query))))

;; (let loop ((i 0))
;;   (when (<  i 100)
;;     (print (- (cpu-time)
;;               (begin
;;                 (write-sparql (rewrite-query
;;                                (parse-query cc8) top-rules)) (cpu-time))))
;;     (loop (+ i 1))))


(define c20 (parse-query "PREFIX obs: <http://data.europa.eu/eurostat/id/observation/>
PREFIX eurostat: <http://data.europa.eu/eurostat/ns/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cms: <http://mu.semte.ch/vocabulary/cms/>
PREFIX auth: <http://mu.semte.ch/vocabularies/authorization/>
PREFIX session: <http://mu.semte.ch/vocabularies/session/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie/#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX app: <http://mu.semte.ch/app/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
 INSERT DATA {
  GRAPH <http://data.europa.eu/eurostat/temp> {
    <http://data.europa.eu/eurostat/id/organization/5994542D1006434909000002> rdf:type ext:Car.
    <http://data.europa.eu/eurostat/id/organization/5994542D1006434909000002> mu:uuid \"5994542D1006434909000002\".
    <http://data.europa.eu/eurostat/id/organization/5994542D1006434909000002> dct:title \"your\".
   }  
 }"))

(define c21 (parse-query "WITH <http://mu.semte.ch/application> INSERT {<http://mu.semte.ch/school/classes/dc6c981b-d2fc-484a-95f5-4412d8ea1257> a <http://mu.semte.ch/vocabularies/school/Class>; <http://mu.semte.ch/vocabularies/core/uuid> \"dc6c981b-d2fc-484a-95f5-4412d8ea1257\"; dc:title \"Categorical Fish 102\" } WHERE {}"))

(define c21bis (parse-query "WITH <http://mu.semte.ch/application> INSERT {<http://mu.semte.ch/school/classes/dc6c981b-d2fc-484a-95f5-4412d8ea1257> a school:Class; <http://mu.semte.ch/vocabularies/core/uuid> \"dc6c981b-d2fc-484a-95f5-4412d8ea1257\";<http://purl.org/dc/terms/title> \"Categorical Fish 102\" } WHERE {}"))

(define c22 (parse-query "WITH <http://mu.semte.ch/application> INSERT  {<http://mu.semte.ch/school/classes/1f4640f8-f358-4308-b1b9-dee3e0579e96> <http://mu.semte.ch/vocabularies/school/grade> <http://mu.semte.ch/school/grades/0ec94657-277a-4efc-880f-75fe1a48f27a> .} WHERE {}"))



(define c23 (parse-query "PREFIX xmlns: <http://xmlns.com/foaf/0.1/>
PREFIX app: <http://mu.semte.ch/school/>
PREFIX school: <http://mu.semte.ch/vocabularies/school/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT (COUNT(DISTINCT ?uuid) AS ?count)
WHERE {
 GRAPH <http://mu.semte.ch/application> {
  <http://mu.semte.ch/school/people/2b331d06-3c48-4d9e-94dd-2eed5255fad8> ^school:student ?resource.
  ?resource mu:uuid ?uuid.
 }
}"))

(define c24 (parse-query "
PREFIX xmlns: <http://xmlns.com/foaf/0.1/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ((COUNT (DISTINCT ?uuid)) AS ?count) WHERE {
    GRAPH <http://mu.semte.ch/application> {
    <http://mu.semte.ch/school/people/85f517b7-92e6-44e4-8cfd-bb2f69bad958> ^school:gradeRecipient ?resource. ?resource mu:uuid ?uuid.  
}
}"))

(define c25 (parse-query "
PREFIX xmlns: <http://xmlns.com/foaf/0.1/>
PREFIX app: <http://mu.semte.ch/school/>
PREFIX school: <http://mu.semte.ch/vocabularies/school/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ((COUNT (DISTINCT ?uuid)) AS ?count) WHERE {
    GRAPH <http://mu.semte.ch/application> {
    ?s mu:uuid ?uuid; a school:Grade. 
}
}
"))

(define c26 (parse-query "
WITH <http://mu.semte.ch/application>
INSERT {
   <http://mu.semte.ch/school/classes/06536ff9-398e-4f7a-8465-928fce597317> a <http://mu.semte.ch/vocabularies/school/Class>;
               <http://mu.semte.ch/vocabularies/core/uuid> \"06536ff9-398e-4f7a-8465-928fce597317\";
               <http://purl.org/dc/terms/title> \"Comparative Fish 201\" 
   } WHERE {}"))

(define c27 (parse-query " DELETE { ?s ?p ?o } WHERE { ?s ?p ?o } "))

(define c27b (parse-query " DELETE { ?s ?p ?o } WHERE { ?s a school:Grade } "))


(define c28 (parse-query "WITH <http://mu.semte.ch/application> INSERT  {<http://mu.semte.ch/school/grades/3df98433-5c89-4dd3-bb55-637d5947c347> <http://mu.semte.ch/vocabularies/school/gradeRecipient> <http://mu.semte.ch/school/people/505ed3df-21af-4142-86c9-75acb8be8c73> .} WHERE {}"))

(define c29 (parse-query "WITH <http://mu.semte.ch/application> INSERT {
         <http://mu.semte.ch/school/grades/3df98433-5c89-4dd3-bb55-637d5947c347> rdf:type school:Grade.
         <http://mu.semte.ch/school/grades/3df98433-5c89-4dd3-bb55-637d5947c347> <http://mu.semte.ch/vocabularies/school/gradeRecipient> <http://mu.semte.ch/school/people/505ed3df-21af-4142-86c9-75acb8be8c73> .
   } WHERE {}"))


(define c29b (parse-query "INSERT {
 <http://mu.semte.ch/school/grades/71918514-bd1b-40c2-b244-f1df869bef6b> rdf:type <http://mu.semte.ch/vocabularies/school/Grade>;
    <http://mu.semte.ch/vocabularies/core/uuid> \"71918514-bd1b-40c2-b244-f1df869bef6b\"; 
    <http://mu.semte.ch/vocabularies/school/points> 9; 
     <http://mu.semte.ch/vocabularies/school/gradeRecipient> <http://mu.semte.ch/school/people/93b8eecf-5476-41f6-9dd9-54d4ec15f136>.
}
WHERE {}"))

(define c29c (parse-query "INSERT {
 <http://mu.semte.ch/school/grades/71918514-bd1b-40c2-b244-f1df869bef6b> rdf:type school:Grade;
    <http://mu.semte.ch/vocabularies/core/uuid> \"71918514-bd1b-40c2-b244-f1df869bef6b\"; 
    <http://mu.semte.ch/vocabularies/school/points> 9; 
     <http://mu.semte.ch/vocabularies/school/gradeRecipient> <http://mu.semte.ch/school/people/93b8eecf-5476-41f6-9dd9-54d4ec15f136>.
}
WHERE {}"))

(define c30 (parse-query "WITH <http://mu.semte.ch/application> INSERT {<http://mu.semte.ch/school/grades/28c52060-6b90-4f0e-ad1f-e1c572aba530> a <http://mu.semte.ch/vocabularies/school/Grade>; <http://mu.semte.ch/vocabularies/core/uuid> \"28c52060-6b90-4f0e-ad1f-e1c572aba530\";<http://mu.semte.ch/vocabularies/school/points> 11 } WHERE {}"))

(define c31 (parse-query "INSERT DATA {
 GRAPH <http://mu.semte.ch/application> {
  <http://mu.semte.ch/school/people/dd14adbb-1f50-4867-b6fd-4e45c5ddb2a4> a <http://xmlns.com/foaf/0.1/Person>;
           <http://mu.semte.ch/vocabularies/core/uuid> \"dd14adbb-1f50-4867-b6fd-4e45c5ddb2a4\"; 
           <http://xmlns.com/foaf/0.1/name> \"Milagros Ruley\"; 
           <http://xmlns.com/foaf/0.1/mbox> \"Milagros.Ruley@gmail.com\"; 
           <http://mu.semte.ch/vocabularies/school/role> \"student\".
 }
}"))

;; except for c33, these work with identical read/write constraint
;; with removing of lone filters+values blocks (so UNION => OPTIONAL)
(define c32 (parse-query "INSERT { ?s <is> <blue> } WHERE { ?s <is> <red> } "))

;; unknown table error
(define c33 (parse-query "DELETE { ?s <is> <purple> } WHERE { ?s <is> <red> } "))

(define c33b (parse-query "DELETE { ?s <is> <blue> } WHERE { ?s <is> <blue> } "))

(define c34 (parse-query "DELETE { ?s <is> <purple> } INSERT { ?s <is> <red> } WHERE { ?s <is> <purple> } "))

(define c35 (parse-query "
 PREFIX foaf: <http://xmlns.com/foaf/0.1/>
 PREFIX app: <http://mu.semte.ch/school/>
 PREFIX graphs: <http://mu.semte.ch/school/graphs/>
 PREFIX school: <http://mu.semte.ch/vocabularies/school/>
 PREFIX dct: <http://purl.org/dc/terms/>
 PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
 PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
 PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
 PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
 PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
 PREFIX owl: <http://www.w3.org/2002/07/owl#>
 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 
 SELECT DISTINCT ?uuid
 WHERE {
  GRAPH <http://mu.semte.ch/application> {
   <http://mu.semte.ch/school/classes/10bc2d66-4246-44e3-960c-f8c1ba9788a7> school:teacher ?resource.
   <http://mu.semte.ch/school/classes/10bc2d66-4246-44e3-960c-f8c1ba9788a7> a school:Class.
   ?resource mu:uuid ?uuid.
   ?resource a foaf:Person.
  }
 }
 GROUP BY ?uuid
 OFFSET 0
 LIMIT 20"))


;; (print (time (print (write-sparql (rewrite-constraints c35)))))

(define c36 (parse-query "
SELECT ?s ?name
WHERE {
  ?s a foaf:Person;
     foaf:name ?name
}
"))
 
(define c37 (parse-query "
 PREFIX foaf: <http://xmlns.com/foaf/0.1/>
 PREFIX app: <http://mu.semte.ch/school/>
 PREFIX graphs: <http://mu.semte.ch/school/graphs/>
 PREFIX school: <http://mu.semte.ch/vocabularies/school/>
 PREFIX dct: <http://purl.org/dc/terms/>
 PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
 PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
 PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
 PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
 PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
 PREFIX owl: <http://www.w3.org/2002/07/owl#>
 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 
 SELECT *
 WHERE {
  GRAPH <http://mu.semte.ch/application> {
   OPTIONAL {
    <http://mu.semte.ch/school/classes/28f8494e-384c-4aba-81b3-2b6d5874d5fd> dct:title ?name.
   }
   OPTIONAL {
    <http://mu.semte.ch/school/classes/28f8494e-384c-4aba-81b3-2b6d5874d5fd> dct:subject ?subject.
   }
  }
 }"))

(define c38 (parse-query "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX app: <http://mu.semte.ch/school/>
PREFIX graphs: <http://mu.semte.ch/school/graphs/>
PREFIX school: <http://mu.semte.ch/vocabularies/school/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT *
WHERE {
 GRAPH <http://mu.semte.ch/application> {
  OPTIONAL {
   <http://mu.semte.ch/users/student7> foaf:name ?name.
  }
  OPTIONAL {
   <http://mu.semte.ch/users/student7> foaf:mbox ?email.
  }
  OPTIONAL {
   <http://mu.semte.ch/users/student7> school:role ?role.
  }
 }
}
"))

(define c39 (parse-query "
DELETE {
 GRAPH <http://mu.semte.ch/application> {
  ?s school:classGrade <http://mu.semte.ch/school/grades/d95df8dc-033e-46ea-a758-d1f87d34a0e4>.
 }
}
WHERE {
 GRAPH <http://mu.semte.ch/application> {
  OPTIONAL {
   ?s school:classGrade <http://mu.semte.ch/school/grades/d95df8dc-033e-46ea-a758-d1f87d34a0e4>.
  }
 }
};

INSERT DATA {
 GRAPH <http://mu.semte.ch/application> {
  <http://mu.semte.ch/school/classes/10bc2d66-4246-44e3-960c-f8c1ba9788a7> school:classGrade <http://mu.semte.ch/school/grades/d95df8dc-033e-46ea-a758-d1f87d34a0e4>.
 }
}"))

(load "/home/nathaniel/projects/graph-acl-basics/config/rewriter/basic-authorization.scm")

(define (parse key q)
  (let ((t1 (cpu-time))
        (result (parse-query q)))
    (log-message "~%==Parse Time (~A)==~%~Ams~%" key (- (- t1 (cpu-time) )))
    result))
(define (proxy-query key rewritten-query-string endpoint)
  (let ((t1 (cpu-time)))
    (let-values (((result uri response)
                  (with-input-from-request 
                   (make-request method: 'POST
                                 uri: (uri-reference endpoint)
                                 headers: (headers
                                           '((Content-Type application/x-www-form-urlencoded)
                                             (Accept application/sparql-results+json)))) 
                   `((query . , (format #f "~A" rewritten-query-string)))
                   read-string)))
      (log-message "~%==Query Time (~A)==~%~Ams~%" key (- (- t1 (cpu-time))))
      (values result uri response))))
