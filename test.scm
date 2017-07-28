(use s-sparql s-sparql-parser mu-chicken-support matchable)

(load "app.scm")
(load "plugins/graph-rewriter.scm")

(access-log "access.log")
(debug-log "debug.log")

(*default-graph* '<http://data.europa.eu/eurostat/graphs>)

(*realm-id-graph* '<http://data.europa.eu/eurostat/uuid>)

(vhost-map `((".*" . ,handle-app)))


(*subscribers-file* "../config/rewriter/subscribers.json")
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; testing

(use spiffy)

(define-namespace skos "http://www.w3.org/2004/02/skos/core#")
(define-namespace eurostat "http://data.europa.eu/eurostat/")

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
    ?s a ?stype.
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


(define rw (read-sparql "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
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

(define subs (read-sparql "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
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
