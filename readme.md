**OLD VERSION**

these docs refer to an earlier version, before refactoring into a plugin-based system.

# mu-query-rewriter


Rewrites SPARQL queries for dispatching on multiple named graphs.

The SPARQL parser is currently limited, so user beware.

## The Rules
 
Graphs are decided on a per-triple basis, based on the subject type, the predicate, and an optional "realm". Rules are defined in the triple store, and  must be defined explicitly and exhaustively for all subject types and predicates. Rules must also be complete: if no graph is matched, triples may still resolve in other graphs.

A realm can be a node or an RDF literal. Note that realms can be added and deleted on the fly using the `/realm` endpoint, see below.

Example:

```
@prefix dct: <http://purl.org/dc/terms/>.
@prefix skos: <http://www.w3.org/2004/02/skos/core#>.
@prefix mu: <http://mu.semte.ch/vocabularies/core/>.
@prefix types: <http://data.europa.eu/eurostat/graphs/types/>.
@prefix retailers: <http://data.europa.eu/eurostat/retailers/>.
@prefix schema: <http://schema.org/>.
@prefix qb: <http://purl.org/linked-data/cube#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.

@prefix rewriter: <http://mu.semte.ch/graphs/>.
@prefix graphs: <http://data.europa.eu/eurostat/>.
@prefix eurostat: <http://data.europa.eu/eurostat/ns/>.
@prefix rules: <http://data.europa.eu/eurostat/graphs/rules/>.

#####
# uuids
graphs:uuid a rewriter:Graph.

rules:rule1 a rewriter:GraphRule;
            rewriter:graph graphs:uuid;
            rewriter:subjectType skos:Concept,
                                 qb:Observation,
                                 schema:Offer,
                                 qb:Dataset,
                                 dct:Agent,
                                 eurostat:Date,
                                 schema:TypeAndQuantityNode;
            rewriter:predicate mu:uuid.

#####
# ECOICOP and ISBA
graphs:ECOICOP a rewriter:Graph.

rules:rule2 a rewriter:GraphRule;
            rewriter:graph graphs:ECOICOP;
            rewriter:subjectType skos:Concept;
            rewriter:predicate rdf:type,
                               skos:prefLabel, 
                               skos:broader,
                               skos:notation.

#####
# Scanner Data
types:ScannerData a rewriter:GraphType.

retailers:Lidl a rewriter:Graph;
               rewriter:type types:ScannerData;
               rewriter:realm <http://data.europa.eu/eurostat/id/organization/595CC438E1D94E0009000003>.

rules:rule3 a rewriter:GraphRule;
              rewriter:graphType types:ScannerData;
              rewriter:subjectType qb:Observation;
              rewriter:predicate rdf:type,
                                 qb:dataSet,
                                 eurostat:product,
                                 eurostat:classification,
                                 eurostat:period,
                                 eurostat:training,
                                 eurostat:amount.

```

As an illustration, the query

```
PREFIX mu: <http://mu.semte.ch/vocabularies/core/> 

SELECT ?s0 ?s1 ?s2
WHERE {
  ?s0 a skos:Concept.
  ?s0 mu:uuid \"12345\".  
  ?s1 a ?type.
  ?s1 ?p ?o1.
  ?s2 ?p2 ?o2 
}
```

is rewritten to:

```
PREFIX rewriter: <http://mu.semte.ch/graphs/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

SELECT ?s0 ?s1 ?s2
FROM <http://data.europa.eu/eurostat/graphs>
FROM <http://data.europa.eu/eurostat/retailers/JollyPirate>
FROM <http://data.europa.eu/eurostat/retailers/Cornerstore>
FROM <http://data.europa.eu/eurostat/retailers>
FROM <http://data.europa.eu/eurostat/uuid>
FROM <http://data.europa.eu/eurostat/ECOICOP>
FROM <http://data.europa.eu/eurostat/products>
FROM <http://data.europa.eu/eurostat/retailers/Lidl>
FROM <http://data.europa.eu/eurostat/datasets>
FROM NAMED <http://data.europa.eu/eurostat/graphs>
FROM NAMED <http://data.europa.eu/eurostat/retailers/JollyPirate>
FROM NAMED <http://data.europa.eu/eurostat/retailers/Cornerstore>
FROM NAMED <http://data.europa.eu/eurostat/retailers>
FROM NAMED <http://data.europa.eu/eurostat/uuid>
FROM NAMED <http://data.europa.eu/eurostat/ECOICOP>
FROM NAMED <http://data.europa.eu/eurostat/products>
FROM NAMED <http://data.europa.eu/eurostat/retailers/Lidl>
FROM NAMED <http://data.europa.eu/eurostat/datasets>
WHERE {
SELECT DISTINCT *
WHERE {
  GRAPH <http://data.europa.eu/eurostat/uuid> {
    ?s0 mu:uuid "12345".
   }  
  GRAPH ?graph2617 {
    ?s0 a skos:Concept.
   }  
  OPTIONAL {
    GRAPH <http://data.europa.eu/eurostat/graphs> {
      ?rule2618 a rewriter:GraphRule.
      ?graph2617 a rewriter:Graph.
      ?rule2618 rewriter:graph ?graph2617.
      ?rule2618 rewriter:predicate rdf:type.
      ?rule2618 rewriter:subjectType skos:Concept.
     }  
   }
  GRAPH ?graph2588 {
    ?s1 a ?type.
   }  
  OPTIONAL {
    GRAPH <http://data.europa.eu/eurostat/graphs> {
      ?rule2589 a rewriter:GraphRule.
      ?graph2588 a rewriter:Graph.
      ?rule2589 rewriter:graph ?graph2588.
      ?rule2589 rewriter:predicate rdf:type.
      ?rule2589 rewriter:subjectType ?type.
     }  
   }
  GRAPH ?graph2591 {
    ?s1 ?p ?o1.
   }  
  OPTIONAL {
    GRAPH <http://data.europa.eu/eurostat/graphs> {
      ?rule2592 a rewriter:GraphRule.
      ?graph2591 a rewriter:Graph.
      ?rule2592 rewriter:graph ?graph2591.
      ?rule2592 rewriter:predicate ?p.
      ?rule2592 rewriter:subjectType ?type.
     }  
   }
  GRAPH ?graph2595 {
    ?s2 ?p2 ?o2.
   }  
  OPTIONAL {
    ?s2 a ?stype2594.
    GRAPH <http://data.europa.eu/eurostat/graphs> {
      ?rule2596 a rewriter:GraphRule.
      ?graph2595 a rewriter:Graph.
      ?rule2596 rewriter:graph ?graph2595.
      ?rule2596 rewriter:predicate ?p2.
      ?rule2596 rewriter:subjectType ?stype2594.
     }  
   }
 }
 }
```

## Parameters

**MU_DEFAULT_GRAPH**

Graph containing the graph rules, defaults to `http://mu.semte.ch/graphs`.

**REALM_ID_GRAPH**

Graph containing uuids of realms, defaults to `http://mu.semte.ch/uuid`.

**REWRITE_GRAPH_STATEMENTS**

If "true" (default), explicit graph statements are flattened and rewritten; if "false" they are left untouched.

This parameter may be overridden on a per-query basis, either by including a `preserve-graph-statements: true` header or with a `?preserve-graph-statements=true` query parameter.

**REWRITE_SELECT_QUERIES**

If "true", SELECT queries are rewritten like update queries. If "false", they are left as is, except that the FROM/FROM NAMED graph statements are replaced to reflect the existing graph rules.

This parameter may be overridden on a per-query basis, either by including a `rewrite-select-queries: true` header or with a `?rewrite-select-queries=true` query parameter.

**MU_SPARQL_ENDPOINT**

The SPARQL endpoint. Defaults to `http://database:8890/sparql` when inside docker, and `http://127.0.0.1:8890/sparql` otherwise.

## API

### GET /sparql
### POST /sparql

Imitates a Virtuoso SPARQL endpoint.

### POST /realm
### POST /realm/:realm-id

Add a graph realm by uri or uuid. The  payload must contain either "graph-realm" (the uri) or "graph-realm-id" (uuid):

```
{ 
  "graph-realm-id": "<ID>",
  "graph-type": "http://data.europa.eu/eurostat/graphs/types/ScannerData",
  "graph": "http://data.europa.eu/eurostat/retailers/Lidl"
}
```

### DELETE /realm
### DELETE /ream/:realm-id

Delete a graph realm by uri or uuid. Payload is a JSON object containing either `graph-realm` or `graph-realm-id`

```
{
  "graph-realm": "http://data.europa.eu/eurostat/id/organization/<ID>"
}
```

### PATCH /session/realm/:realm-id

Changes the realm for current session based on the `mu-session-id` header, which must be present.

## Running

In docker compose:

```
version: "2"
services:
  rewriter:
    build: ./mu-graph-rewriter
    environment:
      MU_DEFAULT_GRAPH: "http://data.europa.eu/eurostat/graphs"
      REALM_ID_GRAPH: "http://data.europa.eu/eurostat/uuid"
      REWRITE_SELECT_QUERIES: "false"
      REWRITE_GRAPH_STATEMENTS: "true"
    ports:
      - "4029:8890"
    links:
      - db:database
```

## Known issues

### Parser limitations

### Other Issues

Query order is not preserved: triples are grouped first. This is a know issue and will be addressed in a future version.