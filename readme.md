# Mu Query Rewriter

The *mu-query-rewriter* is a proxy service for enriching and constraining SPARQL queries before they are sent to the database. It is designed to be used as part of the [mu-semtech](http://mu.semte.ch) microservice architecture.

A constraint is expressed as a standard SPARQL `CONSTRUCT` query, which conceptually represents an intermediate 'constraint' graph. An incoming query is optimally rewritten to a form which, when run against the full database, is equivalent to the original query being run against the constraint graph. Constraining queries in this way allows shared logic to be abstracted almost to the database level, simplifying the logic handled by each microservice. 

![rewriter diagram](rewriter.png)

A simple example application is using multiple graphs to model data, in such a way that individual microservices do not need to be aware of the rules determining which triples are stored in which graph. 

The principle application is modelling access rights directly in the data, so that an incoming query is effectively run against the subset of data which the current user has permission to query or update.

For example, with the following constraint plugin, and `rdf:type` declared as a "functional property" (see below):

```
CONSTRUCT {
  ?a ?b ?c
}
WHERE {
 {
  GRAPH <cars> {
   ?a ?b ?c;
      a <Car>.
  }
 }
 UNION {
  GRAPH <bikes> {
   ?a ?b ?c;
      a <Bike>.
  }
 }
}
```

the query

```
SELECT *
WHERE {
  ?s a <Bike>;
     <color> ?color.
}
```

will be rewritten to

```
SELECT ?s ?color
WHERE {
  GRAPH <bikes> {
    ?s a <Bike>;
       <color> ?color.
  }
}
```

## Using the Proxy Service



## Describing Constraints

