
# mu-constraint-rewriter

The *mu-constraint-rewriter* is a proxy service for enriching and constraining SPARQL queries before they are sent to the database. A constraint is expressed as a standard SPARQL CONSTRUCT query, which conceptually represents an intermediate 'constraint' graph. An incoming query is optimally rewritten to a form which, run against the full database, is equivalent to the original query being run against the intermediate graph. Constraining queries in this way allows shared logic to be abstracted almost to the database level, simplifying the logic handled by each microservice. 

One example application is using multiple graphs to model data in such a way that individual microservices do not need to be aware of the rules determining which triples are stored in which graph. Another application is modelling access rights directly in the database, so that an incoming query is effectively run against the subset of graphs which the current user has permission to query or update.

## Using the Proxy Service

### In Docker

### Running Natively

## Describing Constraints

## Writing Custom Plugins
