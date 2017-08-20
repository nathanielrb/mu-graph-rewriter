(*constraint*
 (conc "CONSTRUCT { ?s ?p ?o } "
       " WHERE { "
       " { SELECT DISTINCT ?graph ?type "
       " WHERE { "
       " GRAPH <http://data.europa.eu/eurostat/graphs> { "
       " ?rule a rewriter:GraphRule. "
       " ?graph a rewriter:Graph. "
       " ?rule rewriter:graph ?graph. "
       " ?rule rewriter:predicate ?p. "
       " ?rule rewriter:subjectType ?type. } } } "
       " GRAPH <http://data.europa.eu/eurostat/graphs> { ?allGraphs a rewriter:Graph }  "
       " GRAPH ?allGraphs { ?s rdf:type ?type } "
       " GRAPH ?graph { ?s ?p ?o } } "))
