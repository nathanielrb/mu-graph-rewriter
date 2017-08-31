

(*constraint* 
 (conc 
  "CONSTRUCT { "
  "  ?s ?p ?o. "
  "} "
  "WHERE "
  "{"
  "  { "
  "    GRAPH <http://mu.semte.ch/application/cars> { "
  "      ?s ?p ?o. "
  "      ?s a ext:Car. "
  "    } "
  "  } "
  "  UNION "
  "  { "
  "    GRAPH <http://mu.semte.ch/application/parts> { "
  "      ?s ?p ?o. "
  "      ?s a ext:CarPart. "
  "    } "
  "  } "
  "} "))


(*constraint* 
 (conc 
  "CONSTRUCT { "
  "  ?s ?p ?o. "
  "} "
  "WHERE "
  "{"
  "  { "
  "  SELECT ?graph ?s ?p ?o "
  "  WHERE {"
  "    GRAPH ?graph { "
  "      ?s ?p ?o. "
  "      ?s a ext:Car. "
  "    } "
  "   } "
  "    VALUES ?graph { <http://mu.semte.ch/application/cars> } "
  "  }"
  "  UNION "
  "  { "
  "  SELECT ?graph ?s ?p ?o "
  "  WHERE {"
  "    GRAPH ?graph { "
  "      ?s ?p ?o. "
  "      ?s a ext:CarPart. "
  "     } "
  "  } "
  "   VALUES ?graph { <http://mu.semte.ch/application/parts> } "
  " } "
  "}"))
