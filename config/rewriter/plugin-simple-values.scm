(define-constraint
 (conc 
  "CONSTRUCT { "
  "  ?s ?p ?o. "
  "} "
  "WHERE "
  "{"
  " GRAPH ?graph { ?s ?p ?o } "
  "  { "
  "    GRAPH ?graph { "
;  "      ?s ?p ?o. "
  "      ?s a dct:Agent. "
  "   } "
  "    VALUES ?graph { <http://data.europa.eu/eurostat/retailers> } "
  "  }"
  "  UNION "
  "  { "
  "    GRAPH ?graph  { "
;  "      ?s ?p ?o. "
  "      ?s a qb:Dataset. "
   "  } "
  "    VALUES ?graph { <http://data.europa.eu/eurostat/datasets> } "
  " } "
  "}"))


(define-constraint
 (conc 
  "CONSTRUCT { "
  "  ?s ?p ?o. "
  "} "
  "WHERE "
  "{"  
  "  { "
  "    GRAPH ?graph { "
  "      ?s ?p ?o. "
  "   } "
  "    VALUES ?p { mu:uuid  } "
  "    VALUES ?graph { <http://data.europa.eu/eurostat/uuid>  } "
  "  } "
  "  UNION "
  "  { "
  "    GRAPH ?graph { "
  "      ?s ?p ?o. "
  "      ?s a dct:Agent. "
  "   } "
  "    VALUES ?graph { <http://data.europa.eu/eurostat/retailers> } "
  "  }"
  "  UNION "
  "  { "
  "    GRAPH ?graph  { "
  "      ?s ?p ?o. "
  "      ?s a qb:Dataset. "
   "  } "
  "    VALUES ?graph { <http://data.europa.eu/eurostat/datasets> } "
  " } "
  "}"))


(define-constraint
 (conc 
  "CONSTRUCT { "
  "  ?s ?p ?o. "
  "} "
  "WHERE "
  "{"  
  "  { "
  "    GRAPH ?graph { "
  "      ?s ?p ?o. "
  "   } "
  "    VALUES (?graph ?p) { (<http://data.europa.eu/eurostat/uuid>  mu:uuid)  } "
  "  } "
  "  UNION "
  "  { "
  "    GRAPH ?graph { "
  "      ?s ?p ?o. "
  "      ?s rdf:type ?type. "
  "   } "
  "    VALUES (?graph ?type) { "
  "       (<http://data.europa.eu/eurostat/retailers> dct:Agent) "
  "       (<http://data.europa.eu/eurostat/datasets> qb:Dataset) "
  "    } "
  "   FILTER ( ?p != mu:uuid )"
  ;;" FILTER ( ?p NOT IN (mu:uuid)) "
;;  " BIND ( COUNT(?type) AS ?count )"
  "  }"
  "}"))


(*functional-properties* '(rdf:type))

;; (define-constraint
;;  (conc 
;;   "CONSTRUCT { "
;;   "  ?s ?p ?o. ?s mu:uuid ?id"
;;   "} "
;;   "WHERE "
;;   "{"  
;;   " {"
;;   "  GRAPH ?uuidGraph { "
;;   "      ?s mu:uuid ?id. "
;;   "   } "
;;   "   VALUES ?uuidGraph { <http://data.europa.eu/eurostat/uuid> } "
;;   " }"
;;   ""
;;   " { "
;;   "  GRAPH ?graph { "
;;   "    ?s ?p ?o. "
;;   "    ?s rdf:type ?type. "
;;   "   } "
;;   "   FILTER ( ?p != mu:uuid ) "
;;   "   VALUES (?graph ?type) { "
;;   "       (<http://data.europa.eu/eurostat/retailers> dct:Agent)"
;;   "       (<http://data.europa.eu/eurostat/datasets> qb:Dataset) "
;;   "   }"
;;   " }"
;;   "}"))
