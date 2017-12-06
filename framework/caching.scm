(load "app.scm")

(define-syntax time
  (syntax-rules ()
    ((_ body ...)
     (-
      (- (cpu-time)
         (begin body ...
                (cpu-time)))))))

(define q   "
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX fao: <http://www.fao.org/countryprofiles/geoinfo/geopolitical/resource/>
PREFIX org: <http://www.w3.org/ns/org#>
PREFIX elodgeo: <http://linkedeconomy.org/geoOntology#>
PREFIX rov: <http://www.w3.org/ns/regorg#>
PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
PREFIX sf: <http://www.opengis.net/ont/sf#>
PREFIX pc: <http://purl.org/procurement/public-contracts#>
PREFIX gr: <http://purl.org/goodrelations/v1#>
PREFIX elod: <http://linkedeconomy.org/ontology#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dctype: <http://purl.org/dc/dcmitype/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rm: <http://mu.semte.ch/vocabularies/logical-delete/>
PREFIX typedLiterals: <http://mu.semte.ch/vocabularies/typed-literals/>
PREFIX app: <http://mu.semte.ch/app/>

SELECT *
WHERE {
 GRAPH <http://yourdatastories.eu/test> {
  <http://linkedeconomy.org/resource/Contract/AwardNotice/2014434014/ID> a elod:Country.
OPTIONAL {
   <http://linkedeconomy.org/resource/Contract/AwardNotice/2014434014/ID> dct:issued ?issued;
                                dct:called \"abc\".
}
 }
}" )

(define (clean str)
  (string-translate* str '(("." . "\\.")
                           ("*" . "\\*")
                           ("?" . "\\?")
                           )))

;; but need way to do this not sequentially, but in a tree?
;; or just index on PREFIXes in a hash table
(define uri-pat "<[^> ]+>")
(define str-pat "\\\"[^\"]+\\\"")

(print
 (time
  (let* ((start-query-index (irregex-match-start-index (irregex-search "SELECT|DELETE|INSERT" q)))
         ;; (start-query-index (irregex-match-start-index (irregex-search "GROUP BY" q))) ; end-stopped group by, limit, offset
         (result (irregex-fold (format "(~A)|(~A)" uri-pat str-pat)  ; do <IRI> 123 "string" but not prefixed:IRI
                               (lambda (from-index match seed)
                                 (print (irregex-match-substring match) " - "  (irregex-match-names match))
                                 (let ((substr (irregex-match-substring match))
                                       (str (first seed))
                                       (bindings (second seed)))
                                   (let* ((key* (cdr-when (assoc substr bindings)))
                                          (key (or key* (symbol->string (gensym 'uri)))))
                                     (list  (conc str
                                                  (clean (substring q from-index (irregex-match-start-index match)))
                                                  (if key*
                                                      (conc "\\k<" key ">")
                                                      (format "(?<~A>~A)" key 
                                                              (cond ((irregex-match uri-pat substr) uri-pat)
                                                                    ((irregex-match str-pat substr) str-pat)))))

                                           (if key* bindings 
                                               (alist-update substr key bindings))
                                           (irregex-match-end-index match)))))

                               '("" () end)
                               q
                               #f
                               start-query-index))
         (pattern (conc ; (clean (substring q 0 start-query-index))
                        (first result)
                        (clean (substring q (third result))))))
    (print pattern)
    (do ((i 0 (+ i 1)))
        ((= i 50) #t)
      (print (irregex-match pattern (substring q start-query-index))))
    )))
