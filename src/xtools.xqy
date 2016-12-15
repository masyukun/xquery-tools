xquery version "1.0-ml";

(:~ 
 : XQuery tools
 : Different tools I''ve written for working in MarkLogic''s XQuery environment.
 : 
 : Example invocation:  xtools:listIndexes(("ELEMENT","PATH"), "FM")
 :
 : @author   Matthew Royal 
 : @see      https://github.com/masyukun/xquery-tools
 : @version  1.1
 :)
module namespace xtools = "https://matthewroyal.com/MarkLogic/xquery-tools";

import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";

declare namespace db = "http://marklogic.com/xdmp/database";


(: PARAMETERS FOR Index Integrity Checker module :)
declare variable $FINDBADURIS := fn:false(); (: Set to fn:true() if you want a complete list of bad URIs. :)
declare variable $MAXURIS as xs:integer := 10; (: How many URIs to return (in case there are a ton.) -1  means no limit. :)




(:~
 : This function simply returns the specified integer.
 : @param   $indexType Choice(s): ELEMENT, ATTRIBUTE, FIELD, PATH
 : @param   $dbName name(s) of database to operate on
 : @return  CSV listing of requested database indexes
 :)
declare function xtools:listIndexes($indexTypes as xs:string+, $dbNames as xs:string+) {

  for $dbName in $dbNames
  for $indexType in $indexTypes
  let $config := admin:get-configuration()
  let $indexes := 
    switch ($indexType)
      case "ELEMENT" return   admin:database-get-range-element-indexes($config, xdmp:database($dbName) )
      case "ATTRIBUTE" return admin:database-get-range-element-attribute-indexes($config, xdmp:database($dbName) )
      case "FIELD" return     admin:database-get-range-field-indexes($config, xdmp:database($dbName) )
      case "PATH" return      admin:database-get-range-path-indexes($config, xdmp:database($dbName) )
      default return fn:error(xs:QName("INVALID-INDEX-TYPE"), "INDEXTYPE must be one of the following: ELEMENT, ATTRIBUTE, FIELD, PATH")
    
  return (
    (: Output the column names :)
    let $localNameNum := -1
    let $columnNames :=
      for $element at $i in $indexes[1]/element()
      let $columnName := $element/local-name()
      let $_ := if ($columnName eq "localname") then xdmp:set($localNameNum, $i) else ()
      return $columnName
    return (
      if ($columnNames) then 
        fn:string-join(("database","index",$columnNames), ",")
      else "No data."
      ,
  
      (: Output the row data found for this index type :)
      for $rawindex in $indexes
  
      (: Sometimes people group localnames in index definitions. This splits them out. :)
      let $splitLocalNames := fn:tokenize( $rawindex/db:localname/fn:string(), " " )
      
      let $index := 
        for $localname in $splitLocalNames
        return 
          fn:string-join(
            (
              $dbName, $indexType, 
              for $r at $i in $rawindex/element()
              return
                if ($i eq $localNameNum) then $localname else $r/fn:string()
            )
            , ","
          )
      return ($index)
    )
  )
};



(:~
 : Get the values of the specified QName from the specified document tree, using recursive descent
 : @param   $docNode Representing the document to search.
 : @param   $searchNode The QName to search for
 : @return  Values of the specified QName from the specified search document
 :)
declare function xtools:getNodeValue($docNode as node(), $searchNode as xs:QName) { 
  if (fn:node-name($docNode) eq $searchNode) then $docNode/text()
  else ($docNode/child::* ! xtools:getNodeValue(., $searchNode))    
}; 




(:~
 : Check all the element range indexes of the specified databases for bad data.
 : This can occur when the index is set to IGNORE bad data, and makes affected URIs appear
 : in the results of every cts:element-range-query() call.
 : @param   $findAffectedUris Set to fn:true() if you want a complete list of bad URIs. Set to fn:false() if you just want a report on which indexes are bad and how many documents contain bad index data.
 : @param   $maxUris  How many URIs to return (in case there are a ton, and you only need examples.) No limit = -1
 : @param   $dbName name(s) of database to operate on
 : @return  CSV listing of requested database indexes
 :)
declare function xtools:checkIndexIntegrity($dbNames as xs:string+, $findAffectedUris as xs:boolean, $maxUris as xs:integer) {


  for $databaseName in $dbNames

  (: Get a list of element indexes for the current database :)
  let $indexes := xtools:listIndexes("ELEMENT", $databaseName )


  (: Process each line that''s not a header:)
  for $entry at $i in $indexes
  return if ($i eq 1) then () 
  else
    (: Tokenize the CSV values we need :)
    let $pieces := fn:tokenize($entry, ",")
    let $namespace := $pieces[4]
    let $elementName := $pieces[5]
    let $type := $pieces[3]


    (: Get a random value from the database for this indexed field :)
    let $qname := fn:QName($namespace, $elementName)
    let $randomDoc := cts:search(/, cts:element-query($qname, cts:and-query(()) ), ("score-random") )[1]
    let $randomValue := xtools:getNodeValue( $randomDoc, $qname )[1]

    (: Do the two kinds of estimates :)
    let $elementValuesEstimate := xdmp:estimate(cts:search(/, cts:element-value-query($qname,      $randomValue ) ))
    let $elementRangeEstimate  := xdmp:estimate(cts:search(/, cts:element-range-query($qname, "=", $randomValue ) ))

    return 
      (: Indexes with no errors result in the numbers being equal. :)
      if ($elementRangeEstimate eq $elementValuesEstimate) then ()
      else (
        (: This index is bad!! :)
        "Index on ["|| fn:string($qname) ||"] has " 
        || fn:format-number($elementRangeEstimate - $elementValuesEstimate, "#,##0") ||" documents with invalid data in its ["|| $type ||"] index. "
        ,
        (: Figure out the bad URIs for this index :)
        if (not($findAffectedUris)) then () 
        else
          (: Set operation between the two search methods to determine the URIs with bad data :)
          let $valueURIs := cts:uris((), ("map"), cts:element-value-query($qname, $randomValue ))
          let $rangeURIs := cts:uris((), ("map"), cts:element-range-query($qname, "=", $randomValue ))
          let $affectedURIs := map:keys($rangeURIs - $valueURIs)


          return (
            (: User-friendly summary of the problem :)
            fn:count( $affectedURIs ) || " affected URIs. "
            || (if ($maxUris eq -1) then ""  else (" First " || $maxUris || " URIS:"))
            , 
            
            (: Output the specified number of URIs :)
            $affectedURIs[1 to (if ($maxUris eq -1) then fn:last() else $maxUris)]
          )
      )
};

