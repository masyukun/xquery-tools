xquery version "1.0-ml";

(:~ 
 : XQuery tools
 : Different tools I've written for working in MarkLogic's XQuery environment.
 : 
 : Example invocation:  xtools:listIndexes(("ELEMENT","PATH"), "FM")
 :
 : @author   Matthew Royal 
 : @see      https://github.com/masyukun/xquery-tools
 : @version  1.0
 :)
module namespace xtools = 'https://matthewroyal.com/MarkLogic/xquery-tools';

import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";



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
    let $columnNames :=
      for $element in $indexes[1]/element()
      return $element/local-name()
    return 
      if ($columnNames) then 
        fn:string-join(("database","index",$columnNames), ",")
      else "No data."
    ,

    (: Output the row data found for this index type :)
    for $index in $indexes
    return 
      string-join(
        (
          $dbName, $indexType
          ,
          for $e in $index/element()
          return fn:string($e)
        ), ","
      )
  )
};

