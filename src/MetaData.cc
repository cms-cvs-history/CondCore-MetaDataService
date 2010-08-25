#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/MetaDataService/interface/MetaDataSchemaUtility.h"
#include "CondCore/MetaDataService/interface/MetaDataNames.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "RelationalAccess/SchemaException.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/TableDescription.h"
#include "RelationalAccess/ITablePrivilegeManager.h"
#include "RelationalAccess/ICursor.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/AttributeSpecification.h"
#include "CoralBase/Attribute.h"
#include <memory>
//#include <iostream>


namespace {

    std::string mdErrorPrefix(const std::string& source, const std::string& name) {
      return source+std::string(": metadata entry \"")+name+std::string("\" ");
    }
    

    void mdError(const std::string& source, const std::string& name, const std::string& mess) {
      throw cond::Exception(mdErrorPrefix(source,name)+mess);
    }
    
    void mdDuplicateEntryError(const std::string& source, const std::string& name) {
      mdError(source, name, "Already exists");
    }

    void mdNoTable(const std::string& source, const std::string& name) {
      mdError(source, name, ". Cond-MetaData table does not exist: please initialize");
    }

    void mdNoEntry(const std::string& source, const std::string& name) {
      mdError(source, name, "does not exists");
    }

}


cond::MetaData::MetaData(cond::DbSession& coraldb):m_coraldb(coraldb){
}
cond::MetaData::~MetaData(){
}
bool 
cond::MetaData::addMapping(const std::string& name, const std::string& iovtoken, cond::TimeType timetype ){
  cond::MetaDataSchemaUtility ut(m_coraldb);
  try{
    ut.create();
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::AttributeList rowBuffer;
    coral::ITableDataEditor& dataEditor = mytable.dataEditor();
    dataEditor.rowBuffer( rowBuffer );
    rowBuffer[cond::MetaDataNames::tagColumn()].data<std::string>()=name;
    rowBuffer[cond::MetaDataNames::tokenColumn()].data<std::string>()=iovtoken;
    rowBuffer[cond::MetaDataNames::timetypeColumn()].data<int>()=timetype;
    dataEditor.insertRow( rowBuffer );
  }catch( const coral::DuplicateEntryInUniqueKeyException& er ){
    mdDuplicateEntryError("addMapping",name);
  }catch(std::exception& er){
    mdError("MetaData::addMapping",name,er.what());
  }
  return true;
}
bool 
cond::MetaData::replaceToken(const std::string& name, const std::string& newtoken){
  try{
    if(!m_coraldb.nominalSchema().existsTable(cond::MetaDataNames::metadataTable())){
      mdNoTable("MetaData::replaceToken", name);
    }
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::AttributeList inputData;
    coral::ITableDataEditor& dataEditor = mytable.dataEditor();
    inputData.extend<std::string>("newToken");
    inputData.extend<std::string>("oldTag");
    inputData[0].data<std::string>() = newtoken;
    inputData[1].data<std::string>() = name;
    std::string setClause(cond::MetaDataNames::tokenColumn());
    setClause+="= :newToken";
    std::string condition( cond::MetaDataNames::tagColumn() );
    condition+="= :oldTag";
    dataEditor.updateRows( setClause, condition, inputData );
  }catch( coral::DuplicateEntryInUniqueKeyException& er ){
    mdDuplicateEntryError("replaceToken",name);
  }catch(std::exception& er){
    mdError("MetaData::replaceToken",name,er.what());
  }
  return true;
}
const std::string 
cond::MetaData::getToken( const std::string& name ) const{
  bool ok=false;
  std::string iovtoken;
  try{
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    query->setRowCacheSize( 100 );
    coral::AttributeList BindVariableList;
    //std::string condition=cond::MetaDataNames::tagColumn()+" = '"+name+"'";
    std::string condition=cond::MetaDataNames::tagColumn()+"= :name";
    BindVariableList.extend("name",typeid(std::string));
    BindVariableList["name"].data<std::string>()=name;
    query->setCondition( condition, BindVariableList );
    query->addToOutputList( cond::MetaDataNames::tokenColumn() );
    coral::ICursor& cursor = query->execute();
    while( cursor.next() ) {
      const coral::AttributeList& row = cursor.currentRow();
      iovtoken=row[ cond::MetaDataNames::tokenColumn() ].data<std::string>();
      ok=true;
    }
  }catch(const coral::TableNotExistingException& er){
    mdNoTable("MetaData::getToken", name);
  }catch(const std::exception& er){
    mdError("MetaData::getToken", name,er.what() );
  }
  if (!ok) mdNoEntry("MetaData::getToken", name);
  return iovtoken;
}

void 
cond::MetaData::getEntryByTag( const std::string& tagname, cond::MetaDataEntry& result ) const{
  bool ok=false;
  result.tagname=tagname;
  try{
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    query->setRowCacheSize( 100 );
    coral::AttributeList BindVariableList;
    //std::string condition=cond::MetaDataNames::tagColumn()+" = '"+tagname+"'";
    std::string condition=cond::MetaDataNames::tagColumn()+" = :tagname";
    BindVariableList.extend("tagname",typeid(std::string) );
    BindVariableList["tagname"].data<std::string>()=tagname;
    query->setCondition( condition,BindVariableList );
    query->addToOutputList( cond::MetaDataNames::tokenColumn() );
    query->addToOutputList( cond::MetaDataNames::timetypeColumn() );
    coral::ICursor& cursor = query->execute();
    while( cursor.next() ) {
      const coral::AttributeList& row = cursor.currentRow();
      result.iovtoken=row[ cond::MetaDataNames::tokenColumn() ].data<std::string>();
      int tp=row[ cond::MetaDataNames::timetypeColumn() ].data<int>();
      result.timetype=(cond::TimeType)tp;
      //result.timetype=row[ cond::MetaDataNames::timetypeColumn() ].data<int>();
      ok=true;
    }
  }catch(const coral::TableNotExistingException& er){
    mdNoTable("MetaData::getEntryByTag", tagname);
  }catch(const std::exception& er){
    mdError("MetaData::getEntryByTag", tagname, er.what() );
  }
  if (!ok) mdNoEntry("MetaData::getEntryByTag", tagname);
}

/*
void 
cond::MetaData::createTable(const std::string& tabname){
  coral::ISchema& schema=m_coraldb.nominalSchema();
  coral::TableDescription description;
  description.setName( tabname );
  description.insertColumn(  cond::MetaDataNames::tagColumn(), coral::AttributeSpecification::typeNameForId( typeid(std::string)) );
  description.insertColumn( cond::MetaDataNames::tokenColumn(), coral::AttributeSpecification::typeNameForId( typeid(std::string)) );
  description.insertColumn( cond::MetaDataNames::timetypeColumn(), coral::AttributeSpecification::typeNameForId( typeid(int)) );
  std::vector<std::string> cols;
  cols.push_back( cond::MetaDataNames::tagColumn() );
  description.setPrimaryKey(cols);
  description.setNotNullConstraint( cond::MetaDataNames::tokenColumn() );
  coral::ITable& table=schema.createTable(description);
  table.privilegeManager().grantToPublic( coral::ITablePrivilegeManager::Select);
}
*/

bool cond::MetaData::hasTag( const std::string& name ) const{
  bool result=false;
  try{
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    coral::AttributeList BindVariableList;
    //std::string condition=cond::MetaDataNames::tagColumn()+" = '"+name+"'";
    std::string condition=cond::MetaDataNames::tagColumn()+" = :name";
    BindVariableList.extend("name", typeid(std::string) );
    BindVariableList["name"].data<std::string>()=name;
    query->setCondition( condition, BindVariableList );
    coral::ICursor& cursor = query->execute();
    if( cursor.next() ) result=true;
    cursor.close();
  }catch(const coral::TableNotExistingException& er){
    ///do not remove ! must ignore this exception!!!
    return false;
  }catch(const std::exception& er){
    mdError("MetaData::hasTag", name, er.what() );
  }
  return result;
}

void 
cond::MetaData::listAllTags( std::vector<std::string>& result ) const{
  try{
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    query->addToOutputList( cond::MetaDataNames::tagColumn() );
    query->setMemoryCacheSize( 100 );
    coral::ICursor& cursor = query->execute();
    while( cursor.next() ){
      const coral::AttributeList& row = cursor.currentRow();
      result.push_back(row[cond::MetaDataNames::tagColumn()].data<std::string>());
    }
    cursor.close();
  }catch(const coral::TableNotExistingException& er){
        mdNoTable("MetaData::listAllTags", "All");
  }catch(const std::exception& er){
    throw cond::Exception( std::string("MetaData::listAllTags: " )+er.what() );
  }
}

void 
cond::MetaData::listAllEntries( std::vector<cond::MetaDataEntry>& result ) const{
  try{
    coral::ITable& mytable=m_coraldb.nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    query->addToOutputList( cond::MetaDataNames::tagColumn() );
    query->setMemoryCacheSize( 100 );
    coral::ICursor& cursor = query->execute();
    while( cursor.next() ){
      cond::MetaDataEntry r;
      const coral::AttributeList& row = cursor.currentRow();
      r.tagname=row[cond::MetaDataNames::tagColumn()].data<std::string>();
      r.iovtoken=row[cond::MetaDataNames::tokenColumn()].data<std::string>();
      int tp=row[cond::MetaDataNames::timetypeColumn()].data<int>();
      r.timetype=(cond::TimeType)tp;
      result.push_back(r);
    }
    cursor.close();
  }catch(const coral::TableNotExistingException& er){
         mdNoTable("MetaData::listAllEntries", "All");
  }catch(const std::exception& er){
    throw cond::Exception( std::string("MetaData::listAllEntries: " )+er.what() );
  }
}

void 
cond::MetaData::deleteAllEntries(){
  coral::AttributeList emptybinddata;
  try{
    coral::ITable& table=m_coraldb.nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::ITableDataEditor& dataEditor = table.dataEditor();
    dataEditor.deleteRows( "",emptybinddata ); 
  }catch(const coral::TableNotExistingException& er){
    ///do not remove ! must ignore this exception!!!
    return;
  }catch(const std::exception& er){
    throw cond::Exception( std::string("MetaData::deleteAllEntries: " )+er.what() );
  }
}
void cond::MetaData::deleteEntryByToken( const std::string& token ){
  coral::AttributeList deletecondition;
  deletecondition.extend( cond::MetaDataNames::tokenColumn(), typeid(std::string) );
  deletecondition[cond::MetaDataNames::tokenColumn()].data<std::string>()=token;
  std::string whereClause=cond::MetaDataNames::tokenColumn()+"=:"+cond::MetaDataNames::tokenColumn();
  try{
    coral::ITable& table=m_coraldb.nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::ITableDataEditor& dataEditor = table.dataEditor();
    dataEditor.deleteRows(whereClause,deletecondition ); 
    return;
  }catch(const coral::TableNotExistingException& er){
    ///do not remove ! must ignore this exception!!!
    return;
  }catch(const std::exception& er){
    throw cond::Exception( std::string("MetaData::deleteEntryByToken: " )+er.what() );
  }
}
void cond::MetaData::deleteEntryByTag( const std::string& tag ){
  coral::AttributeList deletecondition;
  deletecondition.extend( cond::MetaDataNames::tagColumn(), typeid(std::string) );
  deletecondition[cond::MetaDataNames::tagColumn()].data<std::string>()=tag;
  std::string whereClause=cond::MetaDataNames::tagColumn()+"=:"+cond::MetaDataNames::tagColumn();
  try{
    coral::ITable& table=m_coraldb.nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::ITableDataEditor& dataEditor = table.dataEditor();
    dataEditor.deleteRows(whereClause,deletecondition ); 
  }catch(const coral::TableNotExistingException& er){
    ///do not remove ! must ignore this exception!!!
    return;
  }catch(const std::exception& er){
    throw cond::Exception( std::string("MetaData::deleteEntryByTag: " )+er.what() );
  }
}
