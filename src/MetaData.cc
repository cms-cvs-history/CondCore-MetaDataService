#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/MetaDataService/interface/MetaDataNames.h"
#include "CondCore/MetaDataService/interface/MetaDataExceptions.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "RelationalAccess/AccessMode.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IConnectionService.h"
#include "RelationalAccess/IConnectionServiceConfiguration.h"
#include "RelationalAccess/RelationalServiceException.h"
//#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/SchemaException.h"
#include "RelationalAccess/ISession.h"
#include "RelationalAccess/ISessionProxy.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/TableDescription.h"
#include "RelationalAccess/ITablePrivilegeManager.h"
#include "RelationalAccess/IPrimaryKey.h"
#include "RelationalAccess/ICursor.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "CoralBase/Exception.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/AttributeSpecification.h"
#include "CoralBase/Attribute.h"
cond::MetaData::MetaData(const std::string& connectionString, seal::Context* context):m_loader(0),m_con(connectionString),m_context(context),m_sessionProxy(0),m_mode(cond::ReadWriteCreate){
  this->init();
}
cond::MetaData::MetaData(const std::string& connectionString):m_loader(new cond::ServiceLoader),m_con(connectionString),m_context(0),m_sessionProxy(0),m_mode(cond::ReadWriteCreate){
  m_loader->useOwnContext();
  m_context=m_loader->context();
  this->init();
}
seal::Context* cond::MetaData::context(){
  return m_context;
}
void cond::MetaData::init(){
  if(m_loader){
    m_loader->loadMessageService();
    m_loader->loadConnectionService();
    m_loader->loadRelationalService();
  }
  seal::IHandle<coral::IConnectionService> iHandle =
    m_context->query<coral::IConnectionService>( "CORAL/Services/ConnectionService" ); 
  if( !iHandle ){
    throw cond::Exception("ConnectionService is not loaded");
  }
  coral::IConnectionServiceConfiguration& config = iHandle->configuration();
  config.setConnectionRetrialPeriod(1);
  config.setConnectionRetrialTimeOut(10);
  config.enableReadOnlySessionOnUpdateConnections();
  config.setDefaultAuthenticationService( "CORAL/Services/EnvironmentAuthenticationService" );   
}
cond::MetaData::~MetaData(){
}
void cond::MetaData::connect( cond::ConnectMode mod ){
  m_mode=mod;
  seal::IHandle<coral::IConnectionService> iHandle =
    m_context->query<coral::IConnectionService>( "CORAL/Services/ConnectionService" ); 
  if(mod==cond::ReadOnly){
    m_sessionProxy=iHandle->connect(m_con,coral::ReadOnly);
  }else{
    m_sessionProxy=iHandle->connect(m_con);
  }
}
void cond::MetaData::disconnect(){
  delete m_sessionProxy;
  m_sessionProxy=0;
}
bool cond::MetaData::addMapping(const std::string& name, const std::string& iovtoken){
  try{
    m_sessionProxy->transaction().start(false);
    try{
      this->createTable( cond::MetaDataNames::metadataTable() );
    }catch( const coral::TableAlreadyExistingException& er ){
      //std::cout<<"table alreay existing, not creating a new one"<<std::endl;
    }catch(...){
      throw;
    }
    coral::ITable& mytable=m_sessionProxy->nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::AttributeList rowBuffer;
    coral::ITableDataEditor& dataEditor = mytable.dataEditor();
    dataEditor.rowBuffer( rowBuffer );
    rowBuffer[cond::MetaDataNames::tagColumn()].data<std::string>()=name;
    rowBuffer[cond::MetaDataNames::tokenColumn()].data<std::string>()=iovtoken;
    dataEditor.insertRow( rowBuffer );
    m_sessionProxy->transaction().commit() ;
  }catch( const coral::DuplicateEntryInUniqueKeyException& er ){
    m_sessionProxy->transaction().rollback() ;
    throw cond::MetaDataDuplicateEntryException("addMapping",name);
  }catch(std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception(er.what());
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::addMapping Could not commit the transaction" );
  }
  return true;
}
bool cond::MetaData::replaceToken(const std::string& name, const std::string& newtoken){
  try{
    m_sessionProxy->transaction().start(false);
    if(!m_sessionProxy->nominalSchema().existsTable(cond::MetaDataNames::metadataTable())){
      throw cond::Exception( "MetaData::replaceToken MetaData table doesnot exist" );
    }
    coral::ITable& mytable=m_sessionProxy->nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
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
    m_sessionProxy->transaction().commit() ;
  }catch( coral::DuplicateEntryInUniqueKeyException& er ){
    m_sessionProxy->transaction().rollback();
    throw cond::MetaDataDuplicateEntryException("MetaData::replaceToken",name);
  }catch(std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception(er.what());
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::replaceToken Could not commit the transaction" );
  }
  return true;
}
const std::string cond::MetaData::getToken( const std::string& name ){
  std::string iovtoken;
  try{
    if( m_mode!=cond::ReadOnly ){
      m_sessionProxy->transaction().start(false);
    }else{
      m_sessionProxy->transaction().start(true);
    }
    coral::ITable& mytable=m_sessionProxy->nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    query->setRowCacheSize( 100 );
    coral::AttributeList emptyBindVariableList;
    std::string condition=cond::MetaDataNames::tagColumn()+" = '"+name+"'";
    query->setCondition( condition, emptyBindVariableList );
    query->addToOutputList( cond::MetaDataNames::tokenColumn() );
    coral::ICursor& cursor = query->execute();
    while( cursor.next() ) {
      const coral::AttributeList& row = cursor.currentRow();
      iovtoken=row[ cond::MetaDataNames::tokenColumn() ].data<std::string>();
    }
    m_sessionProxy->transaction().commit();
  }catch(const coral::TableNotExistingException& er){
    m_sessionProxy->transaction().commit();
    return "";
  }catch(const std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( std::string("MetaData::getToken error: ")+er.what() );
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::getToken: unknow exception" );
  }
  return iovtoken;
}
void cond::MetaData::createTable(const std::string& tabname){
  coral::ISchema& schema=m_sessionProxy->nominalSchema();
  coral::TableDescription description;
  description.setName( tabname );
  description.insertColumn(  cond::MetaDataNames::tagColumn(), coral::AttributeSpecification::typeNameForId( typeid(std::string)) );
  description.insertColumn( cond::MetaDataNames::tokenColumn(), coral::AttributeSpecification::typeNameForId( typeid(std::string)) );
  std::vector<std::string> cols;
  cols.push_back( cond::MetaDataNames::tagColumn() );
  description.setPrimaryKey(cols);
  description.setNotNullConstraint( cond::MetaDataNames::tokenColumn() );
  coral::ITable& table=schema.createTable(description);
  table.privilegeManager().grantToPublic( coral::ITablePrivilegeManager::Select);
}
bool cond::MetaData::hasTag( const std::string& name ) const{
  bool result=false;
  try{
    if( m_mode!=cond::ReadOnly ){
      m_sessionProxy->transaction().start(false);
    }else{
      m_sessionProxy->transaction().start(true);
    }
    coral::ITable& mytable=m_sessionProxy->nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    coral::AttributeList emptyBindVariableList;
    std::string condition=cond::MetaDataNames::tagColumn()+" = '"+name+"'";
    query->setCondition( condition, emptyBindVariableList );
    coral::ICursor& cursor = query->execute();
    if( cursor.next() ) result=true;
    cursor.close();
    m_sessionProxy->transaction().commit();
  }catch(const coral::TableNotExistingException& er){
    m_sessionProxy->transaction().commit();
    return false;
  }catch(const std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( std::string("MetaData::hasTag: " )+er.what() );
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::hasTag: unknown exception ");
  }
  return result;
}
void cond::MetaData::listAllTags( std::vector<std::string>& result ) const{
  try{
    if( m_mode!=cond::ReadOnly ){
      m_sessionProxy->transaction().start(false);
    }else{
      m_sessionProxy->transaction().start(true);
    }
    coral::ITable& mytable=m_sessionProxy->nominalSchema().tableHandle( cond::MetaDataNames::metadataTable() );
    std::auto_ptr< coral::IQuery > query(mytable.newQuery());
    query->addToOutputList( cond::MetaDataNames::tagColumn() );
    query->setMemoryCacheSize( 100 );
    coral::ICursor& cursor = query->execute();
    while( cursor.next() ){
      const coral::AttributeList& row = cursor.currentRow();
      result.push_back(row[cond::MetaDataNames::tagColumn()].data<std::string>());
    }
    cursor.close();
    m_sessionProxy->transaction().commit();
  }catch(const coral::TableNotExistingException& er){
    m_sessionProxy->transaction().commit();
    return;
  }catch(const std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( std::string("MetaData::listAllTag: " )+er.what() );
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::listAllTag: unknown exception ");
  }
}
void cond::MetaData::deleteAllEntries(){
  coral::AttributeList emptybinddata;
  try{
    m_sessionProxy->transaction().start(false);
    coral::ITable& table=m_sessionProxy->nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::ITableDataEditor& dataEditor = table.dataEditor();
    dataEditor.deleteRows( "",emptybinddata ); 
    m_sessionProxy->transaction().commit();
  }catch(const coral::TableNotExistingException& er){
    m_sessionProxy->transaction().rollback();
    return;
  }catch(const std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( std::string("MetaData::deleteAllEntries: " )+er.what() );
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::deleteAllEntries: unknown exception ");
  }
}
void cond::MetaData::deleteEntryByToken( const std::string& token ){
  coral::AttributeList deletecondition;
  deletecondition.extend( cond::MetaDataNames::tokenColumn(), typeid(std::string) );
  deletecondition[cond::MetaDataNames::tokenColumn()].data<std::string>()=token;
  std::string whereClause=cond::MetaDataNames::tokenColumn()+"=:"+cond::MetaDataNames::tokenColumn();
  try{
    m_sessionProxy->transaction().start(false);
    coral::ITable& table=m_sessionProxy->nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::ITableDataEditor& dataEditor = table.dataEditor();
    dataEditor.deleteRows(whereClause,deletecondition ); 
    m_sessionProxy->transaction().commit();
  }catch(const coral::TableNotExistingException& er){
    m_sessionProxy->transaction().rollback();
    return;
  }catch(const std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( std::string("MetaData::deleteEntryByToken: " )+er.what() );
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::deleteEntryByToken: unknown exception ");
  }
}
void cond::MetaData::deleteEntryByTag( const std::string& tag ){
  coral::AttributeList deletecondition;
  deletecondition.extend( cond::MetaDataNames::tagColumn(), typeid(std::string) );
  deletecondition[cond::MetaDataNames::tagColumn()].data<std::string>()=tag;
  std::string whereClause=cond::MetaDataNames::tagColumn()+"=:"+cond::MetaDataNames::tagColumn();
  try{
    m_sessionProxy->transaction().start(false);
    coral::ITable& table=m_sessionProxy->nominalSchema().tableHandle(cond::MetaDataNames::metadataTable());
    coral::ITableDataEditor& dataEditor = table.dataEditor();
    dataEditor.deleteRows(whereClause,deletecondition ); 
    m_sessionProxy->transaction().commit();
  }catch(const coral::TableNotExistingException& er){
    m_sessionProxy->transaction().rollback();
    return;
  }catch(const std::exception& er){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( std::string("MetaData::deleteEntryByTag: " )+er.what() );
  }catch(...){
    m_sessionProxy->transaction().rollback() ;
    throw cond::Exception( "MetaData::deleteEntryByTag: unknown exception ");
  }
}
