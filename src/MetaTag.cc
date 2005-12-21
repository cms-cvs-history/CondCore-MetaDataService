#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/MetaDataService/interface/MetaDataNames.h"
#include "RelationalAccess/RelationalException.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/IRelationalSession.h"
#include "RelationalAccess/IRelationalTransaction.h"
#include "RelationalAccess/IRelationalSchema.h"
#include "RelationalAccess/IRelationalTable.h"
#include "RelationalAccess/RelationalEditableTableDescription.h"
#include "RelationalAccess/IRelationalTablePrivilegeManager.h"
#include "RelationalAccess/IRelationalPrimaryKey.h"
#include "RelationalAccess/IRelationalCursor.h"
#include "RelationalAccess/IRelationalQuery.h"
#include "RelationalAccess/IRelationalTableDataEditor.h"
#include "AttributeList/AttributeList.h"
#include "SealKernel/Context.h"
#include "SealKernel/Service.h"
#include "FWCore/Utilities/interface/Exception.h"
cond::MetaData::MetaData(const cond::DbSession& session):m_session(session),m_table(0){
  
}

cond::MetaData::~MetaData(){
}

void cond::MetaData::addEntry(const std::string& recordname, 
			      const cond::IOVMetaDataEntry& iovmetadata){
  m_session->session().transaction().start(false);  
  if(!m_session->session()->nominalSchema().existsTable(cond::MetaDataNames::metadataTable())){
    this->createTable(cond::MetaDataNames::metadataTable());
  }else{
    m_table=&(m_session->session().nominalSchema().tableHandle(cond::MetaDataNames::metadataTable()));
  }
  const coral::ITableDescription& desc=m_tabble->description();
  coral::ITableDataEditor& dataEditor = table.dataEditor();
  coral::AttributeList data;
  m_table->dataEditor().rowBuffer( data );
  data[cond::MetaDataNames::recordColumn()].data<std::string>()=recordname;
  data[cond::MetaDataNames::tokenColumn()].data<std::string>()=iovmetadata.token;
  data[cond::MetaDataNames::nameColumn()].data<std::string>()=iovmetadata.name;
  data[cond::MetaDataNames::timeColumn()].data<char>()=iovmetadata.timetype;
  dataEditor.insertRow( data );
  try{
    m_session->session().transaction().commit();  
  }catch(...){
    m_session->session().transaction().rollback();
    //printout what
    //rethrow as cms exception
    throw cms::Exception("cond::MetaData::addMapping Could not commit the transaction");
  }
}
void cond::MetaData::addEntries(cond::MetaData::RecordToMetaDataMapIterator& begIt, cond::MetaData::RecordToMetaDataMapIterator& endIt){
  m_session->session().transaction().start(false);  
  if(!m_session->session()->nominalSchema().existsTable(cond::MetaDataNames::metadataTable())){
    this->createTable(cond::MetaDataNames::metadataTable());
  }else{
    m_table=&(m_session->session().nominalSchema().tableHandle(cond::MetaDataNames::metadataTable()));
  }
  const coral::ITableDescription& desc=m_tabble->description();
  coral::ITableDataEditor& dataEditor = table.dataEditor();
  coral::AttributeList data;
  m_table->dataEditor().rowBuffer( data );
  long n=std::distance(begIt,endIt);
  coral::IBulkOperation* rowInserter = m_table.dataEditor().bulkInsert(data,n);
  for ( int i = 0; i < n; ++i ) {
    data[cond::MetaDataNames::recordColumn()].data<std::string>()=recordname;
    data[cond::MetaDataNames::tokenColumn()].data<std::string>()=iovmetadata.token;
    data[cond::MetaDataNames::nameColumn()].data<std::string>()=iovmetadata.name;
    data[cond::MetaDataNames::timeColumn()].data<char>()=iovmetadata.timetype;
    dataEditor.insertRow( data );
  }
  try{
    m_session->session().transaction().commit();  
  }catch(...){
    m_session->session().transaction().rollback();
    //printout what
    //rethrow as cms exception
    throw cms::Exception("cond::MetaData::addMapping Could not commit the transaction");
  }
}

const std::string cond::MetaData::getToken( const std::string& name ){
  (*m_log)<<seal::Msg::Debug<<"cond::MetaData::getToken "<<name<<seal::flush;
  if ( ! m_session->transaction().start() ) {
    throw cms::Exception( "cond::MetaData::getToken: Could not start a new transaction" );
  }
  if(!m_table){
    m_table=&(m_session->userSchema().tableHandle( cond::MetaDataNames::metadataTable() ));
  }
  std::string iovtoken;
  std::auto_ptr< pool::IRelationalQuery > query( m_table->createQuery() );
  query->setRowCacheSize( 10 );
  pool::AttributeList emptyBindVariableList;
  std::string condition=cond::MetaDataNames::tagColumn()+"='"+name+"'";
  query->setCondition( condition, emptyBindVariableList );
  query->addToOutputList( cond::MetaDataNames::tokenColumn() );
  pool::IRelationalCursor& cursor = query->process();
  if ( cursor.start() ) {
    while( cursor.next() ) {
      const pool::AttributeList& row = cursor.currentRow();
      for ( pool::AttributeList::const_iterator iColumn = row.begin();
	    iColumn != row.end(); ++iColumn ) {
	//std::cout << iColumn->spec().name() << " : " << iColumn->getValueAsString() << "\t";
	iovtoken=iColumn->getValueAsString();
      }
      //std::cout << std::endl;
    }
  }
  if ( ! m_session->transaction().commit() ) {
    throw cms::Exception( "cond::MetaData::getToken: Could not commit a transaction" );
  }
  return iovtoken;
}
void cond::MetaData::createTable(const std::string& tabname){
  //if ( ! m_session->transaction().start() ) {
  //  throw cms::Exception( "cond::MetaData::createTable Could not start transaction." );
  //}
  pool::IRelationalSchema& schema=m_session->userSchema();
  seal::IHandle<pool::IRelationalService> serviceHandle=pool::POOLContext::context()->query<pool::IRelationalService>( "POOL/Services/RelationalService" );
  pool::IRelationalDomain& domain = serviceHandle->domainForConnection(m_con);
  std::auto_ptr< pool::IRelationalEditableTableDescription > desc( new pool::RelationalAccess::RelationalEditableTableDescription( *m_log, domain.flavorName() ) );
  desc->insertColumn(  cond::MetaDataNames::tagColumn(), pool::AttributeStaticTypeInfo<std::string>::type_name() );
  desc->insertColumn( cond::MetaDataNames::tokenColumn(), pool::AttributeStaticTypeInfo<std::string>::type_name() );
  std::vector<std::string> cols;
  cols.push_back( cond::MetaDataNames::tagColumn() );
  desc->setPrimaryKey(cols);
  desc->setNotNullConstraint( cond::MetaDataNames::tokenColumn() );
  m_table=&(schema.createTable(tabname,*desc));  
  m_table->privilegeManager().grantToPublic( pool::IRelationalTablePrivilegeManager::SELECT );
  //if ( ! m_session->transaction().commit() ) {
  //  throw cms::Exception( "cond::MetaData::createTable: Could not commit a transaction" );
  //}
}
