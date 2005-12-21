#include "CondCore/MetaDataService/interface/DbSession.h"
#include "RelationalAccess/ISession.h"

cond::DbSession::DbSession(const std::string& contact)
  :m_con(contact), m_context(new seal::Context), m_property(new DbSessionProperty(this, m_context) ){
  try{
    this->init();
  }catch(){
  }
}
cond::DbSession::~DbSession(){
  delete m_context;
}
void cond::DbSession::connect(){
  m_session->startUserSession();
  m_session->connect();
}
void cond::DbSession::disconnect(){
  m_session->disconnect();
}
cond::DbSessionProperty& cond::DbSession::property() const {
  return m_property;
}
void cond::DbSession::init(){
  seal::PluginManager* pm = seal::PluginManager::get();
  pm->initialise();
  seal::Handle<seal::ComponentLoader> loader = new seal::ComponentLoader(m_context);
  loader->load( "SEAL/Services/MessageService" );
  loader->load( "CORAL/Services/RelationalService" );
  loader->load( "CORAL/Services/EnvironmentAuthenticationService" );//default
  std::vector< seal::IHandle<coral::IRelationalService> > v_svc;
  m_context->query( v_svc );
  if ( v_svc.empty() ) {
    throw std::runtime_error( "Could not locate the relational service" );
  }
  seal::IHandle<coral::IRelationalService>& relationalService = v_svc.front();
  m_session=relationalService->domainForConnection(m_con).newSession(m_con); 
}
coral::ISession& cond::DbSession::session(){
  return *m_session;
}
