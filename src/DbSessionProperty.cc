#include "CondCore/MetaDataService/interface/DbSessionProperty.h"
cond::DbSessionProperty::DbSessionProperty(DbSession* session, seal::Context* context):m_session(session), m_context(context){
  std::vector< seal::Handle<seal::IMessageService> > v_msgSvc;
  m_context->query( v_msgSvc );
  if ( ! v_msgSvc.empty() ) {
    seal::Handle<seal::IMessageService>& msgSvc = v_msgSvc.front();
    msgSvc->setOutputLevel( seal::Msg::Error); //default
  }
}
cond::DbSessionProperty::~DbSessionProperty(){
  
}
void cond::DbSessionProperty::setMessageVerbosityLevel( int level ){
  std::vector< seal::Handle<seal::IMessageService> > v_msgSvc;
  m_context->query( v_msgSvc );
  if ( ! v_msgSvc.empty() ) {
    seal::Handle<seal::IMessageService>& msgSvc = v_msgSvc.front();
    msgSvc->setOutputLevel( level );
  }
}
void cond::DbSessionProperty::setAuthenticationMethod(const std::string& authservicename){
  seal::Handle<seal::ComponentLoader> loader = new seal::ComponentLoader(m_context);
  loader->load( authservicename ); 
}
