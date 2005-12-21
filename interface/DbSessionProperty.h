#ifndef COND_DBSESSIONPROPERTY_H
#define COND_DBSESSIONPROPERTY_H
namespace seal{
  class MessageStream;
  class Context;
}
namespace cond{
  class DbSessionProperty{
  public:
    DbSessionProperty(cond::DbSession* session, seal::Context);
    ~DbSessionProperty();
    void setMessageVerbosityLevel( int level );
    void setAuthenticationMethod( const std::string& authservicename );
  private:
    cond::DbSession* m_session;
    seal::Context* m_context;
  };
}
