#ifndef COND_DBSESSION_H
#define COND_DBSESSION_H
namespace coral{
  class ISession;
}
namespace cond{
  class DbSession{
    friend class MetaData;
    friend class MetaTag;
  public:
    DbSession(const std::string& contact, 
	      DbSessionProperty& property);
    ~DbSession();
    void connect();
    void disconnect();
    cond::DbSessionProperty& property() const;   
  private:
    coral::ISession& session();
  private:  
    std::string m_con;
    seal::Context* m_context;
    cond::DbSessionProperty& m_property;
    coral::ISession* m_session;
  };
}
