#ifndef CondCore_MetaDataService_METADATA_H
#define CondCore_MetaDataService_METADATA_H
#include <string>
#include <memory>
#include <vector>
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "RelationalAccess/ISession.h"
namespace seal{
  class Context;
}
namespace coral{
  class ISessionProxy;
}
namespace cond{
  class ServiceLoader;
  class MetaData {
  public:
    explicit MetaData( const std::string& contact);
    MetaData(const std::string& contact, seal::Context* context);
    ~MetaData();
    void connect( cond::ConnectMode mod=cond::ReadWriteCreate );
    void disconnect();
    bool addMapping(const std::string& name, const std::string& token);
    bool replaceToken(const std::string& name, const std::string& newtoken);
    bool hasTag( const std::string& name ) const;
    void listAllTags( std::vector<std::string>& result ) const;
    const std::string getToken( const std::string& name );
    void deleteAllEntries();
    void deleteEntryByToken( const std::string& token );
    void deleteEntryByTag( const std::string& tag );
    seal::Context* context();
  private:
    void init();
    void createTable(const std::string& tabname);
    /// The service loader
    cond::ServiceLoader* m_loader;
    /// The connection string
    std::string m_con;
    /// The "service" context
    seal::Context* m_context;
    /// The session object in use
    coral::ISessionProxy* m_sessionProxy;
    /// connection mode
    cond::ConnectMode m_mode;
  };
}
#endif
