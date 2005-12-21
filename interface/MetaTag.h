#ifndef COND_METATAG_H
#define COND_METATAG_H
#include <string>
#include <vector>
namespace cond{
  class DbSession;
  /**
     @class MetaTag MetaTag.h
     @author Zhen Xie
  */
  class MetaTag{
  public:
    explicit MetaTag( const DbSession& session );
    virtual ~MetaTag();
    /// Writing operations
    /**
     * Assign a metatag to one iov tag 
     */
    void tagIOV(const std::string& metatagname, 
		const std::string& iovtagname);
    /**
     * Assign a metatag to an iov tag collection 
     */
    void tagIOVs(const std::string& metatagname, 
		 std::vector<std::string>& iovtagnames);
    /// Reading operations
    /**
     * fetch all record names
     */
    void allTagNames( std::vector<std::string>& metatagnames );
    /**
     * get record names associated with given meta tag 
     */
    void recordsInTag(const std::string& metatagname,
		      std::vector<std::string>& recordnames);
    
    /// Delete operations
    /**
     * delete all IOVMetaData entries associated with given meta tag name 
     */
    void deleteTAG(const std::string& metatagname );
  private:
    /**
     * create METATAG table if not existing. 
     */
    void createTable();
  private:
    DbSession& m_session;
    coral::IRelationalTable* m_table;
  };
}
