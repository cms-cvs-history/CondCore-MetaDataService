#ifndef COND_METADATA_H
#define COND_METADATA_H
#include <string>
#include <map>
#include <vector>
#include "CondCore/MetaDataService/MetaDataEntry.h"
namespace coral{
  class IRelationalTable;
}
namespace cond{
  class DbSession;
  /**
     @class MetaData MetaData.h
     @author Zhen Xie
  */
  class MetaData {
  public:
    typedef std::map<std::string, std::vector<cond::IOVMetaDataEntry> > RecordToIOVMetaDataMap;
    typedef RecordToIOVMetaDataMap::const_iterator RecordToIOVMetaDataMapIterator;
  public:
    /// Costructor
    explicit MetaData( const DbSession& session );
    /// Destructor
    virtual ~MetaData();
    /// Writing operations
    /**
     * Insert one IOVMetaDataEntry to a given record
     * 
     */
    void addEntry(const std::string& recordname,
		  const cond::IOVMetaDataEntry& iovmetadata);
    /**
     * Bulk insert Record to IOVMetaDataEntry mapping
     * 
     */
    void addEntries( RecordToMetaDataMapIterator& begIt, 
		     RecordToMetaDataMapIterator& endIt);
    /// Reading operations
    /**
     * returns the number of IOVMetaDataEntries in db
     */
    long numberOfEntriesInDB();
    /**
     * returns the number of Records in db
     */
    long numberOfRecordsInDB();
    /**
     * set the parameters of the bulk reading operation,
     * e.g. allMapping, allRecordNames
     * limit: max number of records to retrieve, 
     *        default limit=-1 means all and offset parameter will be ignored
     * offset: starting record umber
     */
    void setReadLimit(int limit, int offset);
    /**
     * fetch all the RecordToIOVMetaData mapping
     */
    RecordToMetaDataMap& allMapping();
    /**
     * fetch all record names
     */
    void allRecordNames( std::vector<std::string>& records );
    /**
     * fetch all IOVMetaData entries associated with given record name 
     */
    void entriesInRecord( const std::string& recordname, 
			  std::vector<cond::IOVMetaDataEntry>& entries);
    /**
     * fetch the IOVMetaData entry associated with given iov name 
     */
    void entrybyName( const std::string& iovname, 
		      cond::IOVMetaDataEntry& entry);
    /// Delete operations
    /**
     * delete the IOVMetaData entry associated with given iov name 
     * if this is the last IOVMetaData entry associated with a record, 
     * then the record will be deleted as well
     */
    void deleteEntry(const std::string& iovname );
    /**
     * delete the Record associated with given record name 
     */
    void deleteRecord(const std::string& recordname );
    
  private:
    /**
     * create METADATA table if not existing. 
     */
    void createTable();
  private:
    mutable int m_limit;
    mutable int m_offset;
    const cond::DbSession& m_session;
    coral::IRelationalTable* m_table;
    RecordToMetaDataMap m_record2metadata;
  };
}
#endif
