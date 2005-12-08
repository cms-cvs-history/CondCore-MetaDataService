#include "CondCore/MetaDataService/interface/MetaDataNames.h"
static const std::string& cond::MetaDataNames::metadataTable(){
  static const std::string s_metadataTable("METADATA");
  return s_metadataTable;
}
static const std::string& cond::MetaDataNames::tagColumn(){
  static const std::string s_tagColumn("TAG");
  return s_tagColumn;
}
static const std::string& cond::MetaDataNames::tokenColumn(){
  static const std::string s_tokenColumn("TOKEN");
  return s_tokenColumn;
}
static const std::string& cond::MetaDataNames::timetypeColumn(){
  static const std::string s_timetypeColumn("TIMETYPE");
  return s_timetypeColumn;
}
