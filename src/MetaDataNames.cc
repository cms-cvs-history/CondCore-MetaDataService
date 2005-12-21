#include "CondCore/MetaDataService/interface/MetaDataNames.h"
const std::string& cond::MetaDataNames::metadataTable(){
  static const std::string s_metadataTable("METADATA");
  return s_metadataTable;
}
const std::string& cond::MetaDataNames::recordColumn(){
  static const std::string s_recordcolumn("RECORDNAME");
  return s_recordcolumn;
}
const std::string& cond::MetaDataNames::iovnameColumn(){
  static const std::string s_iovnamecolumn("IOVNAME");
  return s_iovnamecolumn;
}
const std::string& cond::MetaDataNames::iovtokenColumn(){
  static const std::string s_iovtokencolumn("TOKEN");
  return s_iovtokencolumn;
}
const std::string& cond::MetaDataNames::iovtimetypeColumn(){
  static const std::string s_iovtimetypecolumn("TIMETYPE");
  return s_iovtimetypecolumn;
}
const std::string& cond::MetaDataNames::metatagTable(){
  static const std::string s_metatagTable("METATAG");
  return s_metatagTable;
}
const std::string& cond::MetaDataNames::metatagidColumn(){
  static const std::string s_metatagidcolumn("METATAGID");
  return s_metatagidcolumn;
}
const std::string& cond::MetaDataNames::metatagnameColumn(){
  static const std::string s_metatagnamecolumn("METATAGNAME");
  return s_metatagnamecolumn;
}

