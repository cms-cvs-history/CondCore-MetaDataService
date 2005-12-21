#ifndef COND_METADATANAMES_H
#define COND_METADATANAMES_H
#include <string>
namespace cond{
  class MetaDataNames {
  public:
    MetaDataNames(){}
    static const std::string& metadataTable();
    static const std::string& recordColumn();
    static const std::string& iovnameColumn();
    static const std::string& iovtokenColumn();
    static const std::string& iovtimetypeColumn();
    static const std::string& metatagTable();
    static const std::string& metatagidColumn();
    static const std::string& metatagnameColumn();
  };
}
#endif
