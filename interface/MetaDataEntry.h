#ifndef COND_IOVMETADATAENTRY_H
#define COND_IOVMETADATAENTRY_H
#include <string>
namespace cond{
  struct IOVMetaDataEntry{
    std::string token;
    std::string name;
    char timetype;//0 runnumber; 1 timestamp
  };
}
#endif
