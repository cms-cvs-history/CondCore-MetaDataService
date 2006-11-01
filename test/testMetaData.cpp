#include "CondCore/DBCommon/interface/AuthenticationMethod.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "SealKernel/IMessageService.h"
#include <string>
#include <vector>
#include <iterator>
#include <algorithm>
#include <iostream>
int main(){
  ::putenv("CORAL_AUTH_USER=cms_xiezhen_dev");
  ::putenv("CORAL_AUTH_PASSWORD=xiezhen123");
  //loader->loadAuthenticationService(cond::Env);
  try{
    cond::MetaData metadata_svc("sqlite_file:pippo.db");
    seal::IHandle<seal::IMessageService> iHandle =
      metadata_svc.context()->query<seal::IMessageService>( "SEAL/Services/MessageService" ); 
    iHandle->setOutputLevel(seal::Msg::Debug);
    //cond::MetaData metadata_svc("oracle://devdb10/cms_xiezhen_dev", *loader);
    metadata_svc.connect();
    //metadata_svc.getToken("mytest2");
    std::string t1("token1");
    metadata_svc.addMapping("mytest1",t1);
    std::string t2("token2");
    metadata_svc.addMapping("mytest2",t2);
    std::string tok1=metadata_svc.getToken("mytest2");
    std::cout<<"got token1 "<<tok1<<std::endl;
    std::string tok2=metadata_svc.getToken("mytest2");
    std::cout<<"got token2 "<<tok2<<std::endl;
    std::string newtok2="newtoken2";
    metadata_svc.replaceToken("mytest2",newtok2);
    std::string mytok2=metadata_svc.getToken("mytest2");
    std::cout<<"get back new tok2 "<<newtok2<<" "<<mytok2<<std::endl;
    std::cout<<"tag exists mytest2 "<<metadata_svc.hasTag("mytest2")<<std::endl;
    std::cout<<"tag exists crap "<<metadata_svc.hasTag("crap")<<std::endl;
    std::vector<std::string> alltags;
    metadata_svc.listAllTags(alltags);
    std::copy (alltags.begin(),
	       alltags.end(),
	       std::ostream_iterator<std::string>(std::cout,"\n")
	       );
    metadata_svc.deleteAllEntries();
    metadata_svc.disconnect();
  }catch(cond::Exception& er){
    std::cout<<er.what()<<std::endl;
  }catch(std::exception& er){
    std::cout<<er.what()<<std::endl;
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
  }
  //delete loader;
}


