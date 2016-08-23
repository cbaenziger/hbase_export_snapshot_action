#!/bin/bash                                                               
 
service oozie status                                                      
service oozie stop                                                        
rm -rf /usr/hdp/current/oozie-server/libext/hbaseoozie-1.0-SNAPSHOT.jar   
cp /home/ubuntu/hbaseoozie-1.0-SNAPSHOT.jar /usr/hdp/current/oozie-server/libext/.     

/usr/hdp/current/oozie-server/bin/oozie-setup.sh prepare-war              

sudo service oozie start                                                  
service oozie status                                                      
