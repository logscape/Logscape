vmargs=-Xms256M -Xmx1024M -XX:MaxPermSize=256m -XX:+HeapDumpOnOutOfMemoryError -Xrs  -XX:CompileCommand=exclude,jregex/Pretokenizer,next
#-Xdebug -Xrunjdwp:transport=dt_socket,address=8990,server=y,suspend=n
sysprops=-DLOGSCAPE -Dvso.base.port=13000 -Dvso.boot.lookup.replication.port=25000 -Dvso.lookup.peers=stcp://localhost:15000,stcp://localhost:25000 -Dvso.resource.type=Failover -Dlog4j.configuration=./agent-log4j.properties -Dweb.app.port=8090 -Dfile.encoding=ISO-8859-1 -Dvso.zone=LOGSCAPE1  

mainclass=com.liquidlabs.vso.VSOMain
sys-bundles=system-bundles
deploy=downloads
pre-cp=foo.jar
post-cp=blah.jar
