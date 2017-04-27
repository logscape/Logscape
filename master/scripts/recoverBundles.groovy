import java.io.*

def host
def port

if(args.length > 1){
	host = args[0]
	port = args[1]
}

else if(args.length == 1){
	host = args[0]
	port = "11000"
}

else if(args.length == 0){

	println "Checking the setup.conf to determine the correct host to collect the bundles from"
	def conf = new File("../etc/setup.conf")
	def hostRegex = ~/"MANAGEMENT_HOST" value="([a-zA-Z0-9]+)"/
	def portRegex = ~/"MANAGEMENT_PORT" value="([a-zA-Z0-9]+)"/


	conf.eachLine{ line ->
		def matcher = hostRegex.matcher(line)
		while (matcher.find()){
			host = matcher.group(1)
		}
		def matcher2 = portRegex.matcher(line)
		while (matcher2.find()){
			port = matcher2.group(1)
		}

	}
}

def String manager = "http://" + host + ":" + port + "/bundles/"


println("Downloading files from:" + manager)

if (manager.contains("localhost")) {
	println("You are targeting the localhost!")
}

def files = [ "boot.zip", "replicator-1.0.zip", "vs-log-1.0.zip", "lib-1.0.zip"]

for ( file in files) {
	def url = manager + file
	def destFilename = "../downloads/" + file
	println "Downloading " + file + "\tto " + destFilename
	def outFile = new File(destFilename).newOutputStream()
	outFile << new URL(manager + file).openStream()
	outFile.close()
}
