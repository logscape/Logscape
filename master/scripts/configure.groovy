#!/user/bin/end groovy
path = args[0]
if(args.length == 4){
    verbose = false
    agentType = args[1]
    managerHostName = args[2]
    managerPort = args[3]
    webPort = "8080"
} else if(args.length == 1){
    verbose = true
} else {
    println "Error, expecting 1 or 4 args."
    println "Automated usage = configure.bat/sh [agent role] [manager address] [manager port]"
    System.exit(1);
}

def agentTypes = [ "1":"dev.Management", "2":"dev.Forwarder", "3":"dev.IndexStore", "4":"dev.Indexer", "5":"dev.Failover" ]

def getInput(defaultValue) {
    value = new InputStreamReader(System.in).readLine();
    if (value == "") {
        if (defaultValue != "")
            value = defaultValue
        else {
            print("Invalid input, try again:")
            return getInput(defaultValue)
        }
    }
    return value.replaceAll(" ","_");
}

def getAgentType(userInput, agentTypes) {
    try {
        if (Integer.parseInt(userInput)){
            return agentTypes[userInput]
        }
    } catch (Throwable t){

        return userInput;
    }
}

if(verbose == true){
    println  "\nWelcome to the LogScape Installer"
    println "---------------------------------\n"
    println "Which type/role of agent would you like to configure?"
    println "   1. dev.Management   \t(Runs Management Services and WebConsole on default port:8080) (includes:SocketServer,IndexStore,SysLogServer)"
    println "   2. dev.Forwarder \t(Forward to Manager OR dedicated IndexStore instance)"
    println "   3. dev.IndexStore    \t(Receive forwarded data streams)\t- already run on Management node)"
    println "   4. dev.Indexer        \t(Index & Search)"
    println "   5. dev.Failover     \t(Run Failover instance - manual config of boot.properties needed)"
    print " (1-5) or enter another role <zone>.<subzone>.<role> (ending with Server or Indexer)? :"

    agentType = getInput("")


    managerHostName = "localhost"
    managerPort = "11000"
    webPort = "8080"

    if (agentType == "1") {
        println "\nManagement Configuration"
        println "---------------------\n"
        print "Choose the Dashboard Port or press [Enter] to accept 8080? "
        webPort = getInput("8080")

    } else {
        println "\nGeneral Configuration"
        println "---------------------\n"
        print "What is the Hostname/IpAddress where the LogScape Management is running: "
        managerHostName = getInput("")

    }

    print "\nEnter the Management port [Enter to accept 11000]? "
    managerPort = getInput("11000")
    while (!managerPort.isInteger()) {
        println "Enter a valid port number :"
        managerPort = getInput("11000")
    }

    if (agentType != "1") {
        println "\nProceeding with Network connectivity check...."
        String[] params = new String[3];
        params[0] = managerHostName
        params[1] = managerPort
        params[2] = "11000"

        def roots = ["."] as String[]
        def gse = new GroovyScriptEngine(roots);
        def binding = new Binding();
        binding.setVariable("args",params);
        gse.run("networkCheck.groovy" , binding);

        if ( ! binding['isSuccess'] ){
            println "\n** NetworkCheck FAILED **"
            println "Ensure Management is running, and firewalls allow for network connectivity in both directions [ThisHost:"+params[2]+"<->Manager:11000]"
            print "\nDo you wish to continue ? (y/n):"
            keepGoing = getInput("y")
            if (keepGoing != "y") {
                println "Quitting"
                System.exit(0)
            }

        } else {
            println "\n** NetworkCheck was SUCCESSFUL **"
        }
    }
}


println "\n\n--------------------- Values Entered ------"
println "AgentType:" + getAgentType(agentType,agentTypes)
println "WebPort:" +  webPort
println "Management:" +  managerHostName + ":" + managerPort


println "Creating back-up of setup.conf"
//path = ""
confFile = path + "setup.conf"
bakFile = path + "setup.conf.bak"

if (!new File(confFile).exists()) {
    println "Given Path:" + path + " -> " + new File(path).getAbsolutePath();
    println "ERROR: Cannot find conf file:" + new File(confFile).getAbsolutePath();
    System.exit(1);
}
new File(confFile).renameTo(new File(bakFile))
def lsHome = new File ( new File("..").getCanonicalPath() ).getCanonicalPath() .replace("\\","\\\\");
println "logscape_home=" + lsHome

// now filter the contents
new File(confFile).withWriter { file ->
    new File(bakFile).eachLine {
        line -> file.writeLine(
                line.
                        replaceFirst(".*key=\"vso.resource.type.*","\t\t<add key=\"vso.resource.type\" value=\"" + getAgentType(agentType,agentTypes) + "\"/>").
                        replaceFirst(".*key=\"agent.role.*","\t\t<add key=\"agent.role\" value=\"" + getAgentType(agentType,agentTypes) + "\"/>").
                        replaceFirst(".*MANAGEMENT_HOST\" value=.*", "\t\t<add key=\"MANAGEMENT_HOST\" value=\"" + managerHostName + "\"/>").
                        replaceFirst(".*MANAGEMENT_PORT\" value=.*", "\t\t<add key=\"MANAGEMENT_PORT\" value=\"" + managerPort + "\"/>").
                        replaceFirst(".*BOOT\" value=.*", "\t\t<add key=\"BOOT\" value=\"" + getAgentType(agentType,agentTypes) + "\"/>").
                        replaceFirst(".*LOGSCAPE_HOME\" value=.*", "\t\t<add key=\"LOGSCAPE_HOME\" value=\"" + lsHome + "\"/>").
                        replaceFirst(".*wrkdir\" value=.*", "\t\t<add key=\"wrkdir\" value=\"" + lsHome + "\"/>").
                        replaceFirst(".*vso.base.port\" value=.*", "\t\t<add key=\"vso.base.port\" value=\"" + managerPort + "\"/>").
                        replaceFirst(".*web.app.port.*value=.*",     "\t\t<add key=\"web.app.port\" value=\"" + webPort + "\"/>")

        )
    }
}







