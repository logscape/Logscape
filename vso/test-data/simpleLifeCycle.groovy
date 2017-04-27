import com.liquidlabs.common.*

/**
* Shows how a script can be hooked into Lifecycle management when the service has <forked>false</forked>
* It also shows how you can direct output to pout and perr streams that are written to the work/DemoApp-1.0/{date}/{service-name}.out/err files
*/
        // these lines are optional and allow the script to be run from the command line - within LogScape pout and perr are defined
		pout = pout == null ? new FileOutputStream("out.txt") : pout
		perr = perr == null ? new FileOutputStream("err.txt")  : perr
		 
		cmd  = "deployed-bundles/DemoApp-1.0/simpleLifecycle.cmd"
		
		pout << "Running:" + cmd + " Arg Length:" + args.length + "\n"
		pout << " Calling deF:" + printMe() + "\n"
		if (args.length > 0) {
			pout << " Arg[0]:" + args[0] + "\n"
		}
		
 	    try { 
 	    
 	        // you must 'def' the process so it has scope in the stop() method
 	    	def process = cmd.execute()
 	    	def oout = pout;
 	    	
			LifeCycle lifeCycle = new LifeCycle(){
			
				public void start() {
				}
		
				public void stop() {
					oout << "Stopping process:" + process + "\n";
					process.destroy()  
				}
			};

 	    	
			if (serviceManager != null) {
				pout << new Date().toString() + " Registering LifeCycle Listener\n"
				perr << new Date().toString() + " Register lifecycle listener\n"
			  	serviceManager.registerLifeCycleObject(lifeCycle)
			} else {
				pout << new Date().toString() + " CANNOT Register  LifeCycle Listener:\n"
			}
			
			// now we need to run the child process!!
			process.consumeProcessOutput(pout, perr)  
        	process.waitFor()
			
 	    	
 	    	 
 	    } catch (Exception e) { 
 	      perr << e.toString(); 
 	      return 
		}  
	
	pout << "printing to stdout:" + new Date()

	log.info("Running Simple Groovy - check the work/DemoApp-1.0/xxxx directory")
return "SimpleLifeCycleScript:" + new Date().toString() + "\n";


 
 def printMe() {
        return new Date().toString() + " printed from groovymethod.printme "
    }
