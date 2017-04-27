package test.feed.sample
/**
 * API methods for live feed handling
 * @param params
 */
received = 0;
def start(String... params){
    println "Starting bespoke script"
}
def stop(){
    println "Stopping bespoke script"

}
def handle(String alertName, String host, String file, String content, Map fields){
    if (received++ < 10) {
        println received + ": " + new Date() + " Host:" + host + " File:" + file + " Content:" + content
    }

}
/**
 * Must return this!
 */
this