package scalate

import org.fusesource.scalate.TemplateEngine
import java.io.File
import org.fusesource.scalate.util.Logging
import sys.SystemProperties

class Boot(engine: TemplateEngine) extends Logging {

  def run: Unit = {
//    // lets change the workingDirectory
//    info("Setting Scalate Working Directory")
//    val properties = new SystemProperties
//    println("This is the value of test property" + properties.get("test.property"))
//    engine.workingDirectory = new File("myScalateWorkDir")
  }
}