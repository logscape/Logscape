package com.logscape.play;

import be.klak.junit.jasmine.JasmineSuite;
import be.klak.junit.jasmine.JasmineTestRunner;
import org.junit.runner.RunWith;

@RunWith(JasmineTestRunner.class)
@JasmineSuite(sources = { "libs/jquery/jquery.js", "logscape.js", "notifications.js","admin.js" }, sourcesRootDir = "src/main/webapp/javascripts")
public class AdminTest {
}
