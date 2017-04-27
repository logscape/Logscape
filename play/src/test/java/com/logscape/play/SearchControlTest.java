package com.logscape.play;

import be.klak.junit.jasmine.JasmineSuite;
import be.klak.junit.jasmine.JasmineTestRunner;
import org.junit.runner.RunWith;

@RunWith(JasmineTestRunner.class)
@JasmineSuite(sources = { "libs/jquery/jquery.js", "libs/underscore/underscore-min.js", "logscape.js", "searchie.control.js"}, sourcesRootDir = "src/main/webapp/javascripts")
public class SearchControlTest {
}
