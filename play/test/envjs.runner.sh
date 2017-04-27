#!/bin/bash

# cleanup previous test runs
rm -f *.xml

# fire up the envjs environment
java -cp test/ext/js.jar:./ext/jline.jar org.mozilla.javascript.tools.shell.Main -opt -1 envjs.bootstrap.js $@
