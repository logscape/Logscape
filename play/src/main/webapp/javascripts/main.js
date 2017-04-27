require.config({
  paths: {
    jquery: 'libs/jquery/jquery',
    underscore: 'libs/underscore/underscore'
  }

});

require([

  // Load our app module and pass it to our definition function
  'logscape',
], function(Logscape){
  // The "app" dependency is passed in as "App"
  Logscape.initialize();
});