
function chooseStoredTheme() {

    try {
        var currentTheme = "white";
        var storedTheme = localStorage.getItem( "logscape-theme-name" )
        if (storedTheme != null && storedTheme != currentTheme){
            console.log("StoredTheme was:" + storedTheme)
            switchTo(storedTheme)
        }
    } catch (err) {

    }
}
var whiteTheme = { DOTeditWorkspaceTitle : "color_black", HASHworkspace : "background-color_#FFF", HASHworkspace_controller: "color_black", DOT1click_here: "color_black"}
var blackTheme = { DOTeditWorkspaceTitle : "color_white", HASHworkspace : "background-color_#21252B", HASHworkspace_controller: "color_white", DOT1click_here: "color_white"}
var themes = { white: whiteTheme, black: blackTheme} ;

function toggleTheme() {
    // items to change
    // workspace bg, workspace title, controller text
    //
    var themeTo = "white"

    var currentTheme = "white"
    var storedTheme = localStorage.getItem( "logscape-theme-name" )
    if (storedTheme != null) currentTheme = storedTheme;


        var chooseTheme = "white"

    if (currentTheme == "white" || currentTheme == "rgb(0, 0, 0)") chooseTheme = "black"


    switchTo(chooseTheme)
}

function switchTo(name) {
    var theme = themes[name]
    for(var key in theme){
        var cssValue = theme[key].split("_");

        var nkey = key.replace("DOT",".")
        nkey = nkey.replace("HASH","#")

        $(nkey).css(cssValue[0].trim(), cssValue[1].trim())
    }
    localStorage.setItem( "logscape-theme-name", name)

}