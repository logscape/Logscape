/**
 * ScrollView - jQuery plugin 0.1
 *
 * This plugin supplies contents view by grab and drag scroll.
 *
 * Copyright (c) 2009 Toshimitsu Takahashi
 *
 * Released under the MIT license.
 *
 * == Usage =======================
 *   // apply to block element.
 *   $("#map").scrollview();
 *
 *   // with setting grab and drag icon urls.
 *   //   grab: the cursor when mouse button is up.
 *   //   grabbing: the cursor when mouse button is down.
 *   //
 *   $("#map".scrollview({
 *     grab : "images/openhand.cur",
 *     grabbing : "images/closedhand.cur"
 *   });
 * ================================
 */

function ScrollView() {
    this.initialize.apply(this, arguments)
}
ScrollView.prototype = {
    initialize: function (container, config) {
        var gecko = navigator.userAgent.indexOf("Gecko/") != -1;
        var opera = navigator.userAgent.indexOf("Opera/") != -1;
        var mac = navigator.userAgent.indexOf("Mac OS") != -1;
        if (opera) {
            this.grab = "default";
            this.grabbing = "move";
        } else if (!(mac && gecko) && config) {
            if (config.grab) {
                this.grab = "url(\"" + config.grab + "\"),default";
            }
            if (config.grabbing) {
                this.grabbing = "url(" + config.grabbing + "),move";
            }
        } else if (gecko) {
            this.grab = "-moz-grab";
            this.grabbing = "-moz-grabbing";
        } else {
            this.grab = "default";
            this.grabbing = "move";
        }

        this.callback = config.callback

        // Get container and image.
        this.m = $(container);
        this.updateChildren()


        this.centering();
    },
    updateChildren: function () {
        console.log("Update Children")
        this.i = this.m.children();

        this.isgrabbing = false;

        // Set mouse events.
        var self = this;
        this.i.mousedown(function (e) {
            self.startgrab();
            this.xp = e.pageX;
            this.yp = e.pageY;
            var offX  = (e.offsetX || e.clientX - $(e.target).offset().left);
            this.offsetX = offX
            return true;
        }).mousemove(function (e) {
                if (!self.isgrabbing) return true;
                return true;
            })
            .mouseout(function () {
//                self.stopgrab()
            })
            .mouseup(function (e) {
                console.log(e)
                self.stopgrab()
                self.callback(this.xp - e.pageX, this.offsetX)
                return true;
            })
            .dblclick(function () {
                var _m = self.m;
                var off = _m.offset();
                var dx = this.xp - off.left - _m.width() / 2;
                if (dx < 0) {
                    dx = "+=" + dx + "px";
                } else {
                    dx = "-=" + -dx + "px";
                }
                var dy = this.yp - off.top - _m.height() / 2;
                if (dy < 0) {
                    dy = "+=" + dy + "px";
                } else {
                    dy = "-=" + -dy + "px";
                }
                _m.animate({ scrollLeft: dx, scrollTop: dy },
                    "normal", "swing");
            });
    },
    centering: function () {
        var _m = this.m;
        var w = this.i.width() - _m.width();
        var h = this.i.height() - _m.height();
        _m.scrollLeft(w / 2).scrollTop(h / 2);
    },
    startgrab: function () {
        console.log("Start grab")
        this.isgrabbing = true;
    },
    stopgrab: function () {
        console.log("Stop grab")
        this.isgrabbing = false;
    },
    scrollTo: function (dx, dy) {
        var _m = this.m;
        var x = _m.scrollLeft() + dx;
        var y = _m.scrollTop() + dy;
        console.log("scroll to x: " + x + " y: " + y)
        _m.scrollLeft(x).scrollTop(y);
    },
    destroy: function() {
        this.m = null;
        this.i = null;
        this.callback = null;

    }
};
