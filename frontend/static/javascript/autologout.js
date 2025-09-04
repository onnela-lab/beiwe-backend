// modified from https://stackoverflow.com/questions/23023916/how-to-implement-auto-logout-in-javascript

// globals
var warningTimerID, timeoutTimerID, flasherTimerID, original_title, favicon, orig_favicon_href
var timeout_after_warning = 90 * 1000  // 90 seconds
var timeout_until_warning = (30 * 60 * 1000) - timeout_after_warning  // total 30 minutes
var flash_title = false

var listeners = [
    "mousemove",   // very aggressive, mouse-over even when not foreground
    "focus",       // focus is when swapped to or window clicked
    "mousedown",
    "keypress",
    "touchmove",
    "onscroll",
    "visibilitychange" // visibilitychange includes getting made foreground - cool
]

var foreground_message =
  "This tab has timed out. Select Cancel (Esc) to stay on this page, or OK (Enter) to go to the login page."

function startTimer() {
    // window.setTimeout returns an Id that can be used to start and stop a timer
    warningTimerID = window.setTimeout(show_warning_start_logout_timer, timeout_until_warning)
    // console.log("starting timer:", logoutTimeout / 1000, "warning timer start:", warningTimeout / 1000)
}

function show_warning_start_logout_timer() {
    // console.log("warning timer", logoutTimeout / 1000)
    window.clearTimeout(warningTimerID)
    timeoutTimerID = window.setTimeout(idleTimeout, timeout_after_warning)
    document.getElementById('logout_alert').hidden = false
    
    // the way alerts work means we never log out due to our on-visibilitychange event listener.
    if (document.hidden) {
        flash_title = true
        check_flasshing_title()
    }
}

function resetTimer() {
    window.clearTimeout(timeoutTimerID)
    window.clearTimeout(warningTimerID)
    document.getElementById('logout_alert').hidden = true
    flash_title = false
    check_flasshing_title()
    startTimer()
}

function check_flasshing_title() {
    if (!flash_title) {
        document.title = original_title
        favicon.href = orig_favicon_href
        return
    }
    if (document.title != original_title) {
        document.title = original_title
        favicon.href = orig_favicon_href
    } else {
        document.title = 'Logging out in 90 seconds!'
        favicon.href = "/static/images/exclamation_mark_red.png"
    }
    
    // the first flip is weirdly slow on some browsers.
    flasherTimerID = setTimeout(check_flasshing_title, 750)  // 3/4 second feels ok.
}

function idleTimeout() {  // Log out the user.
    for (let i = 0; i < listeners.length; i++) {  // remove event listeners, stop updating page.
        document.removeEventListener(listeners[i], resetTimer)
    }
    window.clearTimeout(flasherTimerID)
    window.clearTimeout(timeoutTimerID)
    window.clearTimeout(warningTimerID)
    
    flash_title = false  // stop flashing
    check_flasshing_title()
    document.title = "You have been logged out - " + original_title
    
    var alert_element = document.getElementById("logout_alert")
    alert_element.hidden = false
    alert_element.textContent = "You were logged out. Links will redirect to the login page."
    alert_element.classList.remove("logout-animate")  // stop flashing
    
    fetch('/logout', { method: 'GET' }).then(() => {
        if (confirm(foreground_message)) {
            window.location.href = '/'
        }
    })
}

function setupTimers () {
    // virtually all interaction - you need to not touch the computer or leave it in the background.
    for (let i = 0; i < listeners.length; i++) {
        document.addEventListener(listeners[i], resetTimer, false)
    }
    startTimer()
}

$(document).ready(function() {
    original_title = document.title
    favicon = document.querySelector("link[rel~='icon']")
    orig_favicon_href = favicon.href
    setupTimers()
})
