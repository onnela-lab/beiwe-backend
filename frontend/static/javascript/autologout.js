// modified from https://stackoverflow.com/questions/23023916/how-to-implement-auto-logout-in-javascript
var warningTimerID, timeoutTimerID, original_title, favicon, orig_favicon_href
var logoutTimeout = 30 * 60 * 1000
var warningTimeout = 1 * 60 * 1000
var flash_title = false

function startTimer() {
    // window.setTimeout returns an Id that can be used to start and stop a timer
    warningTimerID = window.setTimeout(show_warning_start_logout_timer, warningTimeout)
    // console.log("starting timer:", logoutTimeout / 1000, "warning timer start:", warningTimeout / 1000)
}

function show_warning_start_logout_timer() {
    // console.log("warning timer", logoutTimeout / 1000)
    window.clearTimeout(warningTimerID)
    timeoutTimerID = window.setTimeout(IdleTimeout, logoutTimeout)
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
        document.title = 'Logging out in 60 seconds!'
        favicon.href = "/static/images/exclamation_mark_red.png"
    }
    setTimeout(check_flasshing_title, 750)  // this isn't consistently times on the first flip...
}

function IdleTimeout() {
    // Logout the user.
    fetch('/logout', { method: 'GET' }).then(() => {
        window.location.href = '/'
    })
}

function setupTimers () {
    // virtually all interaction - you need to not touch the computer or leave it in the background.
    document.addEventListener("mousemove", resetTimer , false) // very aggressive, mouse-over even when not foreground
    document.addEventListener("focus", resetTimer, false) // focus is when swapped to or window clicked
    document.addEventListener("mousedown", resetTimer, false)
    document.addEventListener("keypress", resetTimer, false)
    document.addEventListener("touchmove", resetTimer, false)
    document.addEventListener("onscroll", resetTimer, false)
    // visibilitychange includes getting made foreground - cool
    document.addEventListener("visibilitychange", resetTimer, false)
    startTimer()
}

$(document).ready(function(){
    original_title = document.title
    favicon = document.querySelector("link[rel~='icon']")
    orig_favicon_href = favicon.href
    setupTimers()
})
