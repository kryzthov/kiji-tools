var globals = {
  debugDiv: document.getElementById("debug_div"),
};

/**
 * Logs a message in the debugging section.
 *
 * @param message Text message to append.
 */
function Log(message) {
  var div = globals.debugDiv
  div.appendChild(document.createElement("br"));
  div.appendChild(document.createTextNode(new Date() + ":" + message));
}

/**
 * Extracts a query parameter from the URL.
 *
 * @param name URL query parameter name.
 * @return URL query parameter value.
 */
function GetQueryParameter(name) {
  var regex = new RegExp('[?&]' + encodeURIComponent(name) + '=([^&]*)');
  if (name = regex.exec(location.search)) {
    return decodeURIComponent(name[1]);
  }
}

function WSTest() {
  var ws = new WebSocket("ws://localhost:8383/ws");
  ws.onopen();
}

// -----------------------------------------------------------------------------

/** JavaScript entry point. */
function Main() {
}

Main();
