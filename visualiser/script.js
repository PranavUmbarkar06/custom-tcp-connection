// TCP Session Replay
// Reads sender_log.txt and receiver_log.txt and replays them one event at a time.
// Each log line is: <time_us> <EVENT> <TYPE> <NUM> <WINDOW>

var RECV_WINDOW = 10;   // same as WINDOW_SIZE in protocol.h

var events = [];        // all events from both logs, sorted by time
var step = 0;           // index of the next event to show
var timer = null;       // setInterval handle while playing
var startTime = 0;      // time of the first event

// Sender side state
var totalPackets = 0;
var sendBase = 0;
var nextSeq = 0;
var senderWindow = 10;
var lastAck = -1;

// Receiver side state
var slots = [];         // what seq is in each receive buffer slot (-1 = empty)
var expected = 0;
var written = 0;

// Envelopes still travelling in each lane
var inFlight = { sender: 0, receiver: 0 };


// ---------- Loading ----------

// Turn the text of one log file into a list of event objects
function parseLog(text, side) {
    var list = [];
    var lines = text.split("\n");
    for (var i = 0; i < lines.length; i++) {
        var parts = lines[i].trim().split(" ");
        if (parts.length < 5) continue;   // skip empty lines
        list.push({
            time: Number(parts[0]),
            side: side,
            event: parts[1],
            type: parts[2],
            num: Number(parts[3]),
            win: Number(parts[4]),
            lost: false,
            arrived: false
        });
    }
    return list;
}

function isSend(e) {
    return e.event == "SEND" || e.event == "RESEND";
}

// Put both logs together in time order and work out which packets were lost
function setupEvents(senderText, receiverText) {
    events = parseLog(senderText, "sender").concat(parseLog(receiverText, "receiver"));

    // Sort by time. If two events have the same time, show the SEND first.
    events.sort(function (a, b) {
        if (a.time != b.time) return a.time - b.time;
        return (isSend(b) ? 1 : 0) - (isSend(a) ? 1 : 0);
    });

    if (events.length == 0) {
        document.getElementById("loadMsg").innerHTML = "No events found in the logs.";
        return;
    }
    startTime = events[0].time;

    // Every sent packet starts as "lost".
    for (var i = 0; i < events.length; i++) {
        if (isSend(events[i])) events[i].lost = true;
    }

    // Each RECV belongs to the latest matching SEND before it from the other side.
    // (So if packet 3 is sent, lost, and resent, the RECV goes with the resend.)
    // Any SEND left without a RECV was lost.
    for (var i = 0; i < events.length; i++) {
        var r = events[i];
        if (r.event != "RECV") continue;

        for (var j = i - 1; j >= 0; j--) {
            var e = events[j];
            if (isSend(e) && e.lost && e.side != r.side && e.type == r.type && e.num == r.num) {
                e.lost = false;
                r.sendIndex = j;
                break;
            }
        }
    }

    // Find how many data packets the file was split into
    totalPackets = 0;
    for (var k = 0; k < events.length; k++) {
        if (events[k].event == "TOTAL") totalPackets = events[k].num;
    }

    document.getElementById("loadMsg").innerHTML =
        "Loaded " + events.length + " events (" + totalPackets + " data packets). Press Play or Step.";
    restart();
}

// Read the two files chosen by the user
function loadFiles() {
    var senderFile = document.getElementById("senderFile").files[0];
    var receiverFile = document.getElementById("receiverFile").files[0];

    if (!senderFile || !receiverFile) {
        document.getElementById("loadMsg").innerHTML = "Please choose both log files first.";
        return;
    }

    var reader1 = new FileReader();
    reader1.onload = function () {
        var senderText = reader1.result;

        var reader2 = new FileReader();
        reader2.onload = function () {
            setupEvents(senderText, reader2.result);
        };
        reader2.readAsText(receiverFile);
    };
    reader1.readAsText(senderFile);
}

function loadSample() {
    setupEvents(sampleSenderLog, sampleReceiverLog);
}


// ---------- Controls ----------

function play() {
    if (events.length == 0) return;
    if (timer != null) return;   // already playing
    var speed = Number(document.getElementById("speed").value);
    timer = setInterval(nextStep, speed);
}

function pause() {
    clearInterval(timer);
    timer = null;
}

// Restart the timer with the new speed if we are playing
function changeSpeed() {
    if (timer != null) {
        pause();
        play();
    }
}

function restart() {
    pause();
    step = 0;
    sendBase = 0;
    nextSeq = 0;
    senderWindow = 10;
    lastAck = -1;
    expected = 0;
    written = 0;
    inFlight.sender = 0;
    inFlight.receiver = 0;

    slots = [];
    for (var i = 0; i < RECV_WINDOW; i++) slots.push(-1);

    // Remove old envelopes
    var old = document.querySelectorAll(".envelope");
    for (var j = 0; j < old.length; j++) old[j].remove();

    document.getElementById("eventList").innerHTML = "";
    document.getElementById("senderState").innerHTML = "CLOSED";
    document.getElementById("receiverState").innerHTML = "LISTEN";
    document.getElementById("stepMsg").innerHTML = events.length ? "Ready. Press Play or Step." : "Load the logs to start.";
    drawBuffers();
}


// ---------- One step ----------

function nextStep() {
    if (step >= events.length) {
        pause();
        document.getElementById("stepMsg").innerHTML = "Replay finished.";
        return;
    }

    var e = events[step];
    var text = describe(e);

    if (isSend(e)) {
        if (e.event == "SEND" && e.type == "DATA" && e.num + 1 > nextSeq) nextSeq = e.num + 1;
        sendEnvelope(e);
    } else if (e.event == "RECV") {
        if (e.sendIndex != undefined) arriveEnvelope(events[e.sendIndex]);
        if (e.side == "sender" && e.type == "ACK") lastAck = e.num;
    } else if (e.event == "BASE") {
        sendBase = e.num;
        senderWindow = e.win;
    } else if (e.event == "STORE") {
        slots[e.num % RECV_WINDOW] = e.num;
    } else if (e.event == "FLUSH") {
        slots[e.num % RECV_WINDOW] = -1;
        expected = e.num + 1;
        written++;
    } else if (e.event == "STATE") {
        document.getElementById(e.side + "State").innerHTML = e.type;
    }

    // Show what happened
    var seconds = ((e.time - startTime) / 1000).toFixed(1);
    document.getElementById("stepMsg").innerHTML =
        "Step " + (step + 1) + " / " + events.length + " (t = " + seconds + " ms): " + text;
    addToList(text, e.lost);
    drawBuffers();

    step++;
}

// Make a sentence for an event
function describe(e) {
    var who = (e.side == "sender") ? "Sender" : "Receiver";
    var name = e.type + " " + e.num;
    if (e.type == "SYN" || e.type == "SYNACK" || e.type == "PAUSE" || e.type == "RESUME") name = e.type;

    if (e.event == "SEND") {
        var s = who + " sends " + name;
        if (e.type == "ACK" && e.win > 0) s += " (window " + e.win + ")";
        if (e.lost) s += " -- LOST on the way!";
        return s;
    }
    if (e.event == "RESEND") {
        return who + " retransmits " + name + (e.lost ? " -- LOST again!" : "");
    }
    if (e.event == "RECV") {
        var r = who + " receives " + name;
        if (e.side == "sender" && e.type == "ACK" && e.num == lastAck) r += " (duplicate ACK)";
        return r;
    }
    if (e.event == "BASE")  return "Sender window: base = " + e.num + ", window = " + e.win;
    if (e.event == "STORE") return "Receiver stores packet " + e.num + " in slot " + (e.num % RECV_WINDOW);
    if (e.event == "FLUSH") return "Receiver writes packet " + e.num + " to disk and frees its slot";
    if (e.event == "DROP")  return "Receiver drops packet " + e.num + " (bad checksum)";
    if (e.event == "STATE") return who + " state -> " + e.type;
    if (e.event == "TOTAL") return "Sender split the file into " + e.num + " packets";
    return e.event + " " + name;
}

function addToList(text, lost) {
    var list = document.getElementById("eventList");
    var old = list.querySelector(".current");
    if (old) old.className = old.className.replace("current", "");

    var li = document.createElement("li");
    li.innerHTML = text;
    li.className = "current" + (lost ? " lost" : "");
    list.appendChild(li);
    list.scrollTop = list.scrollHeight;
}


// ---------- Envelopes ----------

// Create an envelope and move it into the channel
function sendEnvelope(e) {
    var channel = document.getElementById("channel");
    var width = channel.clientWidth - 70;   // 66px envelope + border

    var env = document.createElement("div");
    env.className = "envelope " + e.type;
    if (e.event == "RESEND") env.className += " resend";
    env.innerHTML = "<span>" + e.type + " " + e.num + "</span>";

    // Sender's packets use the top lane, receiver's use the bottom lane
    var lane = e.side;
    var top = (lane == "sender") ? 30 : 140;
    var slot = inFlight[lane] % 6;
    inFlight[lane]++;

    // Start at the sender's edge (left) or receiver's edge (right)
    env.style.top = (top + (slot % 2) * 8) + "px";
    env.style.left = (lane == "sender") ? "0px" : width + "px";
    channel.appendChild(env);
    e.envelope = env;

    // Where it waits in the middle of the channel
    var middle;
    if (e.lost) {
        middle = width / 2;
    } else if (lane == "sender") {
        middle = width * (0.15 + slot * 0.12);
    } else {
        middle = width * (0.85 - slot * 0.12);
    }

    // Small delay so the CSS transition can start
    setTimeout(function () {
        env.style.left = middle + "px";
    }, 30);

    if (e.lost) {
        // Halfway there, the envelope disappears
        setTimeout(function () {
            env.className += " lost";
        }, 700);
        setTimeout(function () {
            env.remove();
            if (inFlight[lane] > 0) inFlight[lane]--;
        }, 1400);
    }
}

// Move the envelope to the other side, then remove it
function arriveEnvelope(sendEvent) {
    var env = sendEvent.envelope;
    if (!env) return;
    var channel = document.getElementById("channel");
    var width = channel.clientWidth - 70;
    var lane = sendEvent.side;

    env.style.left = (lane == "sender") ? width + "px" : "0px";
    sendEvent.envelope = null;
    if (inFlight[lane] > 0) inFlight[lane]--;

    setTimeout(function () {
        env.remove();
    }, 650);
}


// ---------- Buffers ----------

function drawBuffers() {
    // Sender buffer: one cell per data packet
    var html = "";
    for (var i = 0; i < totalPackets; i++) {
        var cls = "cell ";
        if (i < sendBase) cls += "acked";
        else if (i < nextSeq) cls += "sent";
        else cls += "notsent";
        if (i >= sendBase && i < sendBase + senderWindow) cls += " inwindow";
        html += "<div class='" + cls + "'>" + i + "</div>";
    }
    document.getElementById("senderBuffer").innerHTML = html;
    document.getElementById("senderInfo").innerHTML =
        "base = " + sendBase + ", next = " + nextSeq + ", window = " + senderWindow;

    // Receiver buffer: the 10 slots
    html = "";
    for (var j = 0; j < RECV_WINDOW; j++) {
        if (slots[j] == -1) html += "<div class='cell notsent'>-</div>";
        else html += "<div class='cell stored'>" + slots[j] + "</div>";
    }
    document.getElementById("receiverBuffer").innerHTML = html;
    document.getElementById("receiverInfo").innerHTML =
        "expected = " + expected + ", written to disk = " + written;
}

// Draw empty buffers when the page opens
restart();
