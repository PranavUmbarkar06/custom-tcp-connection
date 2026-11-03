#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <thread>
#include <mutex>
#include <unordered_set>
#include <chrono>

// Socket Headers
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/time.h>

using namespace std;

// Basic Constants
#define PORT 8080
#define BUFFER_SIZE 1024
#define WINDOW_SIZE 10
#define DEFAULT_RTO_MS 1000

// Flow Control Modes
#define MODE_WINDOW_ADVERTISING 1
#define MODE_PAUSE_RESUME       2
#define MODE_PACKET_LOSS        3   // Receiver drops every 3rd data packet (uses Mode 1 flow control)

// Bitmask Flags
#define FLAG_SYN    1
#define FLAG_ACK    2
#define FLAG_EOF    4
#define FLAG_PAUSE  8
#define FLAG_RESUME 16
#define FLAG_PROBE  32
#define FLAG_FIN    64

// Connection Teardown Constants
#define MSL_MS          1000   // Maximum Segment Lifetime (TIME_WAIT lasts 2*MSL)
#define MAX_FIN_RETRIES 5      // Give up the close after this many FIN retransmissions
#define RECV_POLL_MS    100    // recvfrom timeout so threads can notice transfer_complete

// Header Structure
struct Header {
    int seq_num;        // Sequence number
    int ack_num;        // Acknowledgment number
    int flags;          // Control flags
    int window_size;    // Receiver window size
    int payload_len;    // Length of actual payload data
    unsigned int checksum; // Data checksum
};

// Full Packet (Header + Fixed Payload)
struct Packet {
    Header hdr;
    char payload[BUFFER_SIZE];
};

// Basic Checksum Calculator
inline unsigned int calculate_checksum(const char* data, int length) {
    unsigned int sum = 0;
    for (int i = 0; i < length; i++) {
        sum += (unsigned char)data[i];
    }
    return sum;
}

// Make recvfrom return after ms milliseconds instead of blocking forever
inline void set_recv_timeout(int sockfd, int ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

// ---------- Visualiser Log ----------
// Every line is: <time_us> <EVENT> <TYPE> <NUM> <WINDOW>
// The visualiser page (visualiser/index.html) reads this file and replays it.
ofstream vis_log;
mutex vis_mutex;

// Give a packet type name from its flags
inline string flag_name(int flags) {
    if ((flags & FLAG_SYN) && (flags & FLAG_ACK)) return "SYNACK";
    if (flags & FLAG_SYN)    return "SYN";
    if (flags & FLAG_FIN)    return "FIN";
    if (flags & FLAG_PAUSE)  return "PAUSE";
    if (flags & FLAG_RESUME) return "RESUME";
    if (flags & FLAG_PROBE)  return "PROBE";
    if (flags & FLAG_ACK)    return "ACK";
    return "DATA";
}

// Number written on the envelope: ACK number for ACKs, sequence number for everything else
inline int envelope_num(const Header& h) {
    return (flag_name(h.flags) == "ACK") ? h.ack_num : h.seq_num;
}

// Write one event line (time in microseconds so both programs share the same clock)
inline void log_event(const string& event, const string& type, int num = 0, int win = 0) {
    lock_guard<mutex> lock(vis_mutex);
    long long now_us = chrono::duration_cast<chrono::microseconds>(
                           chrono::system_clock::now().time_since_epoch()).count();
    vis_log << now_us << " " << event << " " << type << " " << num << " " << win << endl;
}

// Print a TCP state machine transition
inline void log_state(const string& side, string& state, const string& next_state) {
    cout << "[" << side << " State] " << state << " -> " << next_state << endl;
    log_event("STATE", next_state);
    state = next_state;
}

#endif // PROTOCOL_H
