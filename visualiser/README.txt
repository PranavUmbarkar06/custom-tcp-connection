TCP Session Replay - how to use

1. Run the receiver (client) and the sender (server) like before.
   - The sender now also writes   server/sender_log.txt
   - The receiver now also writes client/receiver_log.txt
2. Open visualiser/index.html in a browser (double click it).
3. Pick both log files and press "Load Logs".
   (Or press "Use Sample Logs" to see a saved run where packet 3 gets lost.)
4. Press Play, Pause or Step.

Log line format: <time_us> <EVENT> <TYPE> <NUM> <WINDOW>
EVENT is one of SEND, RESEND, RECV, STORE, FLUSH, BASE, STATE, TOTAL, DROP.
A SEND with no matching RECV on the other side is shown as a lost envelope.
