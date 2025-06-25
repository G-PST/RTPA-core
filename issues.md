# Issue 1.
The pdc-listener is throwing CRC validation errors because multiple data packets are being recieved in the same socket buffer. This is fine but
we need to split the packets by the sync value AA or by using the framesize value. The same issue most likely occurs in the pdc buffer as well.

# Issue 2.
We are getting a panic at the arrow frame creation, listed in err.md, which needs to be handled properly.

# Issue 3.

the validate crc function panics.
