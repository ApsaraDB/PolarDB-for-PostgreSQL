## HLC-based distributed transaction protocol

Hybrid logical clock (HLC) is a 64-bit clock that combines physical and logical clocks. Of the 64 bits, 2 bits are reserved to ensure compatibility. In the middle, 46 bits are set aside for physical clocks. The last 16 bits are used for logical clocks.  Each node maintains an HLC named max_ts and periodically performs the persistence operation. The maximum value of the timestamps recorded in the redo logs will be restored when nodes are restarted.

### Three clock operations of HLCs:：

* ClockCurrent reads the current clock. The maximum value between max_ts and the local physical clock local-phys-ts will be returned.
* ClockUpdate updates max_ts to the maximum value between max_ts and the timestamp.
* ClockAdvance returns a value that takes the maximum value of max_ts and local-phys-ts and then adds 1. The whole operation is locked to ensure the atomicity of transactions.

In the preceding operation, local-phys-ts derives its value from the physical clock (unit: milliseconds) of the local machine. local-phys-ts is shifted 16 bits to the left to align with max_ts because the last 16 bits of max_ts are used for logical clocks.  The physical clocks of different machines can be synchronized by using Network Time Protocol (NTP) or Precision Time Protocol (PTP) to ensure small clock skew.

<img src="hlc_2pc.png" alt="Two-phase commit based on HLC" width="600"/>

### Clock algorithms

* When a transaction starts, the coordinator assigns ClockCurrent to the transaction as startTS. After startTS is synchronized to each datanode, startTS is used to update the max-ts of the datanode.
* When a transaction is prepared, each participant calls ClockAdvance to obtain prepareTS and return it to the coordinator. The coordinator takes the maximum prepareTS as the commitTS to update its HLC, sends the commitTS to each participant to commit the transaction, and uses the commitTS to drag the HLC of each datanode forward.

### Visibility

Assume there are two concurrent transactions, T1 and T2, on datanode1.

* If T1 is not prepared and does not have a commit timestamp when T2 scans for modifications on T1, T1 will not be visible to T2.
* If T1 is in the prepared state when T2 scans for modifications on T1, T2 waits for T1 to be committed or aborted. If T1 is aborted, T1 will not be visible to T2. If T1 is committed, a commit timestamp is generated. Visibility is determined based on timestamps. If T2.start_ts is equal to or greater than T1.commit_ts, T1 will be visible to T2.
* If T1 has timestamps when T2 scans the modification of T1, visibility can be determined based on timestamps.


___

Copyright © Alibaba Group, Inc.