1. Implement the ability to select a combination of channels, pmus or stations to accumulate.
2. Read through the various TODO comments in the code
  a. Setting the max data frame size based on the configuration. (Use configuration frame method.)
  b. ...
3. Implement Header Frame and Config3 frame for v2 (Nice to have)
4. Implement Version 3 of the standard. (Nice to have)
5. Expose the IEEE standard as python modules. (Nice to have)
6. Expose the Accumulator as python modules. (Nice to have)
7. Enhance the mock-pdc to create up to 500 mock PMUs.
8. Implement a way to read in a historical csv file, create a mock configuration based on columns, and create a mock pdc.

9. Implement Periodic Data Frame format (Low priority)
--------

Fix StatField test. Might need it in order to drop frames that are bad.
It looks like Command::new_turn_on_transmission() and other methods will need an idcode parameter. Should also fill in the time fields using the standard library.

Fix random config and data frame generation utilities. (Not passing tests)

Choose between TCP, UDP or TCP/UDP (TCP for command, header and config, UDP for data frames)

Update the C37118Timestamp Accumulator to also use a time_base field. Need to review the FRACSEC definition.
Create A C37118Phasor accumulator. Should have fields like input_format (polar, rectangular), output_format (polar, rectangular), PHUNIT, convert_flag. Option to convert all phasors to rectangular or polar.

Ability to connect to multiple individual PMUs. Use the individual idcode for each PMU. May need to create multiple instances of AccumulatorManager and accumulator config. Might need to request individual configuration frames for each PMU. (Could also create multiple instances of PDC buffer to handle each PMU.)

Need to add STAT Accumulator to indicate data validity.

Need to use FNOM and FREQ to determine absolute frequency. Option to choose between deviation from nominal and absolute frequency. FREQ is in mHZ

In Config Struct, Split FRACSEC into LEAP_BYTE and FRACSEC. Even though 2011 version doesn't have LEAP_BYTE defined in the table, it is still used in the standard and applies to 2024.

FRACSEC = ROUND((fractional second of time stamp) * TIME_BASE)
TIME = SOC + FRACSEC/TIME_BASE

For LEAP_BYTE, LEAP_BYTE should have struct arrow column to display the information. Same for time quality messages. Any bitfield that needs to be displayed should have a struct arrow column. Use bitfield structs for these instead of just u8 or u16.

Convert ID_CODE to STREAM_ID. They are equivalent between 2011 and 2024 versions. Perhaps in the DISPLAY implementation, you can choose between ID_CODE and STREAM_ID.

The STAT and STAT_FLAG defintions are the same between 2011 and 2024 versions. Perhaps in the DISPLAY implementation, you can choose between STAT and STAT_FLAG.

New frames/Commands in 2024 version:
Configure Stream, Rename signals, Error Respsone frame, DiscreteEvent data frame.
