1. Implement the ability to select a combination of channels, pmus or stations to accumulate.
2. Read through the various TODO comments in the code
  a. Setting the max data frame size based on the configuration. (Use configuration frame method.)
  b. ...
3. Implement Header Frame and Config3 frame for v2
4. Implement Version 3 of the standard.
5. Expose the IEEE standard as python modules.
6. Expose the Accumulator as python modules.
7. Enhance the mock-pdc to create up to 500 mock PMUs.
8. Implement a way to read in a historical csv file, create a mock configuration based on columns, and create a mock pdc.


--------

Fix StatField test. Might need it in order to drop frames that are bad.
It looks like Command::new_turn_on_transmission() and other methods will need an idcode parameter. Should also fill in the time fields using the standard library.

Fix random config and data frame generation utilities. (Not passing tests)
