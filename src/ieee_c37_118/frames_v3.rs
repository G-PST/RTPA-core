#![allow(unused)]

#[derive(Debug)]
pub struct HeaderFrame2024 {
    pub sync: [u8; 2], // Synchronization bytes, using a u8[2] array here since the first and second byte are read separately.
    pub framesize: u16, // Frame size in bytes, Max=65535, TODO build a test for checking against out of range frames.
    pub stream_id: u16, // Data Stream ID, Identifies destination data stream for commands and source stream for other messages.
    pub soc: u32,       // Timestamp - Time since midnight 01-Jan-1970 (UNIX Time)
    pub leap_byte: u8, // Leap second information, Bit6-> 0=Add, 1=Delete, Bit5->1=Leap second occured, Bit4-> leap second pending
    pub fracsec: [u8; 3], // Fractional Part of a seconde multiplied by TIME_BASE? and rounded to nearest integer.
}

#[derive(Debug)]
struct ConfigTailFrame2024 {
    pub stream_data_rate: u16, // Rate of data transmission for the composite frame in stream. (See PMU_DATA_RATE)
    pub wait_time: u16,        // PDC wait time in milliseconds
    pub chk: u16, // CRC-CCITT (Cyclic Redundancy Check - ),uses polynomial x^16 + x^12 + x^5 + 1, TODO need research more.
}

// Additional Data structures for Configuration frames
// Everything in Common Data frame
#[derive(Debug)]
pub struct PDCConfigurationFrame2024 {
    pub cont_idx: u16, // Continuation index for fragmented frames, 0 no fragments, 1 first frag in series, ...
    pub time_base: u32, // Bits 31-24 reserved =0, Bits23-0 24-bit uint, subdivision of the second that FRACSEC is based on.
    pub pdc_name: String, // TODO: should be 1-256 bytes not sure how to parse if we don't know the length before hand.
    pub num_pmu: u16,     // Number of pmus included in the data frame.

    // ---- Repeated PMU Configuration Frames below for each PMU ---//
    pub stream_data_rate: u16, // Rate of data transmission for the composite frame in stream. (See PMU_DATA_RATE)
    pub wait_time: u16,        // PDC wait time in milliseconds
    pub chk: u16,              //
}

// Repeated NUM_PMU times
#[derive(Debug)]
pub struct PMUConfigurationFrame2024 {
    pub pmu_name: String,   // TODO: SHould be 1-256 bytes
    pub pmu_id: u16,        // 1-65534, 0 and 65535 are reserved
    pub pmu_version: u16,   // Bits 15-4 Reserved =0, Bits 3-0 Version Number from the SYNC word.
    pub g_pmu_id: [u32; 4], // Global PMU ID, Uses RFC 4122 big endian byte encoding.
    pub format: u16, // Bits 15-4 Reserved=0, Bit3=FREQ/DFREQ 0=16bity integer, 1=floating point
    // bit2 Analog 0=16bit int, 1 = floating point
    // bit1 Phasor (format) 0=int, 1=floating point
    // bit0 Phasor (encoding) 0=real and imaginary, 1=magnitude and angle (polar)
    pub phnmr: u16,    //Number of phasors
    pub annmr: u16,    //Number of analog values
    pub frnmr: u16,    //Number of frequency signals,
    pub dfdtnmr: u16,  //Number of df/dt signals,
    pub dgnmr: u16,    //Number of digital status words,
    pub chnam: String, //TODO 1-256 bytes, Phasor and channel name, minimum 2 bytes for each phasor, frequency, ROCOF, analog and digital channel.
    // Names are in the same order as they are transmitted. Re-read IEEE standard,
    pub phscale: [u16; 16], // 16xPHNMR, Conversion factor for phasor channels with flags. Magnitude, and angle scalling for phasors with data flags.
    // The factor has four 4-byte long words.
    // ---- First 4-byte word -----
    // First 2 bytes: 16-bit flag that indicates the type of data modification when data is being modified by a continuous process.
    // When no modification, all bits =0
    // Bit # meaning when bit is set
    // 0-reserved, 1-up sampled from lower rate, 2-downsampled from lower rate,
    // 3-Magnitude filtered, 4-Estimated magnitude, 5-estimated angle,
    // 6-Phasor magnitude adjusted for calibration, 7-phasor phase adjusted for calibration,
    // 8-phasor phase adjusted for offset (+/- 30deg, +/-120 deg, etc.)
    // 9-Psuedo-phasor value (combined from other phasors)
    // 10-14 reserved for future
    // 15 Modification applied. Type not defined here. ???

    // Third Byte: Phasor type indication (Bit 0 is LSb, Bit7 is the MSb)
    // Bits 07-04: reserved = 0
    // Bit 03: 0-voltage, 1-current
    // Bits 02-00: Phasor Component
    // 111: reserved, 110: phase C, 101: Phase B, 100: Phase A
    // 011: reserved, 010: Negative Sequence, 001: Positive Sequence, 000: Zero

    // Fourth Byte: Available for User designation

    // ---- Second and Third 4-byte words
    // Seconde 4 Byte word = Scale factor Y in 32-bit IEEE floating point.
    // Third 4 Byte word = phasor angle adjustment in radians.

    // ---- Fourthe 4 Byte word
    // Voltage class in 32-bit IEEE floating point format
    pub frscale: [u16; 8], //??? 8XFRNMR, Conversion factor for frequency channels
    // First 4 Bytes, magnitude scaling in 32bit floating point
    // Last 4 bytes, offset B in 32-bit floating point.
    pub dfdtscale: [u16; 8], //??? 8XDFDTNMR, conversion factor for ROCOF channels, Same as FRSCALE
    pub anscale: [u16; 8],   //??? 8XANNMR, conversion factor for annalog channels, same as FRSCALE
    pub digunit: [u16; 4],   //??? 4XDGNMR, Mask words for digital status words? TODO re-read
    pub pmu_lat: u32, // Latitude in Degrees, WGS84, -90 to 90, 32bit IEEE floating point, infinity for unspecified locations?
    pub pmu_lon: u32, // Longitude in Degress, WGS84, -179.99999999 to +180, 32bit IEEE floating point, unspecified=infinity
    pub pmu_elev: u32, // PMU elevation in meters, WGS84, Positive values for above mean sea level. IEEE 32 bit float, unspecified=infinity
    pub pmuflag: u16,  //
    // Bit15 1=PMU does not accept any configuration commands, 0=PMU accepts configuration commands.
    // Bit14 1=Data stream auto starts on power up., 0=Data Stream does not auto start on power up.
    // Bit13: 1=50hz, 0=60hz nominal frequency
    // Bit12: 1=Data attributes included in stream, 0=not included
    // Bit11: 1=Data available for retrieval at this PMU, 0=Not available
    // Bits10-4: Reserved- set to 0
    // Bits3-0: Data Filter used. 16 possible combinations
    // 0=P Class
    // 1=M Class
    // 2-7: Reserved
    // 8-15: User defined.
    pub window: i32, //Phasor measurement window length in microseconds, including all measurements and estimation windows in effect.
    // A value of -1 indictes the window length is not available
    pub grp_dly: i32, //Phasor measurement group delay (in microseconds) including all filters and estimation windows in effect.
    // A value of -1 indicates the group delay is not available.
    pub pmu_data_rate: i16, // Rate of data transmission for the PMU.
    // If PMU_DATA_RATE > 0, rate is # frames per second (15=15 frames per second.)
    // If PMU_DATA_RATE < 0, rate is negative of seconds per frame (-5 = 1 frame per 5 seconds)
    pub cfgcnt: u16, // Configuration change count. Value is incremented each time a change is made to PMU.
                     // 0 is factory default and initial value.
}
