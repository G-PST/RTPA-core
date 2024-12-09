#![allow(unused)]
use serde::ser::{SerializeStruct, Serializer};
use serde::{self, Deserialize, Serialize};
use std::collections::HashMap;
// GOAL: Turn Sequence of Bytes in TCP packets into IEEE C37.118.2 formatted structs.
// Define structures common to all frames

// Configuration Frames for PDU+PMUs
// Prefix Frame +
// PDCConfigFrame +
// [PMUFrame1, PMUFrame2,...] // Frames can be fragmented with many PMUs
// CHK - Cyclic Redundancy Check // If fragmented, last two bytes of last fragement contain the CHK.
// CRC-CCITT implementation based on IEEE C37.118.2-2011 Appendix B
pub fn calculate_crc(buffer: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &byte in buffer {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[derive(Debug, Clone, Serialize)]
pub struct PrefixFrame2011 {
    pub sync: u16, // Leading byte = AA hex,
    // second byte: Frame type and version
    // Bit7: reserved=0
    // Bits6-4:
    // 000: Data Frame
    // 001: Header Frame,
    // 010: Configuration Frame 1
    // 011: Configuration Frame 2
    // 101: Configuration Frame 3
    // 100: Command Frame
    // Bits 3-0: Version number in binary (1-15)
    // Version 1 (0001) for messages defined in IEEE Std C37.118-2005
    // Version 2 (0010) for messaged defined in IEEE STD C37.118.2-2011
    pub framesize: u16, // Total number of bytes in the frame including CHK
    pub idcode: u16,
    // Data stream id number
    pub soc: u32, // Time stamp in UNIX time base. Range is 136 years, rolls over in 2106 AD. Leap seconds not included.
    pub fracsec: u32, // Fraction of second and time quaility, time of measurement of data frames,
                  //or time of frame transmission for non-data frames
                  // Bits 31-24: Message Time Quality (TODO needs additional bit mapping)
                  // Bits 23-00: FRACSEC, 24 Bit integer, when divided by TIME_BASE yields actual fractional second. FRACSEC used in all
                  // messages to and from a given PMU shall use the same TIME_BASE that is provided in the configuration message from that PMU.
}
impl PrefixFrame2011 {
    pub fn to_hex(&self) -> [u8; 14] {
        let mut result = [0u8; 14];
        result[0..2].copy_from_slice(&self.sync.to_be_bytes());
        result[2..4].copy_from_slice(&self.framesize.to_be_bytes());
        result[4..6].copy_from_slice(&self.idcode.to_be_bytes());
        result[6..10].copy_from_slice(&self.soc.to_be_bytes());
        result[10..14].copy_from_slice(&self.fracsec.to_be_bytes());
        result
    }

    pub fn from_hex(bytes: &[u8; 14]) -> Result<Self, &'static str> {
        if bytes.len() != 14 {
            return Err("Invalid byte array length");
        }
        Ok(PrefixFrame2011 {
            sync: u16::from_be_bytes([bytes[0], bytes[1]]),
            framesize: u16::from_be_bytes([bytes[2], bytes[3]]),
            idcode: u16::from_be_bytes([bytes[4], bytes[5]]),
            soc: u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]),
            fracsec: u32::from_be_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]),
        })
    }
}

#[derive(Debug, Serialize)]
pub struct HeaderFrame2011 {
    pub prefix: PrefixFrame2011,
    pub data_source: [u8; 32], // Data source identifier 32 byte ASCII
    pub version: [u8; 4],      // Version of data file or stream 4 byte ASCII
    pub chk: u16,              // CRC-CCITT
}

// Command Dataframe struct based on 2011 standard
// Should have a simple IMPL interface to create the 7 basic commands.
// Skip the custom commands for now.
#[derive(Debug)]
pub struct CommandFrame2011 {
    pub prefix: PrefixFrame2011,
    pub command: u16,              // Command word
    pub extframe: Option<Vec<u8>>, // Optional extended frame data
    pub chk: u16,
}

impl CommandFrame2011 {
    pub fn new_turn_off_transmission(idcode: u16) -> Self {
        Self::new_command(idcode, 1)
    }

    pub fn new_turn_on_transmission(idcode: u16) -> Self {
        Self::new_command(idcode, 2)
    }

    pub fn new_send_header_frame(idcode: u16) -> Self {
        Self::new_command(idcode, 3)
    }

    pub fn new_send_config_frame1(idcode: u16) -> Self {
        Self::new_command(idcode, 4)
    }

    pub fn new_send_config_frame2(idcode: u16) -> Self {
        Self::new_command(idcode, 5)
    }

    pub fn new_send_config_frame3(idcode: u16) -> Self {
        Self::new_command(idcode, 6)
    }

    pub fn new_extended_frame(idcode: u16) -> Self {
        Self::new_command(idcode, 8)
    }

    // TODO, decide whether to fill in the value now,
    // or wait until the client sends over TCP.
    // Second option is most precise.
    // e.g. get time in seconds and fracsec, calc crc, to_hex, send.
    fn new_command(idcode: u16, command: u16) -> Self {
        let prefix = PrefixFrame2011 {
            sync: 0xAA41,  // Command frame sync
            framesize: 18, // Fixed size for basic command frame
            idcode,
            soc: 0,     // To be filled by sender
            fracsec: 0, // To be filled by sender
        };
        CommandFrame2011 {
            prefix,
            command,
            extframe: None,
            chk: 0,
        }
    }
    pub fn to_hex(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.prefix.to_hex());
        result.extend_from_slice(&self.command.to_be_bytes());
        if let Some(extframe) = &self.extframe {
            result.extend_from_slice(extframe);
        }
        let crc = calculate_crc(&result);
        result.extend_from_slice(&crc.to_be_bytes());
        result
    }
}

#[derive(Debug)]
pub enum PMUFrameType {
    Floating(PMUDataFrameFloatFreq2011),
    Fixed(PMUDataFrameFixedFreq2011),
}

#[derive(Debug)]
pub struct DataFrame2011 {
    pub prefix: PrefixFrame2011,
    pub data: Vec<PMUFrameType>, // Length of Vec is based on num phasors.
    pub chk: u16,
}

#[derive(Debug, PartialEq)]
pub enum PMUValues {
    Float(Vec<f32>),
    Fixed(Vec<i16>),
}
impl PMUValues {
    pub fn as_string(&self) -> String {
        match self {
            PMUValues::Float(values) => format!("Float values: {:?}", values),
            PMUValues::Fixed(values) => format!("Fixed values: {:?}", values),
        }
    }
}
// This frame is repeated for each PMU available.
// We leave the phasor, analog and digital fields as variable length byte arrays to be parsed later based on the format.
#[derive(Debug)]
pub struct PMUDataFrame<T> {
    // Header frame above plus the following
    // Each Vec<u8> field needs to be converted based on the per-field format determined by the configuration.
    pub stat: u16,        // Bit-mapped flags
    pub phasors: Vec<u8>, // or u64, Phasor Estimates, May be single phase or 3-phase postive, negative or zero sequence.
    // Four or 8 bytes each depending on the fixed 16-bit or floating point format used, as indicated by the FORMATE field.
    // in the configuration frame. The number of values is determined by the PHNMR field in configuration 1,2,3 frames.
    pub freq: T,         // or u32, 2 or 4 bytes, fixed or floating point.
    pub dfreq: T,        // or u32, 2 or 4 bytes, fixed or floating point.
    pub analog: Vec<u8>, // or u32, analog data, 2 or 4 bytes per value depending on fixed or floating point format used,
    // as indicated by the format field in configuration 1, 2, and 3 frames.
    // Number of values is determed by the ANNMR in configuration 1,2, and 3 frames.
    pub digital: Vec<u8>, // Digital data, usually representing 16 digital status points (channels).
                          // The number of values is determined by the DGNMR field in configuration 1, 2, and 3 frames.
}
impl<T> PMUDataFrame<T> {
    pub fn parse_phasors(&self, config: &PMUConfigurationFrame2011) -> Vec<PMUValues> {
        let mut values = Vec::new();
        let chunk_size = config.phasor_size();

        for chunk in self.phasors.chunks(chunk_size) {
            if config.format & 0x0002 != 0 {
                // Parse as floating point
                let float_values: Vec<f32> = chunk
                    .chunks(4)
                    .map(|bytes| f32::from_be_bytes(bytes.try_into().unwrap()))
                    .collect();
                values.push(PMUValues::Float(float_values));
            } else {
                // Parse as fixed point
                let fixed_values: Vec<i16> = chunk
                    .chunks(2)
                    .map(|bytes| i16::from_be_bytes(bytes.try_into().unwrap()))
                    .collect();
                values.push(PMUValues::Fixed(fixed_values));
            }
        }
        values
    }
    pub fn parse_analogs(&self, config: &PMUConfigurationFrame2011) -> PMUValues {
        if config.format & 0x0004 != 0 {
            // Parse as floating point
            let float_values: Vec<f32> = self
                .analog
                .chunks(4)
                .map(|bytes| f32::from_be_bytes(bytes.try_into().unwrap()))
                .collect();
            PMUValues::Float(float_values)
        } else {
            // Parse as fixed point
            let fixed_values: Vec<i16> = self
                .analog
                .chunks(2)
                .map(|bytes| i16::from_be_bytes(bytes.try_into().unwrap()))
                .collect();
            PMUValues::Fixed(fixed_values)
        }
    }
    pub fn parse_digitals(&self) -> Vec<u16> {
        self.digital
            .chunks(2)
            .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
            .collect()
    }
}

pub type PMUDataFrameFixedFreq2011 = PMUDataFrame<i16>;
pub type PMUDataFrameFloatFreq2011 = PMUDataFrame<f32>;

#[derive(Debug, Clone)]
pub enum ChannelDataType {
    PhasorFloat, // 8 bytes (magnitude + angle as f32)
    PhasorFixed, // 4 bytes (magnitude + angle as i16)
    AnalogFloat, // 4 bytes (f32)
    AnalogFixed, // 2 bytes (i16)
    Digital,     // 2 bytes (u16)
    FreqFloat,   // 4 bytes (f32)
    FreqFixed,   // 2 bytes (i16)
    DfreqFloat,  // 4 bytes (f32)
    DfreqFixed,  // 2 bytes (i16)
}
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub data_type: ChannelDataType,
    pub offset: usize, // Offset from start of PMU data section
    pub size: usize,   // Size in bytes
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigurationFrame1and2_2011 {
    pub prefix: PrefixFrame2011,
    pub time_base: u32, // Resolution of
    pub num_pmu: u16,
    // pmu_configs repeated num_pmu times.
    pub pmu_configs: Vec<PMUConfigurationFrame2011>,
    pub data_rate: i16, // Rate of Data Transmission.
    pub chk: u16,
}
impl ConfigurationFrame1and2_2011 {
    pub fn calc_data_frame_size(&self) -> usize {
        // We should be able to calculate the expected data frame size based on
        // num_pmu, and the values in each PMUConfigurationFrame
        // namely, format, phnmr, annmr,
        // there should also be a fixed amount of size to be added based
        // on common things like PrefixFrame, chk and others.
        // Common Frame Parts:
        // PrefixFrame (14 bytes) + CHK (2 bytes) = 16 bytes
        let mut total_size = 16;

        // For each PMU, we need to calculate the size of its data
        for pmu_config in &self.pmu_configs {
            // Each PMU starts with STAT (2 bytes)
            total_size += 2;

            // Add phasor data size
            total_size += pmu_config.phasor_size() * pmu_config.phnmr as usize;

            // Add FREQ/DFREQ size (both use same format)
            total_size += 2 * pmu_config.freq_dfreq_size();

            // Add analog values size
            total_size += pmu_config.analog_size() * pmu_config.annmr as usize;

            // Add digital status words (2 bytes each)
            total_size += 2 * pmu_config.dgnmr as usize;
        }

        total_size
    }
    pub fn get_channel_map(&self) -> HashMap<String, ChannelInfo> {
        let mut channel_map = HashMap::new();
        let mut current_offset = 2; // Start after STAT
        let prefix_offset = 14;

        for pmu_config in &self.pmu_configs {
            let station_name = String::from_utf8_lossy(&pmu_config.stn).trim().to_string();
            let channel_names = pmu_config.get_column_names();
            let id_code = pmu_config.idcode;
            // Add frequency and DFREQ channels
            let freq_type = if pmu_config.format & 0x0008 != 0 {
                ChannelDataType::FreqFloat
            } else {
                ChannelDataType::FreqFixed
            };
            let dfreq_type = if pmu_config.format & 0x0008 != 0 {
                ChannelDataType::DfreqFloat
            } else {
                ChannelDataType::DfreqFixed
            };

            // Add phasor channels
            let phasor_type = if pmu_config.format & 0x0002 != 0 {
                ChannelDataType::PhasorFloat
            } else {
                ChannelDataType::PhasorFixed
            };

            let phasor_size = pmu_config.phasor_size();
            for name in channel_names.iter().take(pmu_config.phnmr as usize) {
                channel_map.insert(
                    name.clone(),
                    ChannelInfo {
                        data_type: phasor_type.clone(),
                        offset: current_offset + prefix_offset,
                        size: phasor_size,
                    },
                );
                current_offset += phasor_size;
            }

            let freq_size = pmu_config.freq_dfreq_size();
            channel_map.insert(
                format!("{}_{}_FREQ", station_name, id_code),
                ChannelInfo {
                    data_type: freq_type,
                    offset: current_offset + prefix_offset,
                    size: freq_size,
                },
            );
            current_offset += freq_size;

            channel_map.insert(
                format!("{}_{}_DFREQ", station_name, id_code),
                ChannelInfo {
                    data_type: dfreq_type,
                    offset: current_offset + prefix_offset,
                    size: freq_size,
                },
            );
            current_offset += freq_size;

            // Add analog channels
            let analog_type = if pmu_config.format & 0x0004 != 0 {
                ChannelDataType::AnalogFloat
            } else {
                ChannelDataType::AnalogFixed
            };

            let analog_size = pmu_config.analog_size();
            for name in channel_names
                .iter()
                .skip(pmu_config.phnmr as usize) // skip the freq/dfreq values and the number of phasors
                .take(pmu_config.annmr as usize)
            {
                channel_map.insert(
                    name.clone(),
                    ChannelInfo {
                        data_type: analog_type.clone(),
                        offset: current_offset + prefix_offset,
                        size: analog_size,
                    },
                );
                current_offset += analog_size;
            }

            // Add digital channels
            for name in channel_names
                .iter()
                .skip(pmu_config.phnmr as usize + pmu_config.annmr as usize)
                .take(pmu_config.dgnmr as usize)
            {
                channel_map.insert(
                    name.clone(),
                    ChannelInfo {
                        data_type: ChannelDataType::Digital,
                        offset: current_offset + prefix_offset,
                        size: 2,
                    },
                );
                current_offset += 2;
            }
        }

        channel_map
    }
}
// This struct is repeated NUM_PMU times.
// For parsing entire configuration frame, need to take into account num_pmu.
#[derive(Debug, Clone)]
pub struct PMUConfigurationFrame2011 {
    pub stn: [u8; 16], // Station Name 16 bytes ASCII
    pub idcode: u16,   // Data source ID number, identifies source of each data block.
    pub format: u16,   // Data format within the data frame
    // 16-bit flag.
    // Bits 15-4: unused
    // Bit 3: 0=Freq/DFREQ 16-bit integer 1=Floating point
    // Bit 2: 0 = analogs 16-bit integer, 1=floating point
    // Bit 1: phasors 16-bit ineger, 1=floating point
    // Bit 0: phasor real and imaginary (rectangular), 1=magnitude and angle (polar)
    pub phnmr: u16,     // Number of phasors - 2 byte integer
    pub annmr: u16,     // Number of analog values -  2 byte integer
    pub dgnmr: u16,     // number of digital status words - 2 byte integer
    pub chnam: Vec<u8>, // Length = 16 x (PHNMR+ANNMR + 16 x DGNMR)
    // Phasor and channel names, 16 bytes for each phasor analog and each digital channel.
    pub phunit: Vec<u32>, // length = 4 x PHNMR, Conversion factor for phasor channels
    pub anunit: Vec<u32>, // length = 4 x ANNMR, Conversion factor for Analog Channels
    pub digunit: Vec<u32>, // length = 4 x DGNMR, Mask words for digital status words
    pub fnom: u16,        // Nominal Frequency code and flags
    pub cfgcnt: u16,      // Configuration change count.
}
impl PMUConfigurationFrame2011 {
    pub fn freq_dfreq_size(&self) -> usize {
        if self.format & 0x0008 != 0 {
            4 // Floating point (4 bytes)
        } else {
            2 // 16-bit integer (2 bytes)
        }
    }

    pub fn analog_size(&self) -> usize {
        if self.format & 0x0004 != 0 {
            4 // Floating point (4 bytes)
        } else {
            2 // 16-bit integer (2 bytes)
        }
    }

    pub fn phasor_size(&self) -> usize {
        if self.format & 0x0002 != 0 {
            8 // Floating point (8 bytes)
        } else {
            4 // Fixed point (4 bytes)
        }
    }

    pub fn is_phasor_polar(&self) -> bool {
        self.format & 0x0001 != 0
    }
    pub fn get_column_names(&self) -> Vec<String> {
        let mut channel_names = Vec::new();
        let station_name = String::from_utf8_lossy(&self.stn).trim().to_string();

        for chunk in self.chnam.chunks(16) {
            let channel = String::from_utf8_lossy(chunk).trim().to_string();
            let full_name = format!("{}_{}_{}", station_name, self.idcode, channel);
            channel_names.push(full_name);
        }

        channel_names
    }
    pub fn get_phasor_columns(&self) -> Vec<String> {
        let mut channel_names = Vec::new();
        let station_name = String::from_utf8_lossy(&self.stn).trim().to_string();

        for chunk in self.chnam.chunks(16).take(self.phnmr as usize) {
            let channel = String::from_utf8_lossy(chunk).trim().to_string();
            let full_name = format!("{}_{}_{}", station_name, self.idcode, channel);
            channel_names.push(full_name);
        }

        channel_names
    }

    pub fn get_analog_columns(&self) -> Vec<String> {
        let mut channel_names = Vec::new();
        let station_name = String::from_utf8_lossy(&self.stn).trim().to_string();

        for chunk in self
            .chnam
            .chunks(16)
            .skip(self.phnmr as usize)
            .take(self.annmr as usize)
        {
            let channel = String::from_utf8_lossy(chunk).trim().to_string();
            let full_name = format!("{}_{}_{}", station_name, self.idcode, channel);
            channel_names.push(full_name);
        }

        channel_names
    }

    pub fn get_digital_columns(&self) -> Vec<String> {
        let mut channel_names = Vec::new();
        let station_name = String::from_utf8_lossy(&self.stn).trim().to_string();

        for chunk in self
            .chnam
            .chunks(16)
            .skip((self.phnmr + self.annmr) as usize)
            .take(self.dgnmr as usize)
        {
            let channel = String::from_utf8_lossy(chunk).trim().to_string();
            let full_name = format!("{}_{}_{}", station_name, self.idcode, channel);
            channel_names.push(full_name);
        }

        channel_names
    }
}

// Implement custom serialization
impl Serialize for PMUConfigurationFrame2011 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Calculate number of fields we'll serialize
        let num_fields = 11; // Adjust based on number of fields we're serializing

        let mut state = serializer.serialize_struct("PMUConfigurationFrame2011", num_fields)?;

        // Convert station name from [u8; 16] to String, trimming nulls and whitespace
        let stn_str = String::from_utf8_lossy(&self.stn)
            .trim_end_matches(char::from(0))
            .trim()
            .to_string();

        state.serialize_field("stn", &stn_str)?;
        state.serialize_field("idcode", &self.idcode)?;
        state.serialize_field("format", &self.format)?;
        state.serialize_field("phnmr", &self.phnmr)?;
        state.serialize_field("annmr", &self.annmr)?;
        state.serialize_field("dgnmr", &self.dgnmr)?;

        // Use get_column_names() instead of raw chnam
        let channel_names = self.get_column_names();
        state.serialize_field("channels", &channel_names)?;

        // You might want to add more meaningful representations of these units
        state.serialize_field("phunit", &self.phunit)?;
        state.serialize_field("anunit", &self.anunit)?;
        state.serialize_field("digunit", &self.digunit)?;
        state.serialize_field("fnom", &self.fnom)?;
        state.serialize_field("cfgcnt", &self.cfgcnt)?;

        // Add some computed properties that might be useful
        state.serialize_field("is_polar", &self.is_phasor_polar())?;

        // Add format flags as readable booleans
        let format_flags = FormatFlags {
            freq_dfreq_float: self.format & 0x0008 != 0,
            analog_float: self.format & 0x0004 != 0,
            phasor_float: self.format & 0x0002 != 0,
            phasor_polar: self.format & 0x0001 != 0,
        };
        state.serialize_field("format_flags", &format_flags)?;

        state.end()
    }
}

// Helper structure for format flags
#[derive(Serialize)]
struct FormatFlags {
    freq_dfreq_float: bool,
    analog_float: bool,
    phasor_float: bool,
    phasor_polar: bool,
}
