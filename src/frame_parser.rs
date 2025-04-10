#![allow(unused)]
use crate::frames::{
    calculate_crc, CommandFrame2011, ConfigurationFrame1and2_2011, DataFrame2011, HeaderFrame2011,
    PMUConfigurationFrame2011, PMUDataFrameFixedFreq2011, PMUDataFrameFloatFreq2011, PMUFrameType,
    PrefixFrame2011,
};

// Define constants
const PREFIX_SIZE: usize = 14; // Size of HeaderFrame2011 in bytes

#[derive(Debug)]
pub enum ParseError {
    InsufficientData,
    InvalidCRC,
    InvalidFrameSize,
    InvalidHeader,
    VersionNotSupported,
    NotImplemented,
}

#[derive(Debug)]
pub enum Frame {
    Header(HeaderFrame2011),
    Prefix(PrefixFrame2011),
    Configuration(ConfigurationFrame1and2_2011),
    Data(DataFrame2011),
    Command(CommandFrame2011),
}

pub fn parse_header(buffer: &[u8]) -> Result<HeaderFrame2011, ParseError> {
    todo!("Impement Header Frame parsing")
}

pub fn parse_command_frame(buffer: &[u8]) -> Result<Frame, ParseError> {
    // TODO/skip
    let prefix_slice: &[u8; PREFIX_SIZE] = buffer[..PREFIX_SIZE].try_into().unwrap();
    let prefix = PrefixFrame2011::from_hex(prefix_slice).map_err(|_| ParseError::InvalidHeader)?;

    let command = u16::from_be_bytes([buffer[PREFIX_SIZE], buffer[PREFIX_SIZE + 1]]);

    let extframe = if buffer.len() > PREFIX_SIZE + 4 {
        Some(buffer[PREFIX_SIZE + 2..buffer.len() - 2].to_vec())
    } else {
        None
    };

    let chk = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);

    Ok(Frame::Command(CommandFrame2011 {
        prefix,
        command,
        extframe,
        chk,
    }))
}

// NO longer needed. Instead we will use the configuration frame to
// generate Sparse Accumulators.
pub fn parse_data_frames(
    buffer: &[u8],
    config: &ConfigurationFrame1and2_2011,
) -> Result<DataFrame2011, ParseError> {
    // data frame parsing here.
    // TODO

    // First get prefix frame
    let prefix_slice: &[u8; PREFIX_SIZE] = buffer[..PREFIX_SIZE].try_into().unwrap();
    let prefix = PrefixFrame2011::from_hex(prefix_slice).map_err(|_| ParseError::InvalidHeader)?;

    let mut data: Vec<PMUFrameType> = Vec::new();

    // Make an offset variable and
    let mut offset = PREFIX_SIZE;

    // for config in config.pmu_configs.
    // Need to get data format, phnmr, annmr, dgnmr and format from
    // from each pmu configuration.
    // determine whether to use PMUDataFrameFloat2011
    // or PMUDataFrameInt2011.
    // Determine next N bytes to read and parse the individual PMU Frame.
    for config in &config.pmu_configs {
        // Values for each individual pmu
        let phnmr = config.phnmr;
        let annmr = config.annmr;
        let dgnmr = config.dgnmr;

        let phasor_datum_usize = config.phasor_size();
        let analog_datum_usize = config.analog_size();
        let freq_dfreq_usize = config.freq_dfreq_size();

        // phasor should be vec<u8> with length 2*phasor_datum_usize*phnmr
        // analog should be vec<u8> with length analog_datum_usize*annmr
        let pmu_frame = if freq_dfreq_usize == 2 {
            let stat = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
            offset += 2;

            let phasors = buffer[offset..offset + phasor_datum_usize * phnmr as usize].to_vec();
            offset += phasor_datum_usize * phnmr as usize;

            let freq = i16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
            offset += 2;

            let dfreq = i16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
            offset += 2;

            let analog = buffer[offset..offset + analog_datum_usize * annmr as usize].to_vec();
            offset += analog_datum_usize * annmr as usize;

            let digital = buffer[offset..offset + 2 * dgnmr as usize].to_vec();
            offset += 2 * dgnmr as usize;

            PMUFrameType::Fixed(PMUDataFrameFixedFreq2011 {
                stat,
                phasors,
                freq,
                dfreq,
                analog,
                digital,
            })
        } else {
            let stat = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
            offset += 2;

            let phasors = buffer[offset..offset + 2 * phasor_datum_usize * phnmr as usize].to_vec();
            offset += 2 * phasor_datum_usize * phnmr as usize;

            let freq = f32::from_be_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]);
            offset += 4;

            let dfreq = f32::from_be_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]);
            offset += 4;

            let analog = buffer[offset..offset + analog_datum_usize * annmr as usize].to_vec();
            offset += analog_datum_usize * annmr as usize;

            let digital = buffer[offset..offset + 2 * dgnmr as usize].to_vec();
            offset += 2 * dgnmr as usize;

            PMUFrameType::Floating(PMUDataFrameFloatFreq2011 {
                stat,
                phasors,
                freq,
                dfreq,
                analog,
                digital,
            })
        };
        data.push(pmu_frame);
    }
    // Read the CRC (chk) from the last two bytes of the buffer
    let chk = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);

    Ok(DataFrame2011 { prefix, data, chk })
}

pub fn parse_config_frame_1and2(buffer: &[u8]) -> Result<ConfigurationFrame1and2_2011, ParseError> {
    // get the header frame struct using the parse_header_frame function

    let prefix_slice: &[u8; PREFIX_SIZE] = buffer[..PREFIX_SIZE].try_into().unwrap();
    let common_header =
        PrefixFrame2011::from_hex(prefix_slice).map_err(|_| ParseError::InvalidHeader)?;

    // read the 4 bytes that come directly after the first 14 header bytes
    // into variable time_base. Read the next two bytes after that into num_pmu;
    let time_base: u32 = u32::from_be_bytes([
        buffer[PREFIX_SIZE],
        buffer[PREFIX_SIZE + 1],
        buffer[PREFIX_SIZE + 2],
        buffer[PREFIX_SIZE + 3],
    ]);
    let num_pmu: u16 = u16::from_be_bytes([buffer[PREFIX_SIZE + 4], buffer[PREFIX_SIZE + 5]]);

    // Create a buffer offset variable that starts after
    // COMMMON_HEADER_SIZE and CONFIG_HEADER_SIZE
    // create a for loop based on config_header.num_pmu:
    // Read the next 26 bytes into a PMUConfigurationFrame2011 struct.
    // Increment the offset by 26 bytes.
    let mut offset = PREFIX_SIZE + 6;

    // PMU and Channel Configs should be same length.
    let mut pmu_configs = Vec::new();
    for _ in 0..num_pmu {
        if offset + 26 > buffer.len() {
            return Err(ParseError::InsufficientData);
        }
        // read the next 16 bytes into a variable pub stn [u8; 16]
        // increment offset by 16
        // read the next 2 bytes into a u16 variable idcode
        // read the next 2 bytes into a u16 variable format
        //
        // read the next 2 bytes into a u16 variable phnmr
        // read the next 2 bytes inot a u16 variable annmr
        let stn: [u8; 16] = buffer[offset..offset + 16].try_into().unwrap();
        offset += 16;
        let idcode = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let format = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let phnmr = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let annmr = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let dgnmr = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;

        let mut pmu_config = PMUConfigurationFrame2011 {
            stn,
            idcode,
            format,
            phnmr,
            annmr,
            dgnmr,
            chnam: Vec::new(),   // Will be populated later
            phunit: Vec::new(),  // Will be populated later
            anunit: Vec::new(),  // Will be populated later
            digunit: Vec::new(), // Will be populated later
            fnom: 0,             // Will be populated later
            cfgcnt: 0,           // Will be populated later
        };

        // determine the next length of bytes to read populate the chnam field.
        let chnam_bytes_len: usize = (16 * (phnmr + annmr + 16 * dgnmr)) as usize;
        // read from offset to chname_bytes_len into a vec<u8> variable.
        let chnam = buffer[offset..offset + chnam_bytes_len].to_vec();
        offset += chnam_bytes_len as usize;
        pmu_config.chnam = chnam;

        // read from offset to 4*phnmr into a vec<u32> variable.
        let phunit = buffer[offset..offset + 4 * phnmr as usize]
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u32>>();
        offset += 4 * phnmr as usize;

        let anunit = buffer[offset..offset + 4 * annmr as usize]
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u32>>();
        offset += 4 * annmr as usize;

        let digunit = buffer[offset..offset + 4 * dgnmr as usize]
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u32>>();
        offset += 4 * dgnmr as usize;

        pmu_config.phunit = phunit;
        pmu_config.anunit = anunit;
        pmu_config.digunit = digunit;

        // finally read the next 2 bytes from the offset into fnom
        let fnom = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        pmu_config.fnom = fnom;

        // read the next 2 bytes from the offset into cfgcnt
        let cfgcnt = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        pmu_config.cfgcnt = cfgcnt;

        pmu_configs.push(pmu_config);
    }
    // Generate the configuration frame 1 and 2 based on the variables throughout this function.
    let config_frame = ConfigurationFrame1and2_2011 {
        prefix: common_header,
        time_base,
        num_pmu,
        pmu_configs,
        data_rate: i16::from_be_bytes([buffer[offset], buffer[offset + 1]]),
        chk: u16::from_be_bytes([buffer[offset + 2], buffer[offset + 3]]),
    };

    Ok(config_frame)
}

pub fn parse_config_frame_3(buffer: &[u8]) -> Result<Frame, ParseError> {
    // TODO
    todo!("Implement Config Frame type 3 parsing.")
}

pub fn parse_frame(
    buffer: &[u8],
    config: Option<ConfigurationFrame1and2_2011>,
) -> Result<Frame, ParseError> {
    // read first two bytes as the sync variable.
    // read second byte and convert to binary
    // check bits 3-0 to get version number of IEEE standard.
    // if bits 3-0 == 0001, use IEEEstandard 2005
    // if bits 3-0 == 0010, use IEEE standard from 2011
    // if bits 3-0 do not equal 0010, throw ParseError:VersionNotSupported
    println!("Reading Frame Prefix");
    let sync = u16::from_be_bytes([buffer[0], buffer[1]]);

    if sync >> 8 != 0xAA {
        let sync_slice = &buffer[0..2];
        println!("MOCK PDC: Invalid Sync value {:02X?}", sync_slice);
        return Err(ParseError::InvalidHeader);
    }
    println!("Reading version");
    let version = buffer[1] & 0b1111;

    match version {
        1 => {
            // Use IEEE standard 2005
            //
            println!("Standard 2005 not supported");
            //return Err(ParseError::VersionNotSupported);
        }
        2 => {
            println!("Standard 2011 detected");
            // Use IEEE standard 2011
            // No error
        }
        _ => {
            println!("Unknown version detected");
            return Err(ParseError::VersionNotSupported);
        }
    }

    // Next, get framesize variable at bytes 3-4
    // verify framesize equals length of buffer.
    // verify checksum, CRC-CCITT matches check value
    // at framesize - 2 bytes
    let framesize = u16::from_be_bytes([buffer[2], buffer[3]]);
    if framesize as usize != buffer.len() {
        return Err(ParseError::InvalidFrameSize);
    }
    println!("Calculating CRC");
    let calculated_crc = calculate_crc(&buffer[..buffer.len() - 2]);
    let frame_crc = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);
    if calculated_crc != frame_crc {
        println!("Invalid CRC");
        return Err(ParseError::InvalidCRC);
    }

    // convert second byte of sync variable to bit representation.
    // If bits 6-4 equal 000 -> parse_data_frame (buffer: &[u8], framesize:u16)
    // if bits 6-4 equal 001 -> parse_header_frame (buffer. framesize)
    // if bits 6-4 equal 010 or 011 -> parse_config_frame_1and2(buffer, framesize)
    // If bits 6-4 equal 101 -> parse_config_frame_3(buffer, framesize)
    // if bits 6-4 equal 100 -> parse_command_frame(buffer, framesize)
    println!("Determining Frame Type");
    let frame_type = (buffer[1] >> 4) & 0b111;
    match frame_type {
        0b000 => match config {
            Some(config) => {
                let data_frame = parse_data_frames(buffer, &config)?;
                Ok(Frame::Data(data_frame))
            }
            None => {
                println!("Configuration Frame required to parse data frame.");
                Err(ParseError::InsufficientData)
            }
        },
        0b001 => {
            println!("Parsing Header");
            let header = parse_header(buffer)?;
            Ok(Frame::Header(header))
        }
        0b010 | 0b011 => {
            println!("parsing config frame");
            let config = parse_config_frame_1and2(buffer)?;
            Ok(Frame::Configuration(config))
        }
        0b101 => parse_config_frame_3(buffer),
        0b100 => {
            println!("Parsing command frame");
            parse_command_frame(buffer)
        }
        _ => Err(ParseError::InvalidFrameSize),
    }
}
