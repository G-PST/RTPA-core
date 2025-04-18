// Unified PMUConfigurationFrame
#![allow(unused)]
use super::common::{ChannelDataType, ChannelInfo, ParseError, PrefixFrame, Version};
use super::units::{AnalogUnits, DataRate, DigitalUnits, NominalFrequency, PhasorUnits};
use super::utils::validate_checksum;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PMUConfigurationFrame {
    pub stn: [u8; 16], // Station name
    pub idcode: u16,
    pub format: u16, // Data format flags
    pub phnmr: u16,
    pub annmr: u16,
    pub dgnmr: u16,
    pub chnam: Vec<u8>, // Channel names
    pub phunit: Vec<PhasorUnits>,
    pub anunit: Vec<AnalogUnits>,
    pub digunit: Vec<u32>,
    pub fnom: NominalFrequency,
    pub cfgcnt: u16,
    // 2024-specific fields (optional)
    pub additional_metadata: Option<Vec<u8>>, // CFG-3 extended metadata
}

impl PMUConfigurationFrame {
    pub fn from_hex(bytes: &[u8], version: Version) -> Result<Self, ParseError> {
        let mut offset = 0;
        let stn = bytes[offset..offset + 16].try_into().unwrap();
        offset += 16;
        let idcode = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let format = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let phnmr = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let annmr = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let dgnmr = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let chnam_len = 16 * (phnmr + annmr + 16 * dgnmr) as usize;
        let chnam = bytes[offset..offset + chnam_len].to_vec();
        offset += chnam_len;

        let mut phunit = vec![];
        for _ in 0..phnmr {
            phunit.push(
                PhasorUnits::from_hex(&bytes[offset..offset + 4]).unwrap(), //u32::from_be_bytes(
                                                                            //bytes[offset..offset + 4].try_into().unwrap(),
                                                                            //)
            );
            offset += 4;
        }

        let mut anunit = vec![];
        for _ in 0..annmr {
            anunit.push(AnalogUnits::from_hex(&bytes[offset..offset + 4]).unwrap());
            offset += 4;
        }

        // TODO implement digital unit struct
        let mut digunit = vec![];
        for _ in 0..dgnmr {
            digunit.push(u32::from_be_bytes(
                bytes[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
        }

        //let fnom = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        let fnom = NominalFrequency::from_hex(&bytes[offset..offset + 2]).unwrap();
        offset += 2;
        let cfgcnt = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let additional_metadata = match version {
            Version::V2024 => Some(bytes[offset..].to_vec()), // CFG-3 or extended data
            _ => None,
        };

        Ok(PMUConfigurationFrame {
            stn,
            idcode,
            format,
            phnmr,
            annmr,
            dgnmr,
            chnam,
            phunit,
            anunit,
            digunit,
            fnom,
            cfgcnt,
            additional_metadata,
        })
    }

    pub fn to_hex(&self, version: Version) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.stn);

        result.extend_from_slice(&self.idcode.to_be_bytes());
        result.extend_from_slice(&self.format.to_be_bytes());
        result.extend_from_slice(&self.phnmr.to_be_bytes());
        result.extend_from_slice(&self.annmr.to_be_bytes());
        result.extend_from_slice(&self.dgnmr.to_be_bytes());

        // Ensure channel names are written with the correct structure
        result.extend_from_slice(&self.chnam);

        for ph in &self.phunit {
            result.extend_from_slice(&ph.to_hex());
        }
        for an in &self.anunit {
            result.extend_from_slice(&an.to_hex());
        }
        for dg in &self.digunit {
            result.extend_from_slice(&dg.to_be_bytes());
        }
        result.extend_from_slice(&self.fnom.to_hex().unwrap());
        result.extend_from_slice(&self.cfgcnt.to_be_bytes());
        if let Some(meta) = &self.additional_metadata {
            if version == Version::V2024 {
                result.extend_from_slice(meta);
            }
        }
        result
    }

    // Reuse your helper methods, adjusted for version
    pub fn freq_dfreq_size(&self) -> usize {
        if self.format & 0x0008 != 0 {
            4
        } else {
            2
        }
    }

    pub fn analog_size(&self) -> usize {
        if self.format & 0x0004 != 0 {
            4
        } else {
            2
        }
    }

    pub fn phasor_size(&self) -> usize {
        if self.format & 0x0002 != 0 {
            8
        } else {
            4
        }
    }

    pub fn is_phasor_polar(&self) -> bool {
        self.format & 0x0001 != 0
    }

    pub fn get_column_names(&self) -> Vec<String> {
        let station_name = String::from_utf8_lossy(&self.stn).trim().to_string();
        self.chnam
            .chunks(16)
            .map(|chunk| {
                let channel = String::from_utf8_lossy(chunk).trim().to_string();
                format!("{}_{}_{}", station_name, self.idcode, channel)
            })
            .collect()
    }
}

// Unified ConfigurationFrame (CFG-1, CFG-2, CFG-3)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationFrame {
    pub prefix: PrefixFrame,
    pub time_base: u32,
    pub num_pmu: u16,
    pub pmu_configs: Vec<PMUConfigurationFrame>,
    pub data_rate: i16,
    pub chk: u16,
    pub cfg_type: u8, // 1, 2, or 3
}

impl ConfigurationFrame {
    pub fn from_hex(bytes: &[u8]) -> Result<Self, ParseError> {
        let prefix = PrefixFrame::from_hex(bytes).unwrap();
        let cfg_type = match (prefix.sync >> 4) & 0x07 {
            0x02 => 1, // CFG-1
            0x03 => 2, // CFG-2
            0x05 => 3, // CFG-3
            _ => return Err(ParseError::InvalidFrameType),
        };

        if prefix.framesize as usize != bytes.len() {
            println!(
                "Error: Configurationframe buffer does not match expected size: {} bytes received, expected {} bytes",
                bytes.len(), prefix.framesize
            );
            return Err(ParseError::InvalidLength);
        }

        if !validate_checksum(bytes) {
            return Err(ParseError::InvalidChecksum);
        }

        let mut offset = 14;
        let time_base = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let num_pmu = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let mut pmu_configs = vec![];
        println!("Buffer length: {}, num_pmu: {}", bytes.len(), num_pmu);

        for i in 0..num_pmu {
            println!("Parsing PMU #{}, offset: {}", i, offset);

            if offset + 26 > bytes.len() {
                //return Err("Buffer too short for PMU configuration");
                return Err(ParseError::InvalidLength);
            }

            // Read basic PMU info to determine sizes
            let phnmr = u16::from_be_bytes(bytes[offset + 20..offset + 22].try_into().unwrap());
            let annmr = u16::from_be_bytes(bytes[offset + 22..offset + 24].try_into().unwrap());
            let dgnmr = u16::from_be_bytes(bytes[offset + 24..offset + 26].try_into().unwrap());

            println!(
                "PMU #{}: phnmr={}, annmr={}, dgnmr={}",
                i, phnmr, annmr, dgnmr
            );

            // Calculate expected size - this calculation might be wrong
            let chnam_size = 16 * (phnmr + annmr + 16 * dgnmr) as usize;
            let unit_size = 4 * (phnmr + annmr + dgnmr) as usize;
            let pmu_size = 26 + chnam_size + unit_size + 4; // 26 basic fields + chnam + units + fnom/cfgcnt

            println!(
                "Calculated PMU size: {}, remaining buffer: {}",
                pmu_size,
                bytes.len() - offset
            );

            if offset + pmu_size > bytes.len() {
                //return Err("Buffer too short for calculated PMU size");
                return Err(ParseError::InvalidLength);
            }

            // Now try to parse with the updated understanding of the format
            let pmu_bytes = &bytes[offset..offset + pmu_size];
            let pmu_config = PMUConfigurationFrame::from_hex(pmu_bytes, prefix.version)?;
            pmu_configs.push(pmu_config);

            offset += pmu_size;
        }

        // Debug the remaining buffer
        println!(
            "After PMU configs, offset: {}, remaining: {}",
            offset,
            bytes.len() - offset
        );

        // Ensure we have enough bytes for data_rate and checksum
        if offset + 4 > bytes.len() {
            //return Err("Buffer too short for data_rate and checksum");
            return Err(ParseError::InvalidLength);
        }

        let data_rate = i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let chk = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());

        Ok(ConfigurationFrame {
            prefix,
            time_base,
            num_pmu,
            pmu_configs,
            data_rate,
            chk,
            cfg_type,
        })
    }

    pub fn to_hex(&self) -> Vec<u8> {
        // Create a copy of the prefix with the correct frame size
        let mut prefix = self.prefix.clone();

        // Calculate the frame size excluding the checksum (which we'll add at the end)
        // We need to add 6 bytes (4 for time_base, 2 for num_pmu) +
        // all PMU configs + 2 for data_rate + 2 for checksum
        let mut frame_size = 14; // Prefix size
        frame_size += 4; // time_base
        frame_size += 2; // num_pmu

        // Add size of each PMU configuration
        for pmu in &self.pmu_configs {
            // Basic size: 16 (STN) + 2 (ID) + 2 (Format) + 6 (channel counts) +
            // chnam + units + 2 (FNOM) + 2 (CFGCNT)
            let chnam_len = 16 * (pmu.phnmr + pmu.annmr + 16 * pmu.dgnmr) as usize;
            let unit_len = 4 * (pmu.phnmr + pmu.annmr + pmu.dgnmr) as usize;
            frame_size += 16 + 2 + 2 + 6 + chnam_len + unit_len + 2 + 2;
        }

        frame_size += 2; // data_rate
        frame_size += 2; // checksum

        // Update the prefix's frame size
        prefix.framesize = frame_size as u16;

        // Now start building the result
        let mut result = Vec::with_capacity(frame_size);

        // Start with the prefix frame bytes
        result.extend_from_slice(&prefix.to_hex());

        // Time base
        result.extend_from_slice(&self.time_base.to_be_bytes());

        // Number of PMUs
        result.extend_from_slice(&self.num_pmu.to_be_bytes());

        // Add each PMU configuration
        for pmu in &self.pmu_configs {
            result.extend_from_slice(&pmu.to_hex(self.prefix.version));
        }

        // Data rate
        result.extend_from_slice(&self.data_rate.to_be_bytes());

        // Calculate the checksum for all bytes so far
        let chk = super::utils::calculate_crc(&result);

        // Add the checksum
        result.extend_from_slice(&chk.to_be_bytes());

        // Verify the result length matches our calculated frame size
        assert_eq!(result.len(), frame_size, "Generated frame size mismatch");

        result
    }

    pub fn calc_data_frame_size(&self) -> usize {
        let mut total_size = 16; // PrefixFrame + CHK
        for pmu_config in &self.pmu_configs {
            total_size += 2; // STAT
            total_size += pmu_config.phasor_size() * pmu_config.phnmr as usize;
            total_size += 2 * pmu_config.freq_dfreq_size();
            total_size += pmu_config.analog_size() * pmu_config.annmr as usize;
            total_size += 2 * pmu_config.dgnmr as usize;
        }
        total_size
    }

    pub fn get_channel_map(&self) -> HashMap<String, ChannelInfo> {
        let mut channel_map = HashMap::new();
        let mut current_offset = 2; // After STAT
        let prefix_offset = 14;

        for pmu_config in &self.pmu_configs {
            let station_name = String::from_utf8_lossy(&pmu_config.stn).trim().to_string();
            let channel_names = pmu_config.get_column_names();
            let id_code = pmu_config.idcode;

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

            let phasor_type = match (
                pmu_config.format & 0x0002 != 0,
                pmu_config.format & 0x0001 != 0,
            ) {
                (false, false) => ChannelDataType::PhasorIntRectangular,
                (false, true) => ChannelDataType::PhasorIntPolar,
                (true, false) => ChannelDataType::PhasorFloatRectangular,
                (true, true) => ChannelDataType::PhasorFloatPolar,
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

            let analog_type = if pmu_config.format & 0x0004 != 0 {
                ChannelDataType::AnalogFloat
            } else {
                ChannelDataType::AnalogFixed
            };

            let analog_size = pmu_config.analog_size();
            for name in channel_names
                .iter()
                .skip(pmu_config.phnmr as usize)
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

            for name in channel_names
                .iter()
                .skip((pmu_config.phnmr + pmu_config.annmr) as usize)
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
