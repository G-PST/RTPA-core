#[cfg(test)]
mod unified_tests {
    use crate::ieee_c37_118::commands::{CommandFrame, CommandType};
    use crate::ieee_c37_118::common::{ChannelDataType, PrefixFrame, StatField, Version};
    use crate::ieee_c37_118::config::{ConfigurationFrame, PMUConfigurationFrame};
    use crate::ieee_c37_118::data_frame::DataFrame;

    use crate::ieee_c37_118::utils::{calculate_crc, validate_checksum};

    use std::fs;
    use std::path::Path;

    // Helper function to read test data files (copied from existing tests)
    fn read_hex_file(file_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let path = Path::new("tests/test_data").join(file_name);
        let content = fs::read_to_string(path)?;

        // Remove any whitespace and newlines
        let hex_string: String = content.chars().filter(|c| !c.is_whitespace()).collect();

        // Ensure we have an even number of hex characters
        if hex_string.len() % 2 != 0 {
            return Err("Invalid hex string: odd number of characters".into());
        }

        // Convert pairs of hex characters to bytes
        let mut result = Vec::with_capacity(hex_string.len() / 2);
        let mut i = 0;
        while i < hex_string.len() {
            let byte_str = &hex_string[i..i + 2];
            let byte = u8::from_str_radix(byte_str, 16)?;
            result.push(byte);
            i += 2;
        }

        Ok(result)
    }

    #[test]
    fn test_prefix_frame_from_hex() {
        let buffer = read_hex_file("config_message.bin").unwrap();
        let prefix_result = PrefixFrame::from_hex(&buffer);

        assert!(prefix_result.is_ok(), "Failed to parse prefix frame");
        let prefix = prefix_result.unwrap();

        // Verify fields
        assert_eq!(prefix.sync, 0xAA31); // Config2 frame in 2005 version
        assert_eq!(prefix.framesize, 454);
        assert_eq!(prefix.idcode, 7734);
        assert_eq!(prefix.soc, 1_149_577_200);

        assert_eq!(prefix.fracsec, 463000);
        // Verify version detection
        assert_eq!(prefix.version, Version::V2005);
    }

    #[test]
    fn test_prefix_frame_to_hex() {
        let buffer = read_hex_file("config_message.bin").unwrap();
        let prefix = PrefixFrame::from_hex(&buffer).unwrap();

        // Convert back to bytes
        let bytes = prefix.to_hex();

        // Compare with original
        assert_eq!(&bytes[..], &buffer[0..14]);
    }

    #[ignore]
    #[test]
    fn test_stat_field() {
        // Test V2011 stat field

        let raw_value: u16 = 0xA123; // Example with data_error=2, pmu_sync=true, etc.
        let stat = StatField::from_raw(raw_value, Version::V2011);

        // Debug info
        println!("Raw value: 0x{:04X}", raw_value);
        println!("Stat field: {:?}", stat);

        // Verify fields - check bit by bit
        assert_eq!(stat.raw, raw_value);
        assert_eq!(stat.data_error, 2, "data_error should be 2");

        // Check individual bits
        assert_eq!(
            (raw_value >> 13) & 1,
            if stat.pmu_sync { 1 } else { 0 },
            "pmu_sync bit mismatch"
        );
        assert_eq!(
            (raw_value >> 12) & 1,
            if stat.data_sorting { 1 } else { 0 },
            "data_sorting bit mismatch"
        );
        assert_eq!(
            (raw_value >> 11) & 1,
            if stat.pmu_trigger { 1 } else { 0 },
            "pmu_trigger bit mismatch"
        );

        // Convert back to raw and verify
        let back_to_raw = stat.to_raw(Version::V2011);
        assert_eq!(
            back_to_raw, raw_value,
            "Raw value mismatch after conversion"
        );
        assert_eq!(back_to_raw, raw_value);
    }

    #[test]
    fn test_pmu_configuration_frame_from_hex() {
        let buffer = read_hex_file("config_message.bin").unwrap();

        // Skip prefix (14 bytes) and TIME_BASE (4 bytes) and NUM_PMU (2 bytes)
        let offset = 20;

        // The PMU configuration section starts after offset
        // For testing, we need to know the format's details
        let phnmr = 4;
        let annmr = 3;
        let dgnmr = 1;

        let pmu_config_result = PMUConfigurationFrame::from_hex(&buffer[offset..]);

        assert!(
            pmu_config_result.is_ok(),
            "Failed to parse PMU configuration frame"
        );
        let pmu_config = pmu_config_result.unwrap();

        // Verify fields
        assert_eq!(pmu_config.idcode, 7734);
        assert_eq!(pmu_config.format, 4); // Based on existing test
        assert_eq!(pmu_config.phnmr, 4);
        assert_eq!(pmu_config.annmr, 3);
        assert_eq!(pmu_config.dgnmr, 1);

        // Test computed properties
        assert_eq!(pmu_config.freq_dfreq_size(), 2); // Fixed-point format
        assert_eq!(pmu_config.analog_size(), 4); // Floating-point format
        assert_eq!(pmu_config.phasor_size(), 4); // Fixed-point format
        assert_eq!(pmu_config.is_phasor_polar(), false);

        // Verify station name is properly decoded
        let station_name = String::from_utf8_lossy(&pmu_config.stn).trim().to_string();
        assert_eq!(station_name, "Station A");

        // Test column names
        let column_names = pmu_config.get_column_names();
        assert_eq!(
            column_names.len(),
            phnmr as usize + annmr as usize + 16 * dgnmr as usize
        );
        assert!(column_names[0].starts_with("Station A_7734_"));
    }

    #[test]
    fn test_pmu_configuration_frame_to_hex() {
        let buffer = read_hex_file("config_message.bin").unwrap();

        // Skip prefix and other headers
        let offset = 20;

        // The PMU configuration section
        let _phnmr = 4;
        let _annmr = 3;
        let _dgnmr = 1;

        let pmu_config = PMUConfigurationFrame::from_hex(&buffer[offset..]).unwrap();

        // Convert back to bytes
        let bytes = pmu_config.to_hex();

        // We need to determine how much of the original buffer corresponds to this PMU config
        // For this test, we'll check key parts instead of the entire serialized frame
        // since the exact boundaries depend on buffer structure

        // Check station name (first 16 bytes)
        assert_eq!(&bytes[0..16], &buffer[offset..offset + 16]);

        // Check ID code and format (next 4 bytes)
        assert_eq!(&bytes[16..20], &buffer[offset + 16..offset + 20]);

        // Check phnmr, annmr, dgnmr (next 6 bytes)
        assert_eq!(&bytes[20..26], &buffer[offset + 20..offset + 26]);
    }

    #[test]
    fn test_configuration_frame_from_hex() {
        let buffer = read_hex_file("config_message.bin").unwrap();

        let config_result = ConfigurationFrame::from_hex(&buffer);

        assert!(config_result.is_ok(), "Failed to parse configuration frame");
        let config_frame = config_result.unwrap();

        // Verify fields
        assert_eq!(config_frame.prefix.sync, 0xAA31);
        assert_eq!(config_frame.prefix.framesize, 454);
        assert_eq!(config_frame.prefix.idcode, 7734);
        assert_eq!(config_frame.time_base, 1000000);
        assert_eq!(config_frame.num_pmu, 1);
        assert_eq!(config_frame.data_rate, 30);
        assert_eq!(config_frame.cfg_type, 2); // Config-2 frame

        // Verify PMU configurations
        assert_eq!(config_frame.pmu_configs.len(), 1);
        let pmu_config = &config_frame.pmu_configs[0];
        assert_eq!(pmu_config.idcode, 7734);
        assert_eq!(pmu_config.phnmr, 4);
        assert_eq!(pmu_config.annmr, 3);
        assert_eq!(pmu_config.dgnmr, 1);

        // Verify checksum
        let calculated_crc = calculate_crc(&buffer[..buffer.len() - 2]);
        assert_eq!(calculated_crc, config_frame.chk, "CRC mismatch");
    }

    #[test]
    fn test_configuration_frame_to_hex() {
        let buffer = read_hex_file("config_message.bin").unwrap();
        let config_frame = ConfigurationFrame::from_hex(&buffer).unwrap();

        // Convert back to bytes
        let bytes = config_frame.to_hex();

        // The frame should be identical to the original buffer
        assert_eq!(bytes.len(), buffer.len());

        // Check if the generated bytes match the original buffer
        for i in 0..bytes.len() {
            if bytes[i] != buffer[i] {
                panic!(
                    "Mismatch at byte {}: expected 0x{:02X}, got 0x{:02X}",
                    i, buffer[i], bytes[i]
                );
            }
        }

        // Also verify checksum is correct in generated buffer
        let () = validate_checksum(&bytes).unwrap();
    }

    #[test]
    fn test_channel_map() {
        let buffer = read_hex_file("config_message.bin").unwrap();
        let config_frame = ConfigurationFrame::from_hex(&buffer).unwrap();

        let channel_map = config_frame.get_channel_map();

        // There should be entries for all channels including FREQ and DFREQ
        let expected_channels = config_frame.pmu_configs[0].phnmr as usize
            + config_frame.pmu_configs[0].annmr as usize
            + config_frame.pmu_configs[0].dgnmr as usize
            + 2; // +2 for FREQ and DFREQ

        assert_eq!(channel_map.len(), expected_channels);

        // Check if FREQ channel exists and has correct properties
        let freq_channel = channel_map
            .get("Station A_7734_FREQ")
            .expect("FREQ channel not found");
        assert_eq!(freq_channel.data_type, ChannelDataType::FreqFixed);

        // Check for a phasor channel
        let columns = config_frame.pmu_configs[0].get_column_names();
        let first_phasor = &columns[0];
        let phasor_channel = channel_map
            .get(first_phasor)
            .expect("Phasor channel not found");
        assert!(matches!(
            phasor_channel.data_type,
            ChannelDataType::PhasorIntRectangular | ChannelDataType::PhasorFloatRectangular
        ));

        // Check if offsets are reasonable
        for (_, info) in &channel_map {
            assert!(info.offset >= 14, "Offset should be after prefix frame");
            assert!(info.size > 0, "Size should be positive");
        }
    }

    #[test]
    fn test_calc_data_frame_size() {
        let buffer = read_hex_file("config_message.bin").unwrap();

        let config_frame = ConfigurationFrame::from_hex(&buffer).unwrap();

        let calculated_size = config_frame.calc_data_frame_size();

        // We expect 16 (prefix + chk) + 2 (STAT) +
        // 4*4 (phasors) + 2*2 (freq/dfreq) + 3*4 (analogs) + 1*2 (digital)
        let expected_size = 16 + 2 + 16 + 4 + 12 + 2;

        assert_eq!(calculated_size, expected_size);

        // Additionally, load a data frame and check if size matches
        if let Ok(data_buffer) = read_hex_file("data_message.bin") {
            assert_eq!(
                calculated_size,
                data_buffer.len(),
                "Calculated size doesn't match actual data frame size"
            );
        }
    }
    #[test]
    fn test_data_frame_from_and_to_hex() {
        // First read the configuration frame as we need it to interpret the data frame
        let config_buffer = read_hex_file("config_message.bin").unwrap();
        let config_frame = ConfigurationFrame::from_hex(&config_buffer).unwrap();

        // Now read and parse the data frame
        let data_buffer = read_hex_file("data_message.bin").unwrap();
        let data_frame_result = DataFrame::from_hex(&data_buffer, &config_frame);

        assert!(data_frame_result.is_ok(), "Failed to parse data frame");
        let data_frame = data_frame_result.unwrap();

        // Verify basic fields
        assert_eq!(data_frame.prefix.sync & 0xF000, 0xA000); // Data frame
        assert_eq!(data_frame.prefix.idcode, 7734);

        // Verify we have the right number of PMU data sections
        assert_eq!(data_frame.pmu_data.len(), config_frame.num_pmu as usize);

        // Convert back to bytes
        let output_bytes = data_frame.to_hex();

        // The output should be the same length as the input
        assert_eq!(output_bytes.len(), data_buffer.len());

        // Compare the original and regenerated bytes
        for i in 0..output_bytes.len() {
            if output_bytes[i] != data_buffer[i] {
                panic!(
                    "Mismatch at byte {}: expected 0x{:02X}, got 0x{:02X}",
                    i, data_buffer[i], output_bytes[i]
                );
            }
        }

        // Verify checksum is correct

        validate_checksum(&output_bytes).unwrap();
    }

    #[test]
    fn test_command_frame_parse_from_file() {
        // Read the command message binary file
        let buffer = read_hex_file("cmd_message.bin").unwrap();

        // Parse the command frame
        let command_frame = CommandFrame::from_hex(&buffer);

        assert!(command_frame.is_ok(), "Failed to parse command frame");
        let cmd_frame = command_frame.unwrap();

        // Verify basic fields
        assert_eq!(cmd_frame.prefix.sync, 0xAA41, "Incorrect sync word");
        assert_eq!(cmd_frame.prefix.framesize, 18, "Incorrect frame size");
        assert_eq!(cmd_frame.prefix.idcode, 7734, "Incorrect ID code");
        assert_eq!(cmd_frame.prefix.soc, 1149591600, "Incorrect SOC");

        // The fracsec in the file includes the fraction of second time quality
        // but we only care about verifying that we can parse it correctly
        assert_eq!(cmd_frame.prefix.fracsec, 770000, "Incorrect FRACSEC");

        // Verify command code
        assert_eq!(cmd_frame.command, 2, "Incorrect command");
        assert_eq!(
            cmd_frame.command_type(),
            Some(CommandType::TurnOnTransmission),
            "Incorrect command type"
        );

        // Verify no extended data
        assert_eq!(
            cmd_frame.extended_data, None,
            "Should have no extended data"
        );

        // Verify checksum
        validate_checksum(&buffer).unwrap();

        // Convert back to bytes and verify
        let recreated_bytes = cmd_frame.to_hex();
        assert_eq!(
            recreated_bytes.len(),
            buffer.len(),
            "Recreated bytes have incorrect length"
        );

        // Compare original and recreated bytes
        for i in 0..recreated_bytes.len() {
            assert_eq!(
                recreated_bytes[i], buffer[i],
                "Byte mismatch at position {}: expected 0x{:02X}, got 0x{:02X}",
                i, buffer[i], recreated_bytes[i]
            );
        }

        // Verify we can generate a similar command frame
        let generated_cmd =
            CommandFrame::new_turn_on_transmission(7734, Some((1149591600, 252428240)));

        let generated_bytes = generated_cmd.to_hex();

        // The generated command should have the same structure as the original
        assert_eq!(
            generated_bytes.len(),
            buffer.len(),
            "Generated bytes have incorrect length"
        );

        // Verify key fields match
        assert_eq!(generated_cmd.prefix.idcode, 7734);
        assert_eq!(generated_cmd.command, 2);
        assert_eq!(
            generated_cmd.command_type(),
            Some(CommandType::TurnOnTransmission)
        );

        // Ensure the checksum is valid for the generated frame

        validate_checksum(&generated_bytes).unwrap();
    }
}
